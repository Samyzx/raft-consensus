"""Loopback dashboard for two real, independently managed Raft clusters."""

import argparse
import asyncio
import json
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import grpc

from . import raft_pb2 as rpc
from . import raft_pb2_grpc as service
from .client import Client
from .config import PROFILES


COUNTS = (3, 5, 7, 9)
MODES = ("standard", "lease")
ASSETS = {
    "/": ("index.html", "text/html"),
    "/style.css": ("style.css", "text/css"),
    "/app.js": ("app.js", "text/javascript"),
}


def reserve_addresses(count):
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(2 * count)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        ports = [sock.getsockname()[1] for sock in sockets]
    return [
        {str(index): f"127.0.0.1:{port}" for index, port in enumerate(group)}
        for group in (ports[:count], ports[count:])
    ]


class Cluster:
    def __init__(self, mode, directory, addresses, *, profile="lab", peer_rtt_ms=0):
        self.mode = mode
        self.directory = directory
        self.addresses = addresses
        self.profile = profile
        self.peer_rtt_ms = peer_rtt_ms
        self.majority = len(addresses) // 2 + 1
        self.processes: dict[str, asyncio.subprocess.Process] = {}
        directory.mkdir(parents=True)
        self.config = directory / "cluster.json"
        self.config.write_text(json.dumps(addresses), encoding="utf-8")

    async def start(self, node_id):
        process = self.processes.get(node_id)
        if process and process.returncode is None:
            return
        with (self.directory / f"process-{node_id}.log").open("ab") as output:
            self.processes[node_id] = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "raftkv", "--cluster", str(self.config),
                "node", "--id", node_id, "--mode", self.mode, "--profile", self.profile,
                "--peer-rtt-ms", str(self.peer_rtt_ms),
                "--data-root", str(self.directory), stdout=output, stderr=output,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )

    async def stop(self, node_id):
        process = self.processes.pop(node_id, None)
        if process and process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()

    async def start_all(self):
        await asyncio.gather(*(self.start(node) for node in self.addresses))

    async def close(self):
        await asyncio.gather(*(self.stop(node) for node in self.addresses))

    async def status(self, node_id):
        endpoint = self.addresses[node_id]
        process = self.processes.get(node_id)
        node = {"id": node_id, "address": endpoint, "role": "stopped"}
        if process is None:
            return node
        if process.returncode is not None:
            return {**node, "role": "exited", "exit_code": process.returncode}
        async with grpc.aio.insecure_channel(endpoint) as channel:
            try:
                status = await service.RaftStub(channel).Inspect(rpc.InspectRequest(), timeout=0.3)
            except grpc.aio.AioRpcError:
                return {**node, "role": "starting"}
        return {
            **node, "role": status.role, "term": status.term, "leader_id": status.leader_id,
            "log_index": status.log_index, "commit_index": status.commit_index, "ready": status.ready,
            "lease_ms": status.lease_remaining_ms, "handoff_ms": status.handoff_remaining_ms,
            "replication_rounds": status.replication_rounds,
        }

    async def snapshot(self):
        nodes = await asyncio.gather(*(self.status(node) for node in self.addresses))
        return {"mode": self.mode, "majority": self.majority, "nodes": nodes}

    async def request(self, command, timeout, *, node_id=None):
        start = time.monotonic()
        result = {"mode": self.mode}
        addresses = self.addresses if node_id is None else {node_id: self.addresses[node_id]}
        preferred = node_id
        if preferred is None:
            snapshot = await self.snapshot()
            ready = [node for node in snapshot["nodes"] if node.get("ready")]
            if ready:
                preferred = max(ready, key=lambda node: node["term"])["id"]
        try:
            async with Client(addresses, rpc_timeout=min(2.0, timeout)) as client:
                client.leader_id = preferred or ""
                data = await client.request(*command, timeout=timeout)
            result.update(success=True, data=data)
        except TimeoutError:
            result.update(success=False, error="No successful reply before the deadline")
        except grpc.aio.AioRpcError as error:
            result.update(success=False, error=error.details() or error.code().name)
        return {**result, "elapsed_ms": round((time.monotonic() - start) * 1000)}


class Lab:
    def __init__(self, directory):
        self.directory = directory
        self.clusters: dict[str, Cluster] = {}
        self.lock = asyncio.Lock()
        self.generation = 0

    async def close(self):
        async with self.lock:
            await self._close_clusters()

    async def _close_clusters(self):
        await asyncio.gather(*(cluster.close() for cluster in self.clusters.values()))

    async def reset(self, count):
        if count not in COUNTS:
            raise ValueError("Choose 3, 5, 7, or 9 nodes per cluster")
        await self._close_clusters()
        self.generation += 1
        self.clusters = {
            mode: Cluster(mode, self.directory / str(self.generation) / mode, addresses)
            for mode, addresses in zip(MODES, reserve_addresses(count), strict=True)
        }
        await asyncio.gather(*(cluster.start_all() for cluster in self.clusters.values()))

    async def snapshot(self):
        generation = self.generation
        clusters = await asyncio.gather(*(cluster.snapshot() for cluster in self.clusters.values()))
        return {"clusters": clusters, "generation": generation, "lease_ms": PROFILES["lab"].lease_ms}

    async def action(self, name, body):
        if self.lock.locked():
            raise ValueError("Another action is in progress")
        async with self.lock:
            if name == "reset":
                await self.reset(body.get("count"))
                return {"message": "Created two fresh clusters"}
            if name == "compare":
                return await self.compare()
            target = body.get("mode")
            if name == "request":
                return await self.request(body, target)
            if target not in MODES:
                raise ValueError("Choose a cluster")
            cluster = self.clusters[target]
            if name == "restore":
                await cluster.start_all()
                return {"message": f"Started stopped nodes in {target}"}
            node = body.get("id")
            if not isinstance(node, str) or node not in cluster.addresses:
                raise ValueError("Choose a node")
            if name == "stop":
                await cluster.stop(node)
                return {"message": f"Stopped {target} node {node}"}
            if name == "start":
                await cluster.start(node)
                return {"message": f"Started {target} node {node} with its saved log"}
            raise ValueError("Unknown action")

    async def request(self, body, target):
        verb, key, value = body.get("verb"), body.get("key"), body.get("value")
        if verb not in ("GET", "SET") or not isinstance(key, str) or not key:
            raise ValueError("Enter a key and choose GET or SET")
        if verb == "SET" and not isinstance(value, str):
            raise ValueError("Enter a string value")
        if target not in (*MODES, "both"):
            raise ValueError("Choose a cluster or both")
        command = (verb, key, value) if verb == "SET" else (verb, key)
        modes = MODES if target == "both" else (target,)
        results = await asyncio.gather(*(self.clusters[mode].request(command, 1.8) for mode in modes))
        return {"verb": verb, "key": key, "results": results}

    async def compare(self):
        state = (await self.snapshot())["clusters"]
        if any(not all("term" in node for node in cluster["nodes"]) for cluster in state):
            raise ValueError("Restore all nodes in both clusters before running this scenario")
        if any(not any(node.get("ready") for node in cluster["nodes"]) for cluster in state):
            raise ValueError("Wait for both clusters to elect a serving leader")

        key = "__quorum_demo__"
        value = "committed before quorum loss"
        seeded = await asyncio.gather(*(cluster.request(("SET", key, value), 1.8)
                                        for cluster in self.clusters.values()))
        if not all(result["success"] for result in seeded):
            raise ValueError("Could not commit the demo value in both clusters")

        state = (await self.snapshot())["clusters"]
        leader_ids = {}
        for cluster in state:
            leader = next((node for node in cluster["nodes"] if node.get("ready")), None)
            if leader is None or (cluster["mode"] == "lease" and leader["lease_ms"] < 2300):
                raise ValueError("Both leaders and a fresh lease are required. Try again shortly.")
            leader_ids[cluster["mode"]] = leader["id"]
        stopped = {
            mode: [node for node in cluster.addresses if node != leader_ids[mode]][:cluster.majority]
            for mode, cluster in self.clusters.items()
        }
        await asyncio.gather(*(self.clusters[mode].stop(node) for mode in MODES for node in stopped[mode]))
        read = ("GET", key)
        before = await self.read_original_leaders(read, leader_ids, 1.2)

        lease = self.clusters["lease"]
        try:
            async with asyncio.timeout(PROFILES["lab"].lease_seconds + 2):
                while True:
                    state = await lease.status(leader_ids["lease"])
                    if "term" in state and state["role"] != "leader":
                        break
                    await asyncio.sleep(0.1)
        except TimeoutError:
            raise ValueError("The lease leader did not step down within the demo window") from None
        after = await self.read_original_leaders(read, leader_ids, 0.8)
        expected = [False, True, False, False]
        observed = [result["success"] for result in before + after]
        passed = observed == expected and before[1].get("data") == value
        return {"before": before, "after": after, "stopped": stopped, "passed": passed, "key": key}

    async def read_original_leaders(self, command, leaders, timeout):
        return await asyncio.gather(*(
            cluster.request(command, timeout, node_id=leaders[mode]) for mode, cluster in self.clusters.items()
        ))


class LabServer(ThreadingHTTPServer):
    def __init__(self, port, lab, loop):
        super().__init__(("127.0.0.1", port), LabHandler)
        self.lab = lab
        self.loop = loop
        self.authority = f"127.0.0.1:{self.server_port}"


class LabHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, status, content_type, payload):
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, status, result):
        self._send(status, "application/json", json.dumps(result).encode())

    def _valid_host(self):
        host = self.headers.get("Host")
        return host in (self.server.authority, f"localhost:{self.server.server_port}")

    def _call(self, coroutine, timeout):
        future = asyncio.run_coroutine_threadsafe(coroutine, self.server.loop)
        try:
            self._json(200, future.result(timeout=timeout))
        except ValueError as error:
            self._json(400, {"error": str(error)})
        except (OSError, TimeoutError) as error:
            future.cancel()
            self._json(500, {"error": str(error) or "Operation timed out"})

    def do_GET(self):
        if not self._valid_host():
            self._json(403, {"error": "Use the local dashboard URL"})
        elif self.path == "/api/status":
            self._call(self.server.lab.snapshot(), 3)
        elif self.path in ASSETS:
            name, content_type = ASSETS[self.path]
            self._send(200, content_type, (Path(__file__).parent / "lab_ui" / name).read_bytes())
        else:
            self._json(404, {"error": "Unknown page"})

    def do_POST(self):
        origin = self.headers.get("Origin")
        if not self._valid_host() or origin not in (None, f"http://{self.headers.get('Host')}"):
            self._json(403, {"error": "Use the local dashboard URL"})
            return
        if not self.path.startswith("/api/"):
            self._json(404, {"error": "Unknown endpoint"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if self.headers.get_content_type() != "application/json" or not 0 < length <= 32768:
                raise ValueError
            self.connection.settimeout(5)
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError
        except (ValueError, UnicodeError, TimeoutError):
            self._json(400, {"error": "Expected a small JSON object"})
            return
        self._call(self.server.lab.action(self.path.removeprefix("/api/"), body), 25)


async def serve(port, nodes):
    with tempfile.TemporaryDirectory(prefix="raft-lab-") as directory:
        lab = Lab(Path(directory))
        server = LabServer(port, lab, asyncio.get_running_loop())
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        try:
            await lab.reset(nodes)
            thread.start()
            print(f"Raft Lab is running at http://{server.authority}", flush=True)
            await asyncio.Event().wait()
        finally:
            if thread.is_alive():
                await asyncio.to_thread(server.shutdown)
            server.server_close()
            await lab.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--nodes", type=int, choices=COUNTS, default=5)
    args = parser.parse_args()
    try:
        asyncio.run(serve(args.port, args.nodes))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
