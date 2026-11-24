"""Measure read latency, peer traffic, and crash recovery with local Raft processes."""

import argparse
import asyncio
import json
import math
import platform
import statistics
import sys
import tempfile
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import grpc

from . import raft_pb2 as rpc
from . import raft_pb2_grpc as service
from .config import PROFILES
from .lab import MODES, Cluster, reserve_addresses


NODE_COUNT = 5
WARMUP = 20
PROBE_SECONDS = 0.05
KEY = "bench"
VALUE = "committed"
READ = rpc.ServeClientArgs(Request=f"GET {KEY}")


@asynccontextmanager
async def cluster_pair(path, profile, rtt_ms):
    clusters = {
        mode: Cluster(mode, path / mode, addresses, profile=profile, peer_rtt_ms=rtt_ms)
        for mode, addresses in zip(MODES, reserve_addresses(NODE_COUNT), strict=True)
    }
    try:
        await asyncio.gather(*(cluster.start_all() for cluster in clusters.values()))
        yield clusters
    finally:
        await asyncio.gather(*(cluster.close() for cluster in clusters.values()))


def elected(snapshot, *, ready=False):
    candidates = [
        node for node in snapshot["nodes"] if node["role"] == "leader" and (not ready or node["ready"])
    ]
    return max(candidates, key=lambda node: node["term"], default=None)


async def seed(cluster):
    async with asyncio.timeout(20):
        while (leader := elected(await cluster.snapshot(), ready=True)) is None:
            await asyncio.sleep(PROBE_SECONDS)
        result = await cluster.request(("SET", KEY, VALUE), 3.0, node_id=leader["id"])
        if not result["success"]:
            raise RuntimeError(f"could not seed {cluster.mode}: {result}")
        while True:
            state = (await cluster.snapshot())["nodes"]
            if all(node.get("commit_index", 0) > leader["commit_index"] for node in state):
                return leader["id"]
            await asyncio.sleep(PROBE_SECONDS)


async def measure_reads(cluster, leader_id, count):
    async with grpc.aio.insecure_channel(cluster.addresses[leader_id]) as channel:
        stub = service.RaftStub(channel)
        for _ in range(WARMUP):
            reply = await stub.ServeClient(READ, timeout=2)
            if not reply.Success or reply.Data != VALUE:
                raise RuntimeError(f"{cluster.mode} warmup read failed")
        before = await stub.Inspect(rpc.InspectRequest(), timeout=1)
        samples = []
        for _ in range(count):
            started = time.perf_counter_ns()
            reply = await stub.ServeClient(READ, timeout=2)
            samples.append((time.perf_counter_ns() - started) / 1_000_000)
            if not reply.Success or reply.Data != VALUE:
                raise RuntimeError(f"{cluster.mode} read failed during measurement")
        after = await stub.Inspect(rpc.InspectRequest(), timeout=1)
    if before.term != after.term or not after.ready:
        raise RuntimeError(f"{cluster.mode} changed leadership during measurement")
    attempts = after.read_confirmation_requests - before.read_confirmation_requests
    samples.sort()
    return {
        "reads": count, "p50_ms": round(statistics.median(samples), 2),
        "p95_ms": round(samples[math.ceil(0.95 * count) - 1], 2),
        "read_confirmation_attempts_per_get": attempts / count,
    }


async def measure_crash(cluster, leader_id):
    async with AsyncExitStack() as stack:
        stubs = {
            node: service.RaftStub(await stack.enter_async_context(grpc.aio.insecure_channel(address)))
            for node, address in cluster.addresses.items() if node != leader_id
        }
        elections = {}
        crashed_at = time.monotonic()
        await cluster.stop(leader_id)
        async with asyncio.timeout(20):
            while True:
                leader = elected(await cluster.snapshot())
                if leader is not None:
                    identity = (leader["id"], leader["term"])
                    elections.setdefault(identity, time.monotonic())
                    reply = await stubs[leader["id"]].ServeClient(READ, timeout=2)
                    if reply.Success:
                        if reply.Data != VALUE:
                            raise RuntimeError(f"{cluster.mode} lost the committed value after a leader crash")
                        recovered_at = time.monotonic()
                        return {
                            "crash_to_get_s": recovered_at - crashed_at,
                            "election_to_get_s": recovered_at - elections[identity],
                        }
                await asyncio.sleep(PROBE_SECONDS)


def summarize_trials(trials):
    summary = {}
    for name in ("crash_to_get_s", "election_to_get_s"):
        samples = [trial[name] for trial in trials]
        summary[name] = {
            "median": round(statistics.median(samples), 3),
            "min": round(min(samples), 3), "max": round(max(samples), 3),
        }
    return summary


async def run(args):
    results = {
        "environment": {"os": platform.platform(), "python": platform.python_version(), "nodes": NODE_COUNT},
        "read_latency": [], "leader_crash": [],
    }
    with tempfile.TemporaryDirectory(prefix="raft-benchmark-") as directory:
        path = Path(directory)
        for index, rtt_ms in enumerate(args.rtt_ms):
            print(f"Measuring steady-state GETs at {rtt_ms:g} ms added peer RTT", file=sys.stderr, flush=True)
            async with cluster_pair(path / f"reads-{index}", "default", rtt_ms) as clusters:
                leaders = dict(zip(MODES, await asyncio.gather(*(seed(c) for c in clusters.values())), strict=True))
                result = {"added_peer_rtt_ms": rtt_ms}
                for mode in MODES[::1 if index % 2 == 0 else -1]:
                    result[mode] = await measure_reads(clusters[mode], leaders[mode], args.reads)
                results["read_latency"].append(result)

        for profile in ("default", "lab"):
            trials = {mode: [] for mode in MODES}
            for trial in range(args.failover_trials):
                print(f"Measuring {profile} leader crash {trial + 1}/{args.failover_trials}", file=sys.stderr, flush=True)
                async with cluster_pair(path / f"crash-{profile}-{trial}", profile, 0) as clusters:
                    leaders = await asyncio.gather(*(seed(cluster) for cluster in clusters.values()))
                    times = await asyncio.gather(*(
                        measure_crash(cluster, leader) for cluster, leader in zip(clusters.values(), leaders, strict=True)
                    ))
                    for mode, elapsed in zip(MODES, times, strict=True):
                        trials[mode].append(elapsed)
            results["leader_crash"].append({
                "profile": profile, "lease_ms": PROFILES[profile].lease_ms, "trials_per_mode": args.failover_trials,
                **{mode: summarize_trials(samples) for mode, samples in trials.items()},
            })
    print(json.dumps(results, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reads", type=int, default=120, help="measured sequential GETs per mode and RTT")
    parser.add_argument("--rtt-ms", type=float, nargs="+", default=[0, 20, 80], help="added peer RTT values")
    parser.add_argument("--failover-trials", type=int, default=5, help="paired crash trials per lease profile")
    args = parser.parse_args()
    if args.reads < 1 or args.failover_trials < 1 or any(not 0 <= rtt <= 500 for rtt in args.rtt_ms):
        parser.error("use positive sample counts and RTT values between 0 and 500 ms")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
