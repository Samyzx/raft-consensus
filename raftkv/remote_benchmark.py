"""Measure a running Raft cluster from a separate load-generator host."""

import argparse
import asyncio
import json
import math
import shlex
import statistics
import sys
import time
from collections import Counter
from contextlib import AsyncExitStack
from pathlib import Path
from uuid import uuid4

import grpc

from . import raft_pb2 as rpc
from . import raft_pb2_grpc as service
from .config import load_cluster


PROBE_INTERVAL = 0.05
VALUE = "committed"


async def inspect(stub):
    try:
        return await stub.Inspect(rpc.InspectRequest(), timeout=0.3)
    except grpc.aio.AioRpcError as error:
        if error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            return None
        raise


async def inspect_all(stubs):
    replies = await asyncio.gather(*(inspect(stub) for stub in stubs.values()))
    return dict(zip(stubs, replies, strict=True))


def leader_of(replies):
    leaders = [(node, status) for node, status in replies.items() if status and status.role == "leader"]
    return max(leaders, key=lambda leader: leader[1].term, default=(None, None))


async def request(stub, command, *, timeout=2.0):
    args = rpc.ServeClientArgs(Request=shlex.join(command))
    try:
        reply = await stub.ServeClient(args, timeout=timeout)
    except grpc.aio.AioRpcError as error:
        return error.code().name.lower(), ""
    return ("ok", reply.Data) if reply.Success else ("rejected", "")


async def seed(stub, key):
    status, _ = await request(stub, ("SET", key, VALUE))
    if status != "ok":
        raise RuntimeError(f"could not commit benchmark key {key}: {status}")


def percentiles(samples):
    if not samples:
        return None
    values = sorted(samples)
    return {
        "p50": round(statistics.median(values), 3),
        "p95": round(values[math.ceil(0.95 * len(values)) - 1], 3),
        "p99": round(values[math.ceil(0.99 * len(values)) - 1], 3),
    }


async def drive(stub, key, *, rate, seconds, write_percent, in_flight):
    loop = asyncio.get_running_loop()
    outcomes = {"GET": Counter(), "SET": Counter()}
    latency = {"GET": [], "SET": []}
    pending = set()
    prefix = uuid4().hex

    async def call(verb, index):
        command = ("SET", f"{prefix}-{index}", VALUE) if verb == "SET" else ("GET", key)
        started = time.perf_counter_ns()
        outcome, value = await request(stub, command)
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        if outcome == "ok" and verb == "GET" and value != VALUE:
            outcome = "wrong_value"
        outcomes[verb][outcome] += 1
        if outcome == "ok":
            latency[verb].append(elapsed_ms)

    started = loop.time()
    try:
        for index in range(round(rate * seconds)):
            scheduled = started + index / rate
            await asyncio.sleep(max(0, scheduled - loop.time()))
            verb = "SET" if (index * write_percent) % 100 + write_percent >= 100 else "GET"
            if len(pending) >= in_flight:
                outcomes[verb]["client_saturated"] += 1
                continue
            task = asyncio.create_task(call(verb, index))
            pending.add(task)
            task.add_done_callback(pending.discard)
        await asyncio.sleep(max(0, started + seconds - loop.time()))
        await asyncio.gather(*pending)
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    elapsed = loop.time() - started
    successful = sum(counts["ok"] for counts in outcomes.values())
    return {
        "elapsed_s": round(elapsed, 3),
        "successful_per_s": round(successful / elapsed, 2),
        "operations": {
            verb: {"outcomes": dict(counts), "success_latency_ms": percentiles(latency[verb])}
            for verb, counts in outcomes.items()
        },
    }


async def workload(stub, args, key):
    await seed(stub, key)
    options = {"rate": args.rps, "write_percent": args.write_percent, "in_flight": args.in_flight}
    if args.warmup:
        await drive(stub, key, seconds=args.warmup, **options)
    before = await inspect(stub)
    result = await drive(stub, key, seconds=args.seconds, **options)
    after = await inspect(stub)
    if before is None or after is None or before.term != after.term or not after.ready:
        raise RuntimeError("leadership changed during the workload; repeat the steady-state run")
    get_count = result["operations"]["GET"]["outcomes"].get("ok", 0)
    attempts = after.read_confirmation_requests - before.read_confirmation_requests
    return {
        **result, "offered_per_s": args.rps, "scheduled_s": args.seconds, "write_percent": args.write_percent,
        "get_confirmation_peer_attempts": attempts,
        "confirmation_peer_attempts_per_successful_get": round(attempts / get_count, 3) if get_count else None,
    }


async def watch_failover(stubs, original, term, key, wait):
    keys = [f"{key}-{index}" for index in range(10)]
    for item in keys:
        await seed(stubs[original], item)
    print(f"Monitoring leader {original}, term {term}. Ready for process crash.", file=sys.stderr, flush=True)
    elections = {}
    detected_at = None
    async with asyncio.timeout(wait):
        while True:
            replies = await inspect_all(stubs)
            now = time.monotonic()
            if replies[original] is None and detected_at is None:
                detected_at = now
            elif replies[original] and replies[original].role == "leader" and replies[original].term == term:
                detected_at = None
            node, status = leader_of({peer: reply for peer, reply in replies.items() if peer != original})
            if status and status.term > term:
                elections.setdefault((node, status.term), now)
            if detected_at is not None and status and status.ready and status.term > term:
                outcome, value = await request(stubs[node], ("GET", keys[0]))
                if outcome == "ok":
                    completed = time.monotonic()
                    remaining = await asyncio.gather(*(request(stubs[node], ("GET", item)) for item in keys[1:]))
                    if any(result != ("ok", VALUE) for result in [(outcome, value), *remaining]):
                        raise RuntimeError("replacement leader could not confirm all acknowledged writes")
                    return {
                        "original_leader": original, "replacement_leader": node, "term": status.term,
                        "unavailability_detection_to_get_s": round(completed - detected_at, 3),
                        "observed_election_to_get_s": round(completed - elections[(node, status.term)], 3),
                        "acknowledged_writes_verified": len(keys),
                    }
            await asyncio.sleep(PROBE_INTERVAL)


async def main(args):
    addresses = load_cluster(args.cluster)
    async with AsyncExitStack() as stack:
        stubs = {
            node: service.RaftStub(await stack.enter_async_context(grpc.aio.insecure_channel(address)))
            for node, address in addresses.items()
        }
        async with asyncio.timeout(20):
            while True:
                node, status = leader_of(await inspect_all(stubs))
                if status and status.ready:
                    break
                await asyncio.sleep(PROBE_INTERVAL)
        mode = "lease" if status.lease_remaining_ms else "standard"
        key = f"cloud-bench-{uuid4().hex}"
        if args.command == "load":
            result = await workload(stubs[node], args, key)
        else:
            result = await watch_failover(stubs, node, status.term, key, args.wait)
        return {"mode": mode, "replicas": len(stubs), **result}


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    load = commands.add_parser("load", help="run a fixed-offered-rate workload against the leader")
    load.add_argument("--rps", type=int, default=100)
    load.add_argument("--seconds", type=int, default=30)
    load.add_argument("--warmup", type=int, default=5)
    load.add_argument("--write-percent", type=int, choices=range(101), default=0, metavar="0-100")
    load.add_argument("--in-flight", type=int, default=128)
    watch = commands.add_parser("watch-failover", help="observe a leader process crash and test recovered data")
    watch.add_argument("--wait", type=int, default=120)
    args = parser.parse_args()
    if args.command == "load" and (min(args.rps, args.seconds, args.in_flight) < 1 or args.warmup < 0):
        parser.error("rate, duration, and concurrency must be positive; warmup can be zero")
    if args.command == "watch-failover" and args.wait < 1:
        parser.error("watch duration must be positive")
    try:
        print(json.dumps(asyncio.run(main(args)), indent=2))
    except (OSError, ValueError, RuntimeError) as error:
        print(str(error) or "benchmark timed out", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(cli())
