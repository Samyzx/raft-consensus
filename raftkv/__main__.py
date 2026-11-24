import argparse
import asyncio
import math
import sys
from dataclasses import replace
from pathlib import Path

import grpc

from .client import Client
from .config import PROFILES, load_cluster
from .node import Node


async def run(args) -> None:
    cluster = load_cluster(args.cluster)
    if args.command == "node":
        if args.id not in cluster:
            raise ValueError(f"unknown node ID {args.id!r}")
        if not math.isfinite(args.peer_rtt_ms) or args.peer_rtt_ms < 0:
            raise ValueError("peer RTT must be a finite, nonnegative number of milliseconds")
        listen = args.listen or cluster[args.id]
        timing = replace(PROFILES[args.profile], peer_rtt=args.peer_rtt_ms / 1000)
        node = Node(args.id, cluster, args.data_root, args.mode, timing)
        try:
            await node.start(listen)
            print(f"Node {args.id} listening on {listen} ({args.mode}, {args.profile})", flush=True)
            await node.task
        finally:
            await node.close()
    else:
        if not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError("timeout must be a positive number of seconds")
        command = [args.command.upper(), args.key]
        if args.command == "set":
            command.append(args.value)
        async with Client(cluster) as client:
            result = await client.request(*command, timeout=args.timeout)
        print(result if args.command == "get" else "OK")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run or query a Raft key-value cluster")
    parser.add_argument("--cluster", type=Path, default=Path("cluster.local.json"))
    commands = parser.add_subparsers(dest="command", required=True)
    node = commands.add_parser("node", help="start a node")
    node.add_argument("--id", required=True)
    node.add_argument("--mode", choices=("standard", "lease"), default="standard")
    node.add_argument("--profile", choices=PROFILES, default="default")
    node.add_argument("--data-root", type=Path, default=Path("."))
    node.add_argument("--listen", help="bind address when it differs from the advertised address")
    node.add_argument("--peer-rtt-ms", type=float, default=0, help="inject RTT on peer RPCs for benchmarks")
    for name in ("get", "set"):
        client = commands.add_parser(name)
        client.add_argument("key")
        if name == "set":
            client.add_argument("value")
        client.add_argument("--timeout", type=float, default=15.0)
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except grpc.aio.AioRpcError as error:
        print(f"{error.code().name}: {error.details()}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
