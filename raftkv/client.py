import asyncio
import shlex
import time
from uuid import uuid4

import grpc

from . import raft_pb2 as rpc
from . import raft_pb2_grpc as service


class Client:
    def __init__(self, cluster: dict[str, str], *, rpc_timeout: float = 2.0):
        self.channels = {node: grpc.aio.insecure_channel(address) for node, address in cluster.items()}
        self.stubs = {node: service.RaftStub(channel) for node, channel in self.channels.items()}
        self.leader_id = ""
        self.rpc_timeout = rpc_timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        for channel in self.channels.values():
            await channel.close()

    async def request(self, *command: str, timeout: float = 15.0, request_id: str | None = None) -> str:
        if request_id is None and command[0] == "SET":
            request_id = uuid4().hex
        request = rpc.ServeClientArgs(Request=shlex.join(command), RequestID=request_id or "")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            remaining_nodes = list(self.stubs)
            while remaining_nodes:
                remaining_time = deadline - time.monotonic()
                if remaining_time <= 0:
                    break
                node = self.leader_id if self.leader_id in remaining_nodes else remaining_nodes[0]
                remaining_nodes.remove(node)
                try:
                    reply = await self.stubs[node].ServeClient(request, timeout=min(self.rpc_timeout, remaining_time))
                except grpc.aio.AioRpcError as error:
                    if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                        raise
                    if node == self.leader_id:
                        self.leader_id = ""
                    continue
                self.leader_id = reply.LeaderID if reply.LeaderID in self.stubs else ""
                if reply.Success:
                    return reply.Data
            await asyncio.sleep(min(0.1, max(0, deadline - time.monotonic())))
        raise TimeoutError("no successful reply before the deadline")
