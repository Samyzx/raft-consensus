import asyncio
import socket
import time
from contextlib import asynccontextmanager
from dataclasses import replace

import grpc
import pytest

from raftkv import raft_pb2 as rpc
from raftkv import raft_pb2_grpc as service
from raftkv.client import Client
from raftkv.config import Timing
from raftkv.lease import LeaderLease
from raftkv.node import Node


FAST = Timing(heartbeat=0.12, election_min=0.55, election_max=0.85, peer_timeout=0.1,
              lease_ms=1450, client_attempt=0.4)


def addresses(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return {str(index): f"127.0.0.1:{sock.getsockname()[1]}" for index, sock in enumerate(sockets)}
    finally:
        for sock in sockets:
            sock.close()


async def eventually(check, *, timeout=8):
    async with asyncio.timeout(timeout):
        while not (result := check()):
            await asyncio.sleep(0.02)
    return result


class Cluster:
    def __init__(self, path, mode, timing, count=5):
        self.path, self.mode, self.timing = path, mode, timing
        self.addresses = addresses(count)
        self.nodes = {}

    async def start(self, node_id):
        node = Node(node_id, self.addresses, self.path, self.mode, self.timing)
        self.nodes[node_id] = node
        await node.start(self.addresses[node_id])
        return node

    async def stop(self, *ids):
        await asyncio.gather(*(self.nodes.pop(node_id).close() for node_id in ids))

    async def leader(self):
        return await eventually(lambda: next(
            (node for node in self.nodes.values() if node._can_serve(node.term)), None
        ))


@asynccontextmanager
async def running_cluster(path, mode, timing=FAST, *, count=5):
    cluster = Cluster(path, mode, timing, count)
    try:
        for node_id in cluster.addresses:
            await cluster.start(node_id)
        yield cluster
    finally:
        await cluster.stop(*cluster.nodes)


def test_standard_replication_recovery_and_quorum_reads(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path, "standard") as cluster:
            leader = await cluster.leader()
            followers = [node_id for node_id in cluster.nodes if node_id != leader.id]
            await eventually(lambda: all(node.applied == 1 for node in cluster.nodes.values()))
            async with Client(cluster.addresses) as client:
                assert await client.request("GET", "missing") == ""
                await cluster.stop(*followers[:2])
                await client.request("SET", "team name", "first", request_id="first-write")
                await client.request("SET", "team name", "new value")
                await client.request("SET", "team name", "first", request_id="first-write")
                assert await client.request("GET", "team name") == "new value"
                for node_id in followers[:2]:
                    await cluster.start(node_id)
                await eventually(lambda: all(node.values.get("team name") == "new value"
                                             for node in cluster.nodes.values()))
                former_id = leader.id
                await cluster.stop(former_id)
                assert await client.request("GET", "team name") == "new value"
                leader = await cluster.leader()
                assert leader.id != former_id
                await cluster.start(former_id)
                followers = [node_id for node_id in cluster.nodes if node_id != leader.id]
                await cluster.stop(*followers[:3])
                with pytest.raises(TimeoutError):
                    await client.request("GET", "team name", timeout=0.6)
                with pytest.raises(TimeoutError):
                    await client.request("SET", "team name", "uncommitted", timeout=0.6)
                assert leader.values["team name"] == "new value"

    asyncio.run(scenario())


def test_lease_handoff_and_restart(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path, "lease") as cluster:
            old = await cluster.leader()
            async with Client(cluster.addresses) as client:
                await client.request("SET", "x", "before crash")
                await eventually(lambda: all(node.values.get("x") == "before crash"
                                             for node in cluster.nodes.values()))
                await eventually(lambda: old.lease.valid_until - time.monotonic() > 1.2)
                old_expiry = old.lease.valid_until
                successor = next(node for node in cluster.nodes.values() if node is not old)
                await cluster.stop(old.id)
                # Trigger the election during the old lease instead of racing randomized timers.
                successor.election_deadline = time.monotonic()
                successor.wake.set()
                await eventually(lambda: successor.role == "leader")
                assert time.monotonic() < old_expiry
                assert not successor._can_serve(successor.term)
                async with grpc.aio.insecure_channel(cluster.addresses[successor.id]) as channel:
                    stub = service.RaftStub(channel)
                    attempted = await asyncio.gather(*(
                        stub.ServeClient(rpc.ServeClientArgs(Request=command), timeout=1)
                        for command in ("GET x", "SET x unsafe")
                    ))
                    assert all(not reply.Success for reply in attempted)
                assert "waiting for Old Leader Lease" in (tmp_path / f"logs_node_{successor.id}" / "dump.txt").read_text()
                assert await client.request("GET", "x") == "before crash"
                assert time.monotonic() >= old_expiry
                assert await cluster.leader() is successor
                restarted = await cluster.start(old.id)
                await client.request("SET", "x", "after crash")
                await eventually(lambda: restarted.values.get("x") == "after crash")
                assert restarted.role == "follower"

    asyncio.run(scenario())


def test_lease_reads_after_quorum_loss(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path, "lease") as cluster:
            leader = await cluster.leader()
            async with Client(cluster.addresses) as client:
                await client.request("SET", "x", "committed")
            followers = [node_id for node_id in cluster.nodes if node_id != leader.id]
            async with grpc.aio.insecure_channel(cluster.addresses[leader.id]) as channel:
                stub = service.RaftStub(channel)
                read = rpc.ServeClientArgs(Request="GET x")
                write = rpc.ServeClientArgs(Request="SET x uncommitted")
                assert (await stub.ServeClient(read, timeout=1)).Data == "committed"
                await eventually(lambda: leader.lease.valid_until - time.monotonic() > 0.9)
                await cluster.stop(*followers[:3])
                reply = await stub.ServeClient(read, timeout=1)
                assert reply.Success and reply.Data == "committed"
                assert not (await stub.ServeClient(write, timeout=1)).Success
                assert leader.values["x"] == "committed"
                await eventually(lambda: leader.role != "leader")
                for request in (read, write):
                    assert not (await stub.ServeClient(request, timeout=1)).Success
            dump = (tmp_path / f"logs_node_{leader.id}" / "dump.txt").read_text()
            assert "lease renewal failed. Stepping Down." in dump

    asyncio.run(scenario())


def test_vote_persistence_and_conflicting_suffix(tmp_path):
    async def scenario():
        cluster = Cluster(tmp_path, "standard", replace(FAST, election_min=30, election_max=30))
        follower_id = "4"
        try:
            follower = await cluster.start(follower_id)
            async with grpc.aio.insecure_channel(cluster.addresses[follower_id]) as channel:
                stub = service.RaftStub(channel)
                initial = await stub.AppendEntries(rpc.AppendRequest(
                    term=1, leader_id="0", entries=[rpc.LogEntry(term=1, noop=True),
                        rpc.LogEntry(term=1, key="x", value="committed"),
                        rpc.LogEntry(term=1, key="x", value="uncommitted")], leader_commit=2,
                ), timeout=1)
                assert initial.success and follower.values["x"] == "committed"
                stale = await stub.RequestVote(rpc.VoteRequest(
                    term=2, candidate_id="1", last_log_term=1, last_log_index=2,
                ), timeout=1)
                assert not stale.granted
                vote = await stub.RequestVote(rpc.VoteRequest(
                    term=2, candidate_id="0", last_log_term=1, last_log_index=3,
                ), timeout=1)
                assert vote.granted
            await cluster.stop(follower_id)
            follower = await cluster.start(follower_id)
            assert follower.values == {"x": "committed"}
            async with grpc.aio.insecure_channel(cluster.addresses[follower_id]) as channel:
                stub = service.RaftStub(channel)
                duplicate = await stub.RequestVote(rpc.VoteRequest(
                    term=2, candidate_id="1", last_log_term=2, last_log_index=3,
                ), timeout=1)
                assert not duplicate.granted
                heartbeat = await stub.AppendEntries(rpc.AppendRequest(
                    term=3, leader_id="1", prev_log_index=2, prev_log_term=1, leader_commit=3,
                ), timeout=1)
                assert heartbeat.success and follower.values == {"x": "committed"}
                repaired = await stub.AppendEntries(rpc.AppendRequest(
                    term=3, leader_id="1", prev_log_index=2, prev_log_term=1, leader_commit=3,
                    entries=[rpc.LogEntry(term=3, key="x", value="repaired")],
                ), timeout=1)
                assert repaired.success and follower.values == {"x": "repaired"}
                assert "uncommitted" not in (tmp_path / "logs_node_4" / "logs.txt").read_text()
        finally:
            await cluster.stop(*cluster.nodes)

    asyncio.run(scenario())


def test_lease_watermark_uses_send_times_and_retains_promises():
    timing = Timing()
    lease = LeaderLease(timing, majority=3, now=10.0, restarted=False)
    lease.elected(10.0)
    lease.activate(10.0)
    lease.acknowledge("0", lease.offer(10.0))
    lease.acknowledge("1", lease.offer(10.1))
    assert lease.valid_until == 0.0
    lease.acknowledge("2", lease.offer(10.2))
    assert lease.valid_until == 12.0
    lease.acknowledge("0", lease.offer(10.5))
    assert lease.valid_until == 12.1
    assert lease.expired(12.1)
    lease.elected(12.2)
    assert lease.wait_until == 12.5
    assert lease.valid_until == 0.0

    restarted = LeaderLease(timing, majority=3, now=20.0, restarted=True)
    assert restarted.remaining_ms(20.0) > timing.lease_ms
    restarted.observe(2500, now=20.0)
    restarted.observe(100, now=20.1)
    assert restarted.remaining_ms(20.1) > 2400


@pytest.mark.parametrize("count", [5, 7])
@pytest.mark.parametrize("mode", ["standard", "lease"])
def test_tradeoff_standard_waits_for_peer_acks_while_a_lease_reads_locally(tmp_path, count, mode):
    async def scenario():
        async with running_cluster(tmp_path, mode, count=count) as cluster:
            leader = await cluster.leader()
            async with Client(cluster.addresses) as client:
                await client.request("SET", "read", "committed")
            await eventually(lambda: all(node.values.get("read") == "committed"
                                         for node in cluster.nodes.values()))
            if leader.lease:
                await eventually(lambda: leader.lease.valid_until - time.monotonic() > 1.1)

            original_call = leader._call
            gated = asyncio.Event()
            one_replied = asyncio.Event()
            release = asyncio.Event()
            allowed_peer = leader.peers[0]

            async def hold_append_entries(peer, method, request):
                if method == "AppendEntries" and peer != allowed_peer:
                    gated.set()
                    await release.wait()
                reply = await original_call(peer, method, request)
                if method == "AppendEntries" and peer == allowed_peer and reply and reply.success:
                    one_replied.set()
                return reply

            leader._call = hold_append_entries
            try:
                async with grpc.aio.insecure_channel(cluster.addresses[leader.id]) as channel:
                    stub = service.RaftStub(channel)
                    before = await stub.Inspect(rpc.InspectRequest(), timeout=1)
                    if mode == "lease":
                        await asyncio.wait_for(gated.wait(), timeout=1)
                    read = asyncio.ensure_future(
                        stub.ServeClient(rpc.ServeClientArgs(Request="GET read"), timeout=2)
                    )
                    if mode == "standard":
                        await asyncio.wait_for(gated.wait(), timeout=1)
                        await asyncio.wait_for(one_replied.wait(), timeout=1)
                        assert not read.done(), "the leader and one peer must not confirm a quorum read"
                        release.set()
                    reply = await read
                    assert reply.Success and reply.Data == "committed"
                    after = await stub.Inspect(rpc.InspectRequest(), timeout=1)
                    attempts = after.read_confirmation_requests - before.read_confirmation_requests
                    assert attempts == (count - 1 if mode == "standard" else 0)
            finally:
                release.set()
                leader._call = original_call

    asyncio.run(scenario())
