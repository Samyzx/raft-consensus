import asyncio
import json
import logging
import random
import shlex
import sqlite3
import time
from pathlib import Path

import grpc

from . import raft_pb2 as rpc
from . import raft_pb2_grpc as service
from .config import Timing
from .lease import LeaderLease
from .storage import Storage, operation

MAX_REQUEST_BYTES = 16 * 1024
MAX_REQUEST_ID_BYTES = 128
APPEND_BATCH = 64


class Node(service.RaftServicer):
    """All protocol state belongs to one asyncio event loop."""

    def __init__(
        self, node_id: str, cluster: dict[str, str], data_root: Path, mode: str, timing: Timing
    ):
        self.id = node_id
        self.cluster = cluster
        self.peers = tuple(peer for peer in cluster if peer != node_id)
        self.majority = len(cluster) // 2 + 1
        self.timing = timing
        self.lease_config_ms = timing.lease_ms if mode == "lease" else 0
        directory = data_root / f"logs_node_{node_id}"
        identity = json.dumps([node_id, cluster, mode, self.lease_config_ms], sort_keys=True)
        self.storage = Storage(directory, identity)
        self.term, self.voted_for, self.commit_index, self.log = self.storage.load()
        self.log.insert(0, rpc.LogEntry())
        self.applied = 0
        self.values: dict[str, str] = {}
        self.request_ids: dict[str, tuple[str, str]] = {}

        self.logger = logging.Logger(f"raft.node.{node_id}", logging.INFO)
        self.trace = logging.FileHandler(directory / "dump.txt", encoding="utf-8")
        self.trace.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        self.logger.addHandler(self.trace)
        self.role = "follower"
        self.leader_id = ""
        self.lease = (
            LeaderLease(timing, self.majority, time.monotonic(), self.storage.restarted)
            if self.lease_config_ms
            else None
        )
        self.noop_index = 0
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.wake = asyncio.Event()
        self.changed = asyncio.Event()
        self.reads: list[asyncio.Future] = []
        self.replicate_soon = False
        self.replication_rounds = 0
        self.fatal_error: sqlite3.Error | None = None
        self.server = None
        self.task = None
        self.channels: dict[str, grpc.aio.Channel] = {}
        self.stubs: dict[str, service.RaftStub] = {}
        self._reset_election()
        self._apply(log_events=False)

    async def start(self, listen: str) -> None:
        self.server = grpc.aio.server(options=(("grpc.so_reuseport", 0),))
        service.add_RaftServicer_to_server(self, self.server)
        self.server.add_insecure_port(listen)
        await self.server.start()
        self.channels = {peer: grpc.aio.insecure_channel(self.cluster[peer]) for peer in self.peers}
        self.stubs = {peer: service.RaftStub(channel) for peer, channel in self.channels.items()}
        self._reset_election()
        self.task = asyncio.create_task(self._run())

    async def close(self) -> None:
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.server is not None:
            await self.server.stop(0)
        for channel in self.channels.values():
            await channel.close()
        self.trace.close()
        self.storage.close()

    def _reset_election(self) -> None:
        self.election_deadline = time.monotonic() + random.uniform(
            self.timing.election_min, self.timing.election_max
        )
        self.wake.set()

    def _notify(self) -> None:
        old_event = self.changed
        self.changed = asyncio.Event()
        old_event.set()

    def _save(
        self,
        *,
        term: int,
        vote: str | None,
        commit: int,
        keep: int | None = None,
        append: tuple[rpc.LogEntry, ...] = (),
    ) -> None:
        try:
            self.storage.save(term, vote, commit, keep=keep, append=append)
        except sqlite3.Error as error:
            self.fatal_error = error
            self.wake.set()
            self._notify()
            raise
        self.term, self.voted_for, self.commit_index = term, vote, commit
        if keep is not None:
            self.log[keep + 1:] = append
        self._apply()
        self.storage.export(include_log=keep is not None)

    def _apply(self, *, log_events: bool = True) -> None:
        while self.applied < self.commit_index:
            self.applied += 1
            entry = self.log[self.applied]
            if not entry.noop and (not entry.request_id or entry.request_id not in self.request_ids):
                self.values[entry.key] = entry.value
                if entry.request_id:
                    self.request_ids[entry.request_id] = (entry.key, entry.value)
            if log_events:
                self.logger.info(
                    "Node %s (%s) committed the entry %s to the state machine.",
                    self.id, self.role, operation(entry),
                )
        self._notify()

    def _follow(self, term: int, leader: str = "") -> None:
        if self.role == "leader":
            self.logger.info("%s Stepping down", self.id)
        self.role, self.leader_id = "follower", leader
        if term > self.term:
            self._save(term=term, vote=None, commit=self.commit_index)
        for read in self.reads:
            if not read.done():
                read.set_result(False)
        self.reads.clear()
        self._reset_election()
        self._notify()

    def _expire_lease(self) -> None:
        if self.role == "leader" and self.lease and self.lease.expired(time.monotonic()):
            self.logger.info("Leader %s lease renewal failed. Stepping Down.", self.id)
            self._follow(self.term)

    async def _run(self) -> None:
        while self.fatal_error is None:
            self.wake.clear()
            self._expire_lease()
            now = time.monotonic()
            if self.role == "leader":
                deadline = self.heartbeat_deadline
                if self.lease:
                    lease = self.lease
                    if not self.noop_index:
                        deadline = min(deadline, lease.wait_until)
                    else:
                        deadline = min(deadline, lease.valid_until or lease.acquire_until)
                if self.replicate_soon or now >= deadline:
                    await self._replicate()
                    continue
            else:
                deadline = self.election_deadline
                if now >= deadline:
                    await self._elect()
                    continue
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=max(0, deadline - now))
            except TimeoutError:
                pass
        raise self.fatal_error

    async def _elect(self) -> None:
        self.logger.info("Node %s election timer timed out, Starting election.", self.id)
        self.role, self.leader_id = "candidate", ""
        self._save(term=self.term + 1, vote=self.id, commit=self.commit_index)
        self._reset_election()
        term = self.term
        request = rpc.VoteRequest(
            term=term, candidate_id=self.id, last_log_index=len(self.log) - 1,
            last_log_term=self.log[-1].term, lease_config_ms=self.lease_config_ms,
        )

        async def ask(peer: str):
            reply = await self._call(peer, "RequestVote", request)
            return reply, time.monotonic()

        replies = await asyncio.gather(*(ask(peer) for peer in self.peers))
        votes = 1
        for reply, received in replies:
            if reply is None:
                continue
            if reply.term > self.term:
                self._follow(reply.term)
            if reply.term == term and reply.granted:
                votes += 1
                if self.lease:
                    self.lease.observe(reply.old_lease_remaining_ms, received)
        if self.role == "candidate" and self.term == term and votes >= self.majority:
            self._lead()

    def _lead(self) -> None:
        self.role, self.leader_id = "leader", self.id
        self.noop_index = 0
        self.next_index = dict.fromkeys(self.peers, len(self.log))
        self.match_index = dict.fromkeys(self.peers, 0)
        now = time.monotonic()
        self.heartbeat_deadline = now
        self.replicate_soon = True
        self.logger.info("Node %s became the leader for term %s.", self.id, self.term)
        if self.lease:
            self.lease.elected(now)
            if self.lease.wait_until > now:
                self.logger.info("New Leader waiting for Old Leader Lease to timeout.")
        self._notify()

    async def _call(self, peer: str, method: str, request):
        try:
            return await getattr(self.stubs[peer], method)(request, timeout=self.timing.peer_timeout)
        except grpc.aio.AioRpcError as error:
            self.logger.info("Error occurred while sending RPC to Node %s. %s", peer, error.code().name)
            return None

    async def RequestVote(self, request, context):
        if (
            self.fatal_error is not None or request.candidate_id not in self.peers
            or request.lease_config_ms != self.lease_config_ms
        ):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "peer configuration mismatch")
        if request.term > self.term:
            self._follow(request.term)
        candidate_log = (request.last_log_term, request.last_log_index)
        local_log = (self.log[-1].term, len(self.log) - 1)
        granted = (
            request.term == self.term
            and self.voted_for in (None, request.candidate_id)
            and candidate_log >= local_log
        )
        if granted:
            if self.voted_for != request.candidate_id:
                self._save(term=self.term, vote=request.candidate_id, commit=self.commit_index)
            self._reset_election()
        self.logger.info(
            "Vote %s for Node %s in term %s.", "granted" if granted else "denied",
            request.candidate_id, request.term,
        )
        remaining = self.lease.remaining_ms(time.monotonic()) if self.lease and granted else 0
        return rpc.VoteReply(term=self.term, granted=granted, old_lease_remaining_ms=remaining)

    async def AppendEntries(self, request, context):
        if (
            self.fatal_error is not None or request.leader_id not in self.peers
            or request.lease_config_ms != self.lease_config_ms
            or request.lease_duration_ms not in (0, self.lease_config_ms)
        ):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "peer configuration mismatch")
        if request.term < self.term:
            return self._append_reply(request, False)
        if request.term > self.term or self.role != "follower":
            self._follow(request.term, request.leader_id)
        else:
            self.leader_id = request.leader_id
            self._reset_election()
        prev = request.prev_log_index
        if prev >= len(self.log) or self.log[prev].term != request.prev_log_term:
            return self._append_reply(request, False)

        keep = prev
        for entry in request.entries:
            if keep + 1 >= len(self.log) or self.log[keep + 1].term != entry.term:
                break
            keep += 1
        append = tuple(request.entries[keep - prev:])
        if append and keep < self.commit_index:
            return self._append_reply(request, False)
        commit = max(self.commit_index, min(request.leader_commit, prev + len(request.entries)))
        if append or commit != self.commit_index:
            self._save(
                term=self.term, vote=self.voted_for, commit=commit,
                keep=keep if append else None, append=append,
            )
        if self.lease:
            self.lease.observe(request.lease_duration_ms, time.monotonic())
        return self._append_reply(request, True)

    def _append_reply(self, request, success: bool):
        self.logger.info(
            "Node %s %s AppendEntries RPC from %s.", self.id,
            "accepted" if success else "rejected", request.leader_id,
        )
        return rpc.AppendReply(term=self.term, success=success)

    async def _replicate(self) -> None:
        self.replicate_soon = False
        self.replication_rounds += 1
        now = time.monotonic()
        term = self.term
        lease = self.lease
        self.heartbeat_deadline = now + self.timing.heartbeat
        if not self.noop_index and (lease is None or now >= lease.wait_until):
            self.noop_index = self._append(rpc.LogEntry(term=term, noop=True))
            if lease:
                lease.activate(time.monotonic())
        duration = 0
        if self.noop_index and lease:
            duration = self.lease_config_ms
            self.logger.info("Leader %s sending heartbeat & Renewing Lease", self.id)
            lease.acknowledge(self.id, lease.offer(time.monotonic()))
        else:
            self.logger.info("Leader %s sending heartbeat", self.id)

        if self.noop_index:
            self._advance_commit()
        reads, self.reads = self.reads, []
        replies = await asyncio.gather(*(self._replicate_peer(peer, term, duration) for peer in self.peers))
        confirmed = self.role == "leader" and self.term == term and 1 + sum(replies) >= self.majority
        for read in reads:
            if not read.done():
                read.set_result(confirmed)

    async def _replicate_peer(self, peer: str, term: int, duration: int) -> bool:
        if self.role != "leader" or self.term != term or self.fatal_error is not None:
            return False
        start = self.next_index[peer]
        entries = self.log[start:start + APPEND_BATCH] if self.noop_index else []
        request = rpc.AppendRequest(
            term=term, leader_id=self.id, prev_log_index=start - 1, prev_log_term=self.log[start - 1].term,
            entries=entries, leader_commit=self.commit_index, lease_duration_ms=duration,
            lease_config_ms=self.lease_config_ms,
        )
        lease_deadline = self.lease.offer(time.monotonic()) if duration and self.lease else 0.0
        reply = await self._call(peer, "AppendEntries", request)
        if reply is None:
            return False
        if reply.term > self.term:
            self._follow(reply.term)
        self._expire_lease()
        if self.role != "leader" or self.term != term or reply.term != term or self.fatal_error is not None:
            return False
        if not reply.success:
            self.next_index[peer] = max(1, start - 1)
            self.replicate_soon |= bool(self.noop_index and start > 1)
            return False

        matched = start - 1 + len(entries)
        self.match_index[peer] = matched
        self.next_index[peer] = matched + 1
        if self.noop_index:
            if self.lease and lease_deadline:
                self.lease.acknowledge(peer, lease_deadline)
            self._advance_commit()
            self.replicate_soon |= self.next_index[peer] < len(self.log)
        return True

    def _append(self, entry: rpc.LogEntry) -> int:
        self._save(
            term=self.term, vote=self.voted_for, commit=self.commit_index,
            keep=len(self.log) - 1, append=(entry,),
        )
        return len(self.log) - 1

    def _advance_commit(self) -> None:
        matches = sorted([len(self.log) - 1, *self.match_index.values()], reverse=True)
        commit = matches[self.majority - 1]
        if commit > self.commit_index and self.log[commit].term == self.term:
            self._save(term=self.term, vote=self.voted_for, commit=commit)
            self.replicate_soon = True

    def _can_serve(self, term: int) -> bool:
        self._expire_lease()
        return self._ready(term, time.monotonic())

    def _ready(self, term: int, now: float) -> bool:
        return (
            self.fatal_error is None and self.role == "leader" and self.term == term
            and 0 < self.noop_index <= self.applied
            and (self.lease is None or now < self.lease.valid_until)
        )

    async def Inspect(self, request, context):
        now = time.monotonic()
        lease = self.lease if self.role == "leader" else None
        return rpc.NodeStatus(
            role=self.role, term=self.term, log_index=len(self.log) - 1,
            commit_index=self.commit_index, leader_id=self.leader_id, ready=self._ready(self.term, now),
            lease_remaining_ms=max(0, int((lease.valid_until - now) * 1000)) if lease else 0,
            handoff_remaining_ms=max(0, int((lease.wait_until - now) * 1000)) if lease else 0,
            replication_rounds=self.replication_rounds,
        )

    def _client_reply(self, success: bool = False, data: str = ""):
        return rpc.ServeClientReply(Data=data, LeaderID=self.leader_id, Success=success)

    async def ServeClient(self, request, context):
        try:
            if (
                len(request.Request.encode()) > MAX_REQUEST_BYTES
                or len(request.RequestID.encode()) > MAX_REQUEST_ID_BYTES
            ):
                raise ValueError("request is too large")
            fields = shlex.split(request.Request)
            if (len(fields), fields[0] if fields else "") not in ((2, "GET"), (3, "SET")):
                raise ValueError("expected GET key or SET key value")
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

        term = self.term
        if not self._can_serve(term):
            return self._client_reply()
        self.logger.info("Node %s (leader) received an %s request.", self.id, shlex.join(fields))
        try:
            async with asyncio.timeout(self.timing.client_attempt):
                if fields[0] == "GET":
                    return await self._read(term, fields[1])
                return await self._write(term, fields[1], fields[2], request.RequestID, context)
        except TimeoutError:
            return self._client_reply()

    async def _read(self, term: int, key: str):
        if self.lease is None:
            confirmed = asyncio.get_running_loop().create_future()
            self.reads.append(confirmed)
            self.replicate_soon = True
            self.wake.set()
            if not await confirmed:
                return self._client_reply()
        if not self._can_serve(term):
            return self._client_reply()
        return self._client_reply(True, self.values.get(key, ""))

    async def _write(self, term: int, key: str, value: str, request_id: str, context):
        if not self._can_serve(term):
            return self._client_reply()
        if request_id and request_id in self.request_ids:
            if self.request_ids[request_id] != (key, value):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request ID already used for a different SET")
            return self._client_reply(True)
        index = self._append(rpc.LogEntry(term=term, key=key, value=value, request_id=request_id))
        self.replicate_soon = True
        self.wake.set()
        while self._can_serve(term):
            if self.applied >= index:
                if request_id and self.request_ids[request_id] != (key, value):
                    await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request ID already used for a different SET")
                return self._client_reply(True)
            await self.changed.wait()
        return self._client_reply()
