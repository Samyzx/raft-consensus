import math

from .config import Timing


class LeaderLease:
    def __init__(self, timing: Timing, majority: int, now: float, restarted: bool):
        self.timing = timing
        self.majority = majority
        # A reboot may lose monotonic clock readings. Cover any pre-restart grant.
        self.known_until = now + timing.conservative_wait(timing.lease_seconds) if restarted else 0.0
        self.wait_until = 0.0
        self.acquire_until = 0.0
        self.valid_until = 0.0
        self.acknowledged: dict[str, float] = {}

    def remaining_ms(self, now: float) -> int:
        return max(0, math.ceil((self.known_until - now) * 1000))

    def observe(self, duration_ms: int, now: float) -> None:
        if duration_ms:
            deadline = now + self.timing.conservative_wait(duration_ms / 1000)
            self.known_until = max(self.known_until, deadline)

    def elected(self, now: float) -> None:
        self.wait_until = max(now, self.known_until)
        self.acquire_until = 0.0
        self.valid_until = 0.0
        self.acknowledged.clear()

    def activate(self, now: float) -> None:
        self.acquire_until = now + self.timing.lease_seconds

    def offer(self, now: float) -> float:
        deadline = now + self.timing.lease_seconds
        self.known_until = max(self.known_until, deadline)
        return deadline

    def acknowledge(self, node_id: str, deadline: float) -> None:
        self.acknowledged[node_id] = max(self.acknowledged.get(node_id, 0.0), deadline)
        if len(self.acknowledged) >= self.majority:
            self.valid_until = sorted(self.acknowledged.values(), reverse=True)[self.majority - 1]

    def expired(self, now: float) -> bool:
        deadline = self.valid_until or self.acquire_until
        return bool(deadline and now >= deadline)
