import json
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Timing:
    heartbeat: float = 0.5
    election_min: float = 3.0
    election_max: float = 5.0
    peer_timeout: float = 0.25
    lease_ms: int = 2000
    max_clock_drift: float = 0.0005
    client_attempt: float = 1.5

    @property
    def lease_seconds(self) -> float:
        return self.lease_ms / 1000

    def conservative_wait(self, seconds: float) -> float:
        drift = self.max_clock_drift
        return seconds * (1 + drift) / (1 - drift)


PROFILES = {
    "default": Timing(),
    "assignment": Timing(heartbeat=1.0, election_min=5.0, election_max=10.0, lease_ms=4000),
}


def load_cluster(path: Path) -> dict[str, str]:
    cluster = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(cluster, dict) or not cluster:
        raise ValueError("cluster must be a JSON object mapping node IDs to host:port")
    for node_id, address in cluster.items():
        if not re.fullmatch(r"[A-Za-z0-9_-]+", node_id):
            raise ValueError(f"invalid node ID: {node_id!r}")
        if not isinstance(address, str):
            raise ValueError(f"invalid address for node {node_id}")
        host, separator, port = address.rpartition(":")
        if not separator or not host or not port.isascii() or not port.isdecimal() or not 0 < int(port) < 65536:
            raise ValueError(f"invalid host:port for node {node_id}: {address!r}")
    if len(set(cluster.values())) != len(cluster):
        raise ValueError("each node must have a distinct address")
    return cluster
