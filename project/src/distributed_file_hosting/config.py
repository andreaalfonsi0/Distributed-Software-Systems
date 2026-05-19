from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path

from distributed_file_hosting.models import PeerConfig


@dataclass(frozen=True)
class Settings:
    node_id: str
    rest_host: str
    rest_port: int
    rpc_host: str
    rpc_port: int
    data_dir: Path
    replication_factor: int
    write_quorum: int
    read_quorum: int
    health_check_interval: float
    rpc_timeout: float
    peers: tuple[PeerConfig, ...]

    @classmethod
    def from_env(cls) -> "Settings":
        node_id = os.getenv("NODE_ID", "node1")
        peers_payload = json.loads(os.getenv("PEERS", "[]"))
        peers = tuple(PeerConfig.from_dict(item) for item in peers_payload)
        cluster_size = len(peers) + 1
        replication_factor = min(int(os.getenv("REPLICATION_FACTOR", cluster_size)), cluster_size)
        default_quorum = max(1, math.floor(replication_factor / 2) + 1)
        write_quorum = min(int(os.getenv("WRITE_QUORUM", default_quorum)), replication_factor)
        read_quorum = min(int(os.getenv("READ_QUORUM", default_quorum)), replication_factor)
        return cls(
            node_id=node_id,
            rest_host=os.getenv("REST_HOST", "0.0.0.0"),
            rest_port=int(os.getenv("REST_PORT", "8000")),
            rpc_host=os.getenv("RPC_HOST", "0.0.0.0"),
            rpc_port=int(os.getenv("RPC_PORT", "9000")),
            data_dir=Path(os.getenv("DATA_DIR", "node_data") ),
            replication_factor=replication_factor,
            write_quorum=write_quorum,
            read_quorum=read_quorum,
            health_check_interval=float(os.getenv("HEALTH_CHECK_INTERVAL", "5")),
            rpc_timeout=float(os.getenv("RPC_TIMEOUT", "3")),
            peers=peers,
        )
