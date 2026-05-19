from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PeerConfig:
    node_id: str
    host: str
    rest_port: int
    rpc_port: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PeerConfig":
        return cls(
            node_id=str(payload["node_id"]),
            host=str(payload["host"]),
            rest_port=int(payload["rest_port"]),
            rpc_port=int(payload["rpc_port"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "rest_port": self.rest_port,
            "rpc_port": self.rpc_port,
        }


@dataclass(frozen=True)
class FileVersion:
    lamport_counter: int
    version_node: str
    updated_by: str
    updated_at: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FileVersion":
        return cls(
            lamport_counter=int(payload["lamport_counter"]),
            version_node=str(payload["version_node"]),
            updated_by=str(payload["updated_by"]),
            updated_at=str(payload["updated_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "lamport_counter": self.lamport_counter,
            "version_node": self.version_node,
            "updated_by": self.updated_by,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class FileMetadata:
    logical_path: str
    stored_name: str
    size: int
    checksum: str
    content_type: str
    version: FileVersion

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FileMetadata":
        return cls(
            logical_path=str(payload["logical_path"]),
            stored_name=str(payload["stored_name"]),
            size=int(payload["size"]),
            checksum=str(payload["checksum"]),
            content_type=str(payload["content_type"]),
            version=FileVersion.from_dict(payload["version"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "logical_path": self.logical_path,
            "stored_name": self.stored_name,
            "size": self.size,
            "checksum": self.checksum,
            "content_type": self.content_type,
            "version": self.version.to_dict(),
        }


def compare_versions(left: FileVersion, right: FileVersion) -> int:
    left_key = (left.lamport_counter, left.version_node)
    right_key = (right.lamport_counter, right.version_node)
    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0
