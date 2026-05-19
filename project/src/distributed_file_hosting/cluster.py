from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
from datetime import UTC, datetime
from typing import Any

from distributed_file_hosting.config import Settings
from distributed_file_hosting.lamport import LamportClock
from distributed_file_hosting.models import FileMetadata, FileVersion, PeerConfig, compare_versions
from distributed_file_hosting.rpc import JsonRpcClient, JsonRpcServer
from distributed_file_hosting.storage import FileStorage

logger = logging.getLogger(__name__)


class QuorumNotReachedError(RuntimeError):
    pass


class ClusterManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.clock = LamportClock()
        self.storage = FileStorage(settings.data_dir)
        self.rpc_server = JsonRpcServer(settings.rpc_host, settings.rpc_port, self.handle_rpc)
        self.peer_status: dict[str, dict[str, Any]] = {
            peer.node_id: {"reachable": False, "last_error": "not checked yet"}
            for peer in settings.peers
        }
        self._monitor_task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        self.storage.load(self.clock)
        await self.rpc_server.start()
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_peers())

    async def stop(self) -> None:
        self._running = False
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        await self.rpc_server.stop()

    def list_files(self) -> list[dict[str, Any]]:
        return self.storage.list_metadata()

    def health_snapshot(self) -> dict[str, Any]:
        return {
            "node_id": self.settings.node_id,
            "lamport_clock": self.clock.value,
            "replication_factor": self.settings.replication_factor,
            "write_quorum": self.settings.write_quorum,
            "read_quorum": self.settings.read_quorum,
            "peers": self.peer_status,
        }

    async def upload_file(self, logical_path: str, content: bytes, content_type: str) -> dict[str, Any]:
        version = FileVersion(
            lamport_counter=self.clock.tick(),
            version_node=self.settings.node_id,
            updated_by=self.settings.node_id,
            updated_at=datetime.now(UTC).isoformat(),
        )
        metadata, _ = self.storage.put_version(logical_path, content, content_type, version)
        replica_peers = self._replica_peers_for(logical_path)
        results = await asyncio.gather(
            *(self._replicate_to_peer(peer, logical_path, content, content_type, version) for peer in replica_peers),
            return_exceptions=True,
        )

        successful_peers: list[str] = []
        failed_peers: dict[str, str] = {}
        for peer, result in zip(replica_peers, results, strict=True):
            if isinstance(result, Exception):
                failed_peers[peer.node_id] = str(result)
            else:
                successful_peers.append(peer.node_id)

        acknowledgements = 1 + len(successful_peers)
        if acknowledgements < self.settings.write_quorum:
            raise QuorumNotReachedError(
                f"write quorum not reached: {acknowledgements}/{self.settings.write_quorum} acknowledgements"
            )

        return {
            "metadata": metadata,
            "replica_targets": [peer.node_id for peer in replica_peers],
            "acknowledged_replicas": successful_peers,
            "failed_replicas": failed_peers,
        }

    async def fetch_latest_file(self, logical_path: str) -> tuple[dict[str, Any], bytes]:
        local_metadata = self.storage.get_metadata(logical_path)
        candidates: list[tuple[FileMetadata, PeerConfig | None]] = []
        if local_metadata is not None:
            candidates.append((local_metadata, None))

        responses = await asyncio.gather(
            *(self._get_metadata_from_peer(peer, logical_path) for peer in self.settings.peers),
            return_exceptions=True,
        )
        for peer, result in zip(self.settings.peers, responses, strict=True):
            if isinstance(result, Exception) or result is None:
                continue
            candidates.append((FileMetadata.from_dict(result), peer))

        if not candidates:
            raise FileNotFoundError(logical_path)

        latest_metadata, source_peer = max(
            candidates,
            key=lambda item: (item[0].version.lamport_counter, item[0].version.version_node),
        )

        if source_peer is None:
            content = self.storage.read_content(logical_path)
            if content is None:
                raise FileNotFoundError(logical_path)
            return latest_metadata.to_dict(), content

        fetched = await JsonRpcClient.request(
            source_peer,
            "get_file_content",
            {"logical_path": logical_path},
            timeout=self.settings.rpc_timeout,
        )
        content = base64.b64decode(fetched["content_b64"])
        self.clock.update(latest_metadata.version.lamport_counter)
        self.storage.put_version(
            logical_path=logical_path,
            content=content,
            content_type=latest_metadata.content_type,
            version=latest_metadata.version,
        )
        return latest_metadata.to_dict(), content

    async def handle_rpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "ping":
            return {"node_id": self.settings.node_id, "status": "ok", "lamport_clock": self.clock.value}

        if method == "replicate_file":
            logical_path = str(params["logical_path"])
            version = FileVersion.from_dict(params["version"])
            content = base64.b64decode(params["content_b64"])
            content_type = str(params.get("content_type", "application/octet-stream"))
            self.clock.update(version.lamport_counter)
            metadata, stored = self.storage.put_version(logical_path, content, content_type, version)
            return {"stored": stored, "metadata": metadata}

        if method == "get_file_metadata":
            logical_path = str(params["logical_path"])
            metadata = self.storage.get_metadata(logical_path)
            return {"metadata": metadata.to_dict() if metadata is not None else None}

        if method == "get_file_content":
            logical_path = str(params["logical_path"])
            metadata = self.storage.get_metadata(logical_path)
            content = self.storage.read_content(logical_path)
            if metadata is None or content is None:
                raise FileNotFoundError(logical_path)
            return {
                "metadata": metadata.to_dict(),
                "content_b64": base64.b64encode(content).decode("utf-8"),
            }

        if method == "get_catalog":
            return {"catalog": self.storage.get_catalog()}

        raise ValueError(f"unknown rpc method: {method}")

    async def _replicate_to_peer(
        self,
        peer: PeerConfig,
        logical_path: str,
        content: bytes,
        content_type: str,
        version: FileVersion,
    ) -> None:
        await JsonRpcClient.request(
            peer,
            "replicate_file",
            {
                "logical_path": logical_path,
                "content_type": content_type,
                "content_b64": base64.b64encode(content).decode("utf-8"),
                "version": version.to_dict(),
            },
            timeout=self.settings.rpc_timeout,
        )

    async def _get_metadata_from_peer(self, peer: PeerConfig, logical_path: str) -> dict[str, Any] | None:
        response = await JsonRpcClient.request(
            peer,
            "get_file_metadata",
            {"logical_path": logical_path},
            timeout=self.settings.rpc_timeout,
        )
        return response["metadata"]

    def _replica_peers_for(self, logical_path: str) -> list[PeerConfig]:
        if self.settings.replication_factor <= 1:
            return []
        peers = list(self.settings.peers)
        if not peers:
            return []
        offset = int(hashlib.sha256(logical_path.encode("utf-8")).hexdigest(), 16) % len(peers)
        rotated = peers[offset:] + peers[:offset]
        return rotated[: self.settings.replication_factor - 1]

    async def _monitor_peers(self) -> None:
        while self._running:
            await asyncio.gather(*(self._check_peer(peer) for peer in self.settings.peers), return_exceptions=True)
            await asyncio.sleep(self.settings.health_check_interval)

    async def _check_peer(self, peer: PeerConfig) -> None:
        previous = self.peer_status.get(peer.node_id, {}).get("reachable", False)
        try:
            await JsonRpcClient.request(peer, "ping", {}, timeout=self.settings.rpc_timeout)
            self.peer_status[peer.node_id] = {"reachable": True, "last_error": None}
            if not previous:
                await self._synchronize_with_peer(peer)
        except Exception as exc:
            self.peer_status[peer.node_id] = {"reachable": False, "last_error": str(exc)}

    async def _synchronize_with_peer(self, peer: PeerConfig) -> None:
        try:
            response = await JsonRpcClient.request(peer, "get_catalog", {}, timeout=self.settings.rpc_timeout)
        except Exception as exc:
            logger.warning("catalog sync with %s failed: %s", peer.node_id, exc)
            return

        remote_catalog = {
            logical_path: FileMetadata.from_dict(item)
            for logical_path, item in response["catalog"].items()
        }
        local_catalog = {
            logical_path: FileMetadata.from_dict(item)
            for logical_path, item in self.storage.get_catalog().items()
        }

        for logical_path, local_metadata in local_catalog.items():
            remote_metadata = remote_catalog.get(logical_path)
            if remote_metadata is None or compare_versions(local_metadata.version, remote_metadata.version) > 0:
                content = self.storage.read_content(logical_path)
                if content is None:
                    continue
                try:
                    await self._replicate_to_peer(
                        peer,
                        logical_path,
                        content,
                        local_metadata.content_type,
                        local_metadata.version,
                    )
                except Exception as exc:
                    logger.warning("recovery push to %s for %s failed: %s", peer.node_id, logical_path, exc)

        for logical_path, remote_metadata in remote_catalog.items():
            local_metadata = local_catalog.get(logical_path)
            if local_metadata is not None and compare_versions(remote_metadata.version, local_metadata.version) <= 0:
                continue
            try:
                response = await JsonRpcClient.request(
                    peer,
                    "get_file_content",
                    {"logical_path": logical_path},
                    timeout=self.settings.rpc_timeout,
                )
                content = base64.b64decode(response["content_b64"])
                self.clock.update(remote_metadata.version.lamport_counter)
                self.storage.put_version(
                    logical_path=logical_path,
                    content=content,
                    content_type=remote_metadata.content_type,
                    version=remote_metadata.version,
                )
            except Exception as exc:
                logger.warning("recovery pull from %s for %s failed: %s", peer.node_id, logical_path, exc)
