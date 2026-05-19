from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path, PurePosixPath
from typing import Any

from distributed_file_hosting.lamport import LamportClock
from distributed_file_hosting.models import FileMetadata, FileVersion, compare_versions


class FileStorage:
    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._files_dir = data_dir / "files"
        self._metadata_path = data_dir / "metadata.json"
        self._metadata: dict[str, FileMetadata] = {}
        self._lock = threading.RLock()

    def load(self, clock: LamportClock) -> None:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._files_dir.mkdir(parents=True, exist_ok=True)
        if self._metadata_path.exists():
            payload = json.loads(self._metadata_path.read_text(encoding="utf-8"))
            self._metadata = {
                logical_path: FileMetadata.from_dict(item)
                for logical_path, item in payload.items()
            }
        highest_clock = max((item.version.lamport_counter for item in self._metadata.values()), default=0)
        clock.seed(highest_clock)

    def list_metadata(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._metadata[key].to_dict() for key in sorted(self._metadata)]

    def get_catalog(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {key: value.to_dict() for key, value in self._metadata.items()}

    def get_metadata(self, logical_path: str) -> FileMetadata | None:
        with self._lock:
            return self._metadata.get(logical_path)

    def read_content(self, logical_path: str) -> bytes | None:
        metadata = self.get_metadata(logical_path)
        if metadata is None:
            return None
        file_path = self._files_dir / metadata.stored_name
        if not file_path.exists():
            return None
        return file_path.read_bytes()

    def put_version(
        self,
        logical_path: str,
        content: bytes,
        content_type: str,
        version: FileVersion,
    ) -> tuple[dict[str, Any], bool]:
        with self._lock:
            current = self._metadata.get(logical_path)
            if current is not None and compare_versions(version, current.version) <= 0:
                return current.to_dict(), False

            stored_name = self._stored_name_for(logical_path)
            file_path = self._files_dir / stored_name
            file_path.write_bytes(content)

            metadata = FileMetadata(
                logical_path=logical_path,
                stored_name=stored_name,
                size=len(content),
                checksum=hashlib.sha256(content).hexdigest(),
                content_type=content_type or "application/octet-stream",
                version=version,
            )
            self._metadata[logical_path] = metadata
            self._persist_metadata()
            return metadata.to_dict(), True

    def _persist_metadata(self) -> None:
        payload = {key: value.to_dict() for key, value in self._metadata.items()}
        self._metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _stored_name_for(self, logical_path: str) -> str:
        suffix = PurePosixPath(logical_path).suffix
        digest = hashlib.sha256(logical_path.encode("utf-8")).hexdigest()
        return f"{digest}{suffix}"
