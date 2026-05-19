from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from distributed_file_hosting.models import PeerConfig


JsonHandler = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]


class JsonRpcServer:
    def __init__(self, host: str, port: int, handler: JsonHandler) -> None:
        self._host = host
        self._port = port
        self._handler = handler
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_connection, self._host, self._port)

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            request = json.loads(line.decode("utf-8"))
            result = await self._handler(request["method"], request.get("params", {}))
            response = {"ok": True, "result": result}
        except Exception as exc:
            response = {"ok": False, "error": str(exc)}
        writer.write((json.dumps(response) + "\n").encode("utf-8"))
        await writer.drain()
        writer.close()
        await writer.wait_closed()


class JsonRpcClient:
    @staticmethod
    async def request(
        peer: PeerConfig,
        method: str,
        params: dict[str, Any],
        timeout: float,
    ) -> dict[str, Any]:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(peer.host, peer.rpc_port),
            timeout=timeout,
        )
        try:
            request = {"method": method, "params": params}
            writer.write((json.dumps(request) + "\n").encode("utf-8"))
            await writer.drain()
            raw_response = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not raw_response:
                raise RuntimeError(f"peer {peer.node_id} closed the connection without a response")
            response = json.loads(raw_response.decode("utf-8"))
            if not response.get("ok"):
                raise RuntimeError(response.get("error", "rpc request failed"))
            return response["result"]
        finally:
            writer.close()
            await writer.wait_closed()
