from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse, Response

from distributed_file_hosting.cluster import ClusterManager, QuorumNotReachedError
from distributed_file_hosting.config import Settings

settings = Settings.from_env()
cluster = ClusterManager(settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await cluster.start()
    try:
        yield
    finally:
        await cluster.stop()


app = FastAPI(
    title="Distributed File Hosting Node",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "distributed-file-hosting", "node_id": settings.node_id}


@app.get("/health")
async def health() -> dict[str, object]:
    return cluster.health_snapshot()


@app.get("/files")
async def list_files() -> list[dict[str, object]]:
    return cluster.list_files()


@app.post("/files/upload")
async def upload_file(
    file: UploadFile = File(...),
    logical_path: str | None = Form(default=None),
) -> JSONResponse:
    content = await file.read()
    target_path = logical_path or file.filename
    if not target_path:
        raise HTTPException(status_code=400, detail="logical path is required")
    try:
        result = await cluster.upload_file(target_path, content, file.content_type or "application/octet-stream")
    except QuorumNotReachedError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return JSONResponse(status_code=201, content=result)


@app.get("/files/{logical_path:path}/metadata")
async def get_metadata(logical_path: str) -> dict[str, object]:
    metadata = cluster.storage.get_metadata(logical_path)
    if metadata is None:
        raise HTTPException(status_code=404, detail="file not found")
    return metadata.to_dict()


@app.get("/files/{logical_path:path}/download")
async def download_file(logical_path: str) -> Response:
    try:
        metadata, content = await cluster.fetch_latest_file(logical_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="file not found") from exc

    headers = {"Content-Disposition": f'attachment; filename="{logical_path.split("/")[-1]}"'}
    return Response(content=content, media_type=str(metadata["content_type"]), headers=headers)
