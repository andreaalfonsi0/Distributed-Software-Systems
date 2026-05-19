# Distributed File Hosting System

Lightweight distributed file hosting built with FastAPI and asyncio. Each node exposes a REST API for uploads and downloads, and a TCP JSON-RPC service for replication, metadata synchronization, health checks, and recovery.

## Features

- REST upload and download endpoints
- Automatic multi-node replication with configurable replication factor
- Write quorum enforcement
- Lamport logical clocks for versioning and conflict resolution
- Periodic peer health monitoring
- Metadata synchronization and recovery after node restarts
- Dockerized three-node demo topology
- Demo script that simulates upload, failure, and recovery

## Project Layout

```text
.
├── Dockerfile
├── docker-compose.yml
├── docs/
│   └── architecture.md
├── requirements.txt
├── scripts/
│   └── simulate_cluster.py
└── src/
    └── distributed_file_hosting/
        ├── cluster.py
        ├── config.py
        ├── lamport.py
        ├── main.py
        ├── models.py
        ├── rpc.py
        └── storage.py
```

## API

### Upload a file

```bash
curl -X POST http://127.0.0.1:8001/files/upload \
  -F "file=@./sample.txt" \
  -F "logical_path=docs/sample.txt"
```

### List local metadata

```bash
curl http://127.0.0.1:8001/files
```

### Download the latest version

```bash
curl http://127.0.0.1:8002/files/docs/sample.txt/download -o sample.txt
```

### Inspect health and cluster state

```bash
curl http://127.0.0.1:8001/health
```

## Run with Docker Compose

```bash
docker compose up -d --build
```

Nodes are exposed on:

- node1 REST: http://127.0.0.1:8001
- node2 REST: http://127.0.0.1:8002
- node3 REST: http://127.0.0.1:8003

Each container also runs an internal RPC server on port 9000 for node-to-node coordination.

## Run the Demo Simulation

The demo script will:

1. Start the cluster.
2. Upload a file to node1.
3. Read it from node2.
4. Stop node3.
5. Upload a newer version while node3 is offline.
6. Restart node3.
7. Verify that background recovery brings node3 up to date.

```bash
python3 scripts/simulate_cluster.py
```

## Local Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src uvicorn distributed_file_hosting.main:app --reload --port 8001
```

To start multiple nodes outside containers, set distinct values for `NODE_ID`, `REST_PORT`, `RPC_PORT`, `DATA_DIR`, and `PEERS` for each process.

## Configuration

Environment variables:

- `NODE_ID`: current node identifier
- `REST_HOST`, `REST_PORT`: REST listener settings
- `RPC_HOST`, `RPC_PORT`: TCP RPC listener settings
- `DATA_DIR`: local data directory for file blobs and metadata
- `PEERS`: JSON array of peer definitions with `node_id`, `host`, `rest_port`, `rpc_port`
- `REPLICATION_FACTOR`: number of copies to maintain including the local node
- `WRITE_QUORUM`: acknowledgements required for successful writes
- `READ_QUORUM`: recorded for topology and health reporting
- `HEALTH_CHECK_INTERVAL`: seconds between peer checks
- `RPC_TIMEOUT`: timeout for RPC calls in seconds

## Distributed Systems Notes

- Consistency: writes are versioned with Lamport clocks and conflicting versions are resolved by `(lamport_counter, version_node)` ordering.
- Availability: writes remain possible as long as the node can satisfy the configured write quorum.
- Partition tolerance: the system keeps serving healthy nodes and repairs replicas once connectivity returns.
- Recovery: nodes exchange catalogs after a peer becomes reachable and transfer missing or outdated objects.

See `docs/architecture.md` for diagrams and flow descriptions.