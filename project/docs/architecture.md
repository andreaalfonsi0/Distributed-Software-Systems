# Distributed File Hosting Architecture

## Component View

```mermaid
flowchart LR
    Client[REST Client] --> API1[FastAPI Node 1]
    Client --> API2[FastAPI Node 2]
    Client --> API3[FastAPI Node 3]

    API1 <-->|JSON RPC over TCP| API2
    API2 <-->|JSON RPC over TCP| API3
    API1 <-->|JSON RPC over TCP| API3

    API1 --> S1[(Local file store)]
    API2 --> S2[(Local file store)]
    API3 --> S3[(Local file store)]
```

## Upload Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant N1 as Ingress Node
    participant N2 as Replica Node
    participant N3 as Replica Node

    C->>N1: POST /files/upload
    N1->>N1: Tick Lamport clock
    N1->>N1: Persist file + metadata
    par Replication
        N1->>N2: replicate_file RPC
        N1->>N3: replicate_file RPC
    end
    N2->>N2: Update Lamport clock
    N3->>N3: Update Lamport clock
    N2-->>N1: ack
    N3-->>N1: ack
    N1-->>C: 201 Created
```

## Recovery Flow

```mermaid
sequenceDiagram
    participant N1 as Healthy Node
    participant N3 as Recovered Node

    loop health check
        N1->>N3: ping RPC
    end
    N3-->>N1: reachable again
    N1->>N3: get_catalog RPC
    N1->>N3: replicate newer local objects
    N1->>N1: fetch newer remote objects when needed
```

## CAP Trade-off

This implementation is biased toward partition tolerance and operational availability. During a partition, writes can still succeed when the configured write quorum is met, but a full-cluster consistent view is not guaranteed until synchronization completes. Logical clocks provide deterministic conflict resolution, but they do not preserve full causal history the way vector clocks would.
