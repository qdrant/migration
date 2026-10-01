# Qdrant Cluster-to-Cluster & Disaster Recovery (DR) Data Synchronization

A production-ready guide and automation script for synchronizing data between distributed multi-node Qdrant clusters (e.g. a 3-node primary cluster and a 3-node Disaster Recovery site).

Addresses **Issue #347 ("Local deployed cluster data sync")**.

---

## The Core Concept: Do You Need to Migrate Every Node?

**No, you do not need to migrate each node individually.**

You only need to specify:
- **One healthy node (or load balancer)** from the source cluster.
- **One healthy node (or load balancer)** from the target DR cluster.

### Why?

Qdrant is a distributed vector database coordinated by an internal Raft consensus layer and peer-to-peer (P2P) communication:

1. **Automatic Read Proxying (Source Cluster)**:
   When you connect to any healthy node in the source cluster and issue a read or scroll request, that node acts as a **coordinator**. If data for a given shard resides on another peer node in the cluster, the coordinator forwards the request internally, gathers the shard records, and returns the combined result stream to your client.

2. **Automatic Sharding & Replication (Target DR Cluster)**:
   When you stream batches into any healthy node in the target DR cluster, that node acts as the write coordinator. It hashes the point IDs (or shard keys) to determine which cluster nodes hold the primary shard and replicas, routes the batches across the internal network, and ensures your collection's configured replication factor and write consistency are maintained.

```
       Primary Cluster (3 Nodes)                          DR Cluster (3 Nodes)
   ┌────────────────────────────────┐             ┌────────────────────────────────┐
   │  [Node 1] ◄─── (Coordinator)   │             │  [Node 1] ◄─── (Coordinator)   │
   │     ▲                          │             │     │                          │
   │  Raft/P2P                      │             │  Raft/P2P                      │
   │     ▼                          │             │     ▼                          │
   │  [Node 2]       [Node 3]       │             │  [Node 2]       [Node 3]       │
   └─────┬──────────────────────────┘             └───────────────────────▲────────┘
         │                                                                │
         │           sync_cluster.py / qdrant-migration                   │
         └─────────────────── Streaming Batches ──────────────────────────┘
```

Attempting to migrate node-by-node manually is neither necessary nor recommended, as it bypasses the consensus layer and risks duplicate writes.

---

## Solution 1: Automated Multi-Collection Python Script (`sync_cluster.py`)

We provide [`sync_cluster.py`](../../sync_cluster.py) at the repository root for automated cluster replication.

### Features
- Pre-flight connection verification and health checks for both clusters.
- Auto-discovers all source collections or selectively migrates requested ones.
- Extracts source collection topology (vector dimensionality, distance metric, shard number, replication factor, sparse vectors, and quantization) and automatically provisions missing collections on the target DR cluster.
- Incremental batched streaming using `client.scroll()` and `client.upsert()`.
- Built-in batch retry with exponential backoff for network resilience.
- Isolated collection-level error guarding (if one collection fails, the rest proceed smoothly).
- Comprehensive end-of-run execution and throughput report.

### Running `sync_cluster.py`

```bash
# Install dependencies
pip install -r requirements.txt

# Migrate all collections from primary cluster to DR cluster
python sync_cluster.py \
  --source-url http://source-cluster-node1:6333 \
  --target-url http://dr-cluster-node1:6333 \
  --batch-size 256

# Migrate specific collections with custom timeout and API keys
python sync_cluster.py \
  --source-url http://10.0.0.10:6333 \
  --target-url http://10.0.1.10:6333 \
  --source-api-key "source-secret" \
  --target-api-key "dr-secret" \
  --collections "embeddings_v1,products_v2" \
  --batch-size 512 \
  --timeout 120
```

---

## Solution 2: Official `qdrant-migration` Docker CLI

You can also use the standard containerized migration tool to move collections between clusters:

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration qdrant \
    --source.url 'http://source-cluster-node1:6334' \
    --source.collection 'my_collection' \
    --target.url 'http://dr-cluster-node1:6334' \
    --target.collection 'my_collection' \
    --migration.batch-size 256
```

> **Note**: Port `6334` is the default gRPC port used by the Docker migration CLI. Port `6333` is the HTTP REST API port used by standard HTTP clients.
