# Distributed Key-Value Store - Design Document

## 1. System Overview

This is a **Leaderless (Peer-to-Peer)** distributed key-value store. It prioritizes high availability and partition tolerance while ensuring data durability through replication.

The system uses a **Hybrid Architecture**:

* **External API:** REST/HTTP (for Clients)

* **Internal Communication:** gRPC (for Node-to-Node Replication)

## 2. Core Features

### A. Data Partitioning (Consistent Hashing)

* **Algorithm:** Consistent Hashing.

* **Implementation:** `TreeMap<Long, NodeInfo>` (Java) to simulate the Ring.

* **Hashing:** MD5 of the Node ID (IP:Port).

* **Topology:**

    * Data is placed on a theoretical Ring (0 to 2^64).

    * A Key is assigned to the first node found moving **clockwise** on the ring.

    * **Simplification:** No Virtual Nodes (1 Physical Node = 1 Point on the Ring).

### B. Replication Strategy

* **Replication Factor (N):** 3.

* **Placement:** The "Coordinator" (Owner) replicates the data to the next 2 unique successor nodes on the ring.

* **Durability:** Data exists on 3 distinct physical machines.

### C. Consistency Model (Quorum Consensus)

The system uses **Strict Quorum** to ensure consistency.

* **Write Quorum (W):** 2 (At least 2 nodes must acknowledge a write).

* **Read Quorum (R):** Implicitly handled via **Read Repair**.

* **Conflict Resolution:** **Last-Write-Wins (LWW)**.

    * Every write carries a `timestamp` (client-side or coordinator-assigned).

    * If two nodes have different values for the same key, the one with the higher timestamp overwrites the other.

## 3. Workflows

### Write Path (`PUT /key`)

1. Client sends HTTP request to **Any Node** (Coordinator).

2. Coordinator calculates the **Preference List** (Owner + 2 Replicas).

3. Coordinator sends **Parallel gRPC Requests** to all 3 nodes.

4. Each node writes to its local `ConcurrentHashMap` (Atomic `put` logic using Timestamp check).

5. Coordinator waits for responses.

6. **Success Condition:** If **Acks >= 2**, return `200 OK`.

7. **Failure Condition:** If Acks < 2, return `503 Service Unavailable` (Write rejected to preserve consistency).

### Read Path (`GET /key`)

1. Client sends HTTP request to **Any Node**.

2. Coordinator performs a **Scatter-Gather**:

    * Checks its local storage.

    * Sends gRPC `InternalGet` to the other replicas.

3. Coordinator collects results.

4. **Read Repair:** If replicas disagree, the value with the **highest timestamp** is selected.

5. Return the latest value to the Client.

### Delete Path (`DELETE /key`)

* **Mechanism:** **Tombstones**.

* We do not strictly delete the key from memory immediately.

* We write a special entry: `{ value: null, is_tombstone: true, timestamp: NOW }`.

* This ensures the "Delete" event propagates to replicas that might be temporarily down.

### Batch Path (`POST /batch`)

1. Coordinator receives a list of keys.

2. **Grouping:** Keys are grouped by their **Primary Owner**.

3. **Parallel Execution:** Sub-batches are sent to the respective Owners via `InternalBatchPut`.

4. **Partial Failure:** The API returns `207 Multi-Status` if some keys succeeded and others failed (e.g., if one shard is down).

### Scan Path (`GET /range`)

1. Since partitioning is random, a range query (e.g., "UserA" to "UserZ") requires a **Full Cluster Scan**.

2. Coordinator sends `InternalScan` to **ALL** unique nodes in the ring.

3. Results are merged, deduplicated (via LWW), and sorted lexicographically before returning.

## 4. Failure Handling

### Detection (Passive)

* **No Gossip Protocol:** To keep the system simple and robust for small clusters (3-5 nodes), we use **Reactive Detection**.

* **Mechanism:** If a gRPC call throws `StatusRuntimeException: UNAVAILABLE`, the node is considered "Down" for the duration of that request.

### Recovery (Anti-Entropy)

* **Trigger:** When a node starts (or restarts).

* **Discovery:** Node connects to a **Seed Node** to get the full peer list.

* **Sync:**

    1. The node iterates through all peers.

    2. Sends `TransferRange` (Fetch Everything) request.

    3. **Client-Side Filtering:** The node downloads the stream but only saves keys that **it is responsible for** (Primary or Replica) based on the current Ring topology.

## 5. Technical Stack

| **Component** | **Technology**            | **Usage** |
| --- |---------------------------| --- |
| **Language** | Java 25                   | Core Logic |
| **Framework** | Spring Boot 3.5.8         | Application Skeleton |
| **HTTP Server** | Spring WebFlux (Netty)    | Public API (Port 8080) |
| **RPC Framework** | Spring gRPC (Netty)       | Internal Communication (Port 9090) |
| **Serialization** | Protocol Buffers (Proto3) | High-performance binary transfer |
| **Storage** | `ConcurrentHashMap`       | In-Memory Data Store |
| **Locks** | `ReentrantReadWriteLock`  | Thread-safe Ring updates |
