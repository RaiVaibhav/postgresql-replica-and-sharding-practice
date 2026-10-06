# PostgreSQL Replication & Sharding Lab

A practical, hands-on reference implementation of PostgreSQL **Physical Streaming Replication** (with read/write splitting) and **Horizontal Database Sharding** (with key-based routing, scatter-gather queries, and distributed constraint handling).

---

## 1. Architecture & Port Mapping

| Component | Port | Role | Description |
| :--- | :--- | :--- | :--- |
| **`pg-primary`** | `5433` | Database | Primary writer node (WAL source). |
| **`pg-replica`** | `5434` | Database | Physical streaming replica (Hot Standby, read-only). |
| **`rw-api`** | `8005` | FastAPI | Routes writes to `5433`, reads to `5434`. |
| **`shard-am`** | `5435` | Database | Shard 1: stores keys `[a-m]` (Read/Write). |
| **`shard-nz`** | `5436` | Database | Shard 2: stores keys `[n-z]` (Read/Write). |
| **`sharding-api`** | `8006` | FastAPI | Routes requests to `5435` or `5436` based on key. |

---

## 2. Core Concepts: Replication vs. Sharding

| Feature | Replication (Ports 5433 & 5434) | Sharding (Ports 5435 & 5436) |
| :--- | :--- | :--- |
| **Data Distribution** | **Same data** on all nodes (exact clone). | **Data is split** across nodes (partitions). |
| **Writes** | Only 1 node writes (Primary). | **Every shard writes** for its own partition. |
| **Reads** | Scaled across all replicas. | Routed to the specific shard holding the key. |
| **Problem Solved** | Read scaling & High Availability. | **Write throughput** & Storage limits. |

---

## 3. Part 1: Physical Streaming Replication

### How It Works Under the Hood
PostgreSQL does not replicate by re-running SQL queries. It streams raw disk page deltas.
1. **WAL (Write-Ahead Log):** Every write is sequentially appended to binary files in `pg_wal/`. Each entry gets an **LSN (Log Sequence Number)**—a monotonic byte offset.
2. **`walsender` (Primary):** Pushes WAL bytes over TCP.
3. **`walreceiver` (Replica):** Receives the stream into the replica's local `pg_wal/`.
4. **`startup` (Replica):** Replays the WAL bytes onto the replica's local data files in read-only **Hot Standby** mode.

```
[ Primary :5433 ]                             [ Replica :5434 ]
Writes -> pg_wal/ -> walsender ===(TCP)===> walreceiver -> pg_wal/ -> startup -> Replay
```

### Setup Commands
```bash
# 1. Initialize & start Primary
initdb -D ./primary_data
pg_ctl -D ./primary_data -o "-p 5433" -l primary.log start

# 2. Configure replication user & permissions
psql -p 5433 -d postgres -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replica_secret';"
echo "host replication replicator 127.0.0.1/32 scram-sha-256" >> ./primary_data/pg_hba.conf
psql -p 5433 -d postgres -c "SELECT pg_reload_conf();"

# 3. Clone Primary using pg_basebackup with -R
# (-R creates standby.signal and configures primary_conninfo)
PGPASSWORD='replica_secret' pg_basebackup -h 127.0.0.1 -p 5433 -U replicator -D ./replica_data -Fp -Xs -R

# 4. Start Replica
pg_ctl -D ./replica_data -o "-p 5434" -l replica.log start
```

### Read/Write Split API (`main.py`)
Run the FastAPI service on port `8005`:
```bash
./.venv/bin/uvicorn main:app --port 8005 --reload
```

* **`POST /users`** $\rightarrow$ Uses `writer_pool` (Port 5433).
* **`GET /users`** $\rightarrow$ Uses `reader_pool` (Port 5434).
* **`POST /test-replica-write`** $\rightarrow$ Intentionally writes to the replica to catch Postgres's `ReadOnlySQLTransactionError`.

### Key Learning: Read-After-Write Consistency
If a user updates their profile (`POST /profile` $\rightarrow$ Primary) and is immediately redirected to view it (`GET /profile` $\rightarrow$ Replica), **replication lag** can cause them to see stale data.

**Solutions:**
1. **Session Sticky Window (GitHub/Shopify pattern):** After any write, pin that specific user to the Primary for 2–3 seconds. Everyone else reads from the replica.
2. **LSN Tokens:** Return `pg_current_wal_lsn()` in an HTTP header (`X-Database-LSN`). The next read verifies that the replica's replay LSN has caught up before querying it.
3. **Return-On-Write:** Have `POST` endpoints return `RETURNING *` in the response body so the client doesn't need an immediate `GET`.

---

## 4. Part 2: Horizontal Database Sharding

Located in [`sharding/`](./sharding/). Data is partitioned by the first letter of `username`.

```
                    +---------------------------+
                    |   Sharded API (Port 8006) |
                    +-------------+-------------+
                                  |
              +-------------------+-------------------+
              | keys [a-m]                            | keys [n-z]
              v                                       v
     +-----------------+                     +-----------------+
     | Shard 1 (:5435) |                     | Shard 2 (:5436) |
     | alice, bob, etc.|                     | peter, zoe, etc.|
     +-----------------+                     +-----------------+
```

### Setup Commands
```bash
# 1. Initialize two independent databases
initdb -D ./sharding/shard_am_data
initdb -D ./sharding/shard_nz_data

# 2. Start both shards
pg_ctl -D ./sharding/shard_am_data -o "-p 5435" -l ./sharding/shard_am.log start
pg_ctl -D ./sharding/shard_nz_data -o "-p 5436" -l ./sharding/shard_nz.log start

# 3. Create the users table on both
psql -p 5435 -d postgres -c "CREATE TABLE users (id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE, email VARCHAR(100));"
psql -p 5436 -d postgres -c "CREATE TABLE users (id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE, email VARCHAR(100));"
```

### Sharded Router API (`sharding/main.py`)
Run the router service on port `8006`:
```bash
./.venv/bin/uvicorn sharding.main:app --port 8006 --reload
```

* **`POST /users`** $\rightarrow$ Computes shard from key, inserts into 5435 or 5436.
* **`GET /users/{username}`** $\rightarrow$ Direct single-shard targeted lookup.
* **`GET /users` (Scatter-Gather)** $\rightarrow$ Queries both shards in parallel using `asyncio.gather()`, merges results, and aggregates counts.

---

## 5. Part 3: Constraints, Foreign Keys & IDs in Sharding

Database constraints only exist within a single PostgreSQL process. Once you shard across ports or hosts, constraints change dramatically:

### 1. Foreign Keys
* **Intra-Shard / Colocated Tables (FKs Work):**
  If `users` and `orders` share the same shard key (`user_id`), User 42 and all of User 42's orders live on the same physical shard. Standard SQL `FOREIGN KEY (user_id) REFERENCES users(id)` works normally.
* **Cross-Shard Tables (FKs Break):**
  If `orders` (sharded by `user_id` on Shard 1) references `products` (sharded by `product_id` on Shard 2), Postgres cannot enforce the constraint across sockets.
  * **Solution A:** Application-level validation before write.
  * **Solution B (Broadcast Tables):** Small, slowly changing tables (e.g. `categories`, `countries`) are duplicated on *every* shard so local joins/FKs work.

### 2. Global Uniqueness Constraints
A local `UNIQUE (email)` on Shard 2 cannot see records on Shard 1.
* Two users could register the same email on different shards without triggering a DB constraint violation!
* **Solution:** Maintain a global lookup index (e.g. Redis or a centralized table) mapping `email -> shard_id`, or shard by the unique credential itself.

### 3. Primary Key Collisions (`SERIAL` breaks)
Standard auto-incrementing `SERIAL` starts at `1` on both Shard 1 and Shard 2, creating duplicate IDs immediately.
* **Solution:** Use **UUIDs** (`uuid_generate_v4()`) or **Snowflake IDs** (64-bit integers encoding `Timestamp + Shard ID + Sequence`).

---

## 6. Testing Guide

### Test Part 1: Replication & Read/Write Split (Port 8005)
```bash
# 1. Write user -> Primary (5433)
curl -X POST http://localhost:8005/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice"}'

# 2. Read users -> Replica (5434)
curl http://localhost:8005/users

# 3. Test replica write lock -> returns 400 Bad Request
curl -X POST http://localhost:8005/test-replica-write \
  -H "Content-Type: application/json" \
  -d '{"name": "Attacker"}'
```

### Test Part 2: Sharding & Routing (Port 8006)
```bash
# 1. Write 'alice' -> Routes to Shard 1 (5435)
curl -X POST http://localhost:8006/users \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "email": "alice@example.com"}'

# 2. Write 'peter' -> Routes to Shard 2 (5436)
curl -X POST http://localhost:8006/users \
  -H "Content-Type: application/json" \
  -d '{"username": "peter", "email": "peter@example.com"}'

# 3. Targeted Single-Shard Lookups
curl http://localhost:8006/users/alice
curl http://localhost:8006/users/peter

# 4. Scatter-Gather Query (Cross-shard parallel fetch)
curl http://localhost:8006/users

# 5. Verify physical data isolation on disk
psql -p 5435 -d postgres -c "SELECT username FROM users;"  # Only 'alice'
psql -p 5436 -d postgres -c "SELECT username FROM users;"  # Only 'peter'
```

---

## 7. Production Reality Checklist

1. **Replication Slots (`pg_create_physical_replication_slot`):** Ensures the primary never deletes WAL files that a temporarily disconnected replica still needs.
2. **Automatic Failover (Patroni):** Postgres does not elect leaders on its own. Use **Patroni** + **etcd** for distributed Raft leader election.
3. **Connection Pooling (PgBouncer / PgCat):** Each Postgres connection is an OS process. A pooler prevents connection exhaustion under microservice traffic.

---

## 8. Stopping All Lab Processes

```bash
pg_ctl -D ./primary_data stop
pg_ctl -D ./replica_data stop
pg_ctl -D ./sharding/shard_am_data stop
pg_ctl -D ./sharding/shard_nz_data stop
```
