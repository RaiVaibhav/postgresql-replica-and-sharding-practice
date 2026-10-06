# Setting Up PostgreSQL Streaming Replication from Scratch (and Why Your App Might Still Show Stale Data)

For a long time, database replication was an infrastructure black box to me: check a box in AWS or GCP, get a writer and reader endpoint, and hope replication lag stays low.

To understand what actually happens under the hood, I set up PostgreSQL streaming replication manually on my local machine—no Docker, no cloud consoles, just two Postgres clusters on different ports.

Here is how Postgres moves bytes between nodes, the exact commands to wire it up, and the architectural headaches you hit when splitting reads and writes.

---

## 1. What's Actually Happening Under the Hood?

When a replica syncs with a primary, it does **not** re-execute SQL statements. It streams raw disk-block deltas.

### The Write-Ahead Log (WAL)
PostgreSQL writes changes to the **WAL** before touching table files on disk. Every change is sequentially appended to binary files in `pg_wal/` and tagged with a **Log Sequence Number (LSN)**—a monotonic 64-bit byte offset. Once the WAL flushes to disk, the transaction is committed. A background process (`checkpointer`) flushes dirty pages to table files later.

```
[ App Write ] ---> [ Postgres Engine ] ---> Appends to [ pg_wal/ ] (Source of Truth)
                                                   |
                                                   v
                                 (Flushed later to disk table pages)
```

### The Streaming Daemon Trio
Three background processes handle replication:

1. **`walsender` (Primary):** Tails `pg_wal/` and pushes raw bytes over TCP.
2. **`walreceiver` (Replica):** Receives WAL chunks and writes them to the replica's local `pg_wal/`.
3. **`startup` (Replica):** Replays those WAL records directly onto the replica's local data files.

Because it replays exact page edits, the replica is a physical binary clone of the primary running in read-only **Hot Standby** mode.

---

## 2. Hands-On: Setting Up Local Replication

We will run two Postgres instances on the same machine:
* **Primary:** port `5433`
* **Replica:** port `5434`

### Step 1: Initialize and Start the Primary

```bash
# Initialize primary cluster
initdb -D ./primary_data

# Start primary on port 5433
pg_ctl -D ./primary_data -o "-p 5433" -l primary.log start
```

Verify it's running:
```bash
psql -p 5433 -d postgres -c "SELECT version();"
```

---

### Step 2: Configure Replication Credentials on the Primary

Standard users cannot access raw WAL streams. We need a role with the `REPLICATION` privilege and firewall clearance in `pg_hba.conf`.

#### 1. Create the replication role
```bash
psql -p 5433 -d postgres -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replica_secret';"
```
* `WITH REPLICATION`: Authorizes the user to initiate WAL streaming connections.
* `LOGIN`: Allows the role to authenticate and open a session.

#### 2. Allow replication in `pg_hba.conf`
Postgres has an internal host-based authentication firewall (`pg_hba.conf`). We must permit `replicator` to connect over TCP:

```bash
echo "host replication replicator 127.0.0.1/32 scram-sha-256" >> ./primary_data/pg_hba.conf
```
*Note: `replication` in the second column is a reserved keyword for the streaming protocol, not a database name.*

#### 3. Reload configuration
Reload changes without restarting Postgres:
```bash
psql -p 5433 -d postgres -c "SELECT pg_reload_conf();"
```

---

### Step 3: Clone the Primary using `pg_basebackup -R`

A replica must begin as an exact physical binary clone of the primary's data directory.

```bash
PGPASSWORD='replica_secret' pg_basebackup -h 127.0.0.1 -p 5433 -U replicator -D ./replica_data -Fp -Xs -R
```

The **`-R`** flag automates the standby setup:
1. It creates an empty file called `replica_data/standby.signal`. When Postgres boots, this file tells it: *"Do not accept writes; boot in read-only recovery mode."*
2. It appends the primary's connection info to `replica_data/postgresql.auto.conf`:
   ```text
   primary_conninfo = 'host=127.0.0.1 port=5433 user=replicator password=replica_secret ...'
   ```

---

### Step 4: Start the Replica

Launch the replica on port `5434`:

```bash
pg_ctl -D ./replica_data -o "-p 5434" -l replica.log start
```

---

### Step 5: Verification & Testing

#### 1. Confirm recovery mode on Replica (5434)
```bash
psql -p 5434 -d postgres -c "SELECT pg_is_in_recovery();"
```
Returns: `t` (true).

#### 2. Confirm streaming status on Primary (5433)
```bash
psql -p 5433 -d postgres -c "SELECT usename, state, sync_state FROM pg_stat_replication;"
```
Output:
```text
  usename   |   state   | sync_state 
------------+-----------+------------
 replicator | streaming | async
```

#### 3. Test real data replication
Create a table and insert rows on the **Primary (5433)**:
```bash
psql -p 5433 -d postgres -c "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));"
psql -p 5433 -d postgres -c "INSERT INTO users (name) VALUES ('Alice'), ('Bob');"
```

Query the **Replica (5434)**:
```bash
psql -p 5434 -d postgres -c "SELECT * FROM users;"
```
The rows appear immediately.

#### 4. Test the read-only lock
Try writing directly to the **Replica**:
```bash
psql -p 5434 -d postgres -c "INSERT INTO users (name) VALUES ('Mallory');"
```
Postgres blocks it:
```text
ERROR: cannot execute INSERT in a read-only transaction
```

#### 5. Compare LSN positions
Compare the current write position on the primary with the replayed position on the replica:
```bash
# Primary write position:
psql -p 5433 -d postgres -c "SELECT pg_current_wal_lsn();"

# Replica replayed position:
psql -p 5434 -d postgres -c "SELECT pg_last_wal_replay_lsn();"
```
When caught up, these two hex values match.

---

## 3. The Application Problem: Read/Write Splitting

With the database nodes running, the backend must route traffic appropriately: writes to the primary, and reads across replicas.

### Approach A: Application-Level Routing
The backend maintains two connection pools: `writerPool` (:5433) and `readerPool` (:5434).

```javascript
// Writes go to Primary
app.post('/users', async (req, res) => {
  const result = await writerPool.query('INSERT INTO users(name) VALUES($1) RETURNING *', [req.body.name]);
  res.json(result.rows[0]);
});

// Reads go to Replica
app.get('/users', async (req, res) => {
  const result = await readerPool.query('SELECT * FROM users');
  res.json(result.rows);
});
```

* **Pros:** Full application context. The backend knows the user, session state, and recent actions. Zero middleman latency.
* **Cons:** App code manages multiple pools. Multiple app containers duplicate connections to both databases.

---

### Approach B: Proxy-Level Routing (e.g. PgCat, ProxySQL)
The app connects to a single proxy address (`postgres://proxy:6432/appdb`). The proxy inspects incoming SQL text:
* Statements starting with `SELECT` route to the replica.
* Statements with `INSERT`, `UPDATE`, `DELETE`, or wrapped in `BEGIN ... COMMIT` route to the primary.

* **Pros:** Single connection string in code. The proxy acts as a centralized connection pooler.
* **Cons:** Adds a network hop (`App -> Proxy -> DB`). Complex queries (like CTEs containing writes) risk misrouting.

---

## 4. The Read-After-Write Consistency Problem

This race condition bites almost every team implementing read replicas:

1. User updates their profile name from "Alice" to "Alison" (`POST /profile` $\rightarrow$ Primary).
2. Browser redirects to `GET /profile` ($\rightarrow$ Replica).
3. The replica is 15ms behind.
4. The user sees "Alice" again and assumes the save failed.

### How real-world systems fix it:

#### 1. The Session Sticky Window (What GitHub and Shopify do)
When a user writes data, the backend records `last_write_at = now()` in their session cookie or Redis.

For the next **2 to 3 seconds**, all `GET` requests from **that specific user** route to the **Primary**. Other users continue reading from the replicas. Once the window expires, the user quietly switches back to the replicas.

#### 2. LSN Token Tracking
1. The primary captures the LSN after a write:
   ```sql
   INSERT INTO ... RETURNING id;
   SELECT pg_current_wal_lsn(); -- e.g. "0/16E4F18"
   ```
2. The server passes this LSN back in a response header (`X-Database-LSN: 0/16E4F18`).
3. On the next read, the client sends the header back. The backend checks `pg_last_wal_replay_lsn()` on the replica:
   - If `replica_lsn >= requested_lsn`: Safe to read from replica.
   - If `replica_lsn < requested_lsn`: The replica is lagging; fall back to the primary for that query.

#### 3. Return-on-Write API Design
Modern SPAs avoid the immediate `GET` altogether. The `POST` endpoint runs `RETURNING *` and returns the saved object. The frontend updates local state immediately without a follow-up request.

---

## 5. What's Missing Before Production?

Running two local clusters exposes the core mechanics, but production needs three additional safeguards:

1. **Replication Slots (`pg_create_physical_replication_slot`):** If a replica goes offline, the primary might rotate and delete old WAL files. A replication slot forces the primary to retain WAL files until the replica confirms receipt.
2. **Automatic Failover (Patroni):** PostgreSQL has no built-in leader election. Production setups use **Patroni** with **etcd** or Consul to manage automated health checks and failovers via Raft consensus.
3. **Connection Pooling (PgBouncer):** Each connection in Postgres is a separate OS process. A pooler prevents connection exhaustion under heavy load.

---

## Wrapping Up

Stripped of cloud abstractions, PostgreSQL replication is a clean pipeline: sequential binary logging (WAL), a streaming TCP socket (`walsender`/`walreceiver`), and recovery replay.

The real challenge is at the application layer: routing queries cleanly and preventing users from seeing stale data during replication lag.
