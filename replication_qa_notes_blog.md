# The Questions You Only Ask When You Actually Build Postgres Replication Yourself

Most tutorials on PostgreSQL replication start with a pre-baked Docker Compose file and a paragraph about high availability. Everything works on the first try, but you walk away not really knowing what just happened.

When I decided to set up streaming replication by hand on my local machine, I found myself pausing at almost every step with questions that docs gloss over:

* *Why do I need a special user attribute just to copy data?*
* *Why does my firewall rule name a database that doesn't exist?*
* *Do I even need Docker for this?*
* *And once it works, why did my test user just see stale data after updating their profile?*

Here is a breakdown of the exact questions that came up while building a live replication cluster from scratch, where those doubts originated, and what they taught me about how Postgres really works.

---

## 1. "Can we do this with PostgreSQL instead of MySQL?"

### Where the doubt came from:
A lot of classic replication tutorials and system design cheat-sheets are written around MySQL master-slave setups with the binary log (`binlog`). When shifting to PostgreSQL, it’s natural to wonder if the setup is fundamentally different or requires completely different tooling.

### How it relates to replication:
The core concept is the same, but the underlying engine is different:
* **MySQL** traditionally replicates logical row events or statements via the **binlog**.
* **PostgreSQL** uses physical streaming replication driven by the **Write-Ahead Log (WAL)**. 

In Postgres, replication doesn't replay SQL statements. It works at the disk-block level. When you insert a row on the primary, Postgres records the raw byte deltas to the WAL. The replica streams those exact bytes and applies them to its own disk pages. 

**The takeaway:** Postgres physical replication produces an identical, bit-for-bit clone of the primary that boots in read-only "Hot Standby" mode.

---

## 2. "Do we actually need Docker Compose to test this locally?"

### Where the doubt came from:
Almost every online guide immediately tells you to create a `docker-compose.yml` with container networks and volumes. But if you already have PostgreSQL installed on your machine, Docker just adds layers of abstraction—virtual networks, mapped ports, and hidden volumes—that obscure what files are actually being created.

### How it relates to replication:
Postgres doesn't care about containers. It only cares about **data directories** and **port numbers**.

You can run two completely independent database instances natively on your machine using built-in Postgres tools:
```bash
# Instance 1 (Primary) on Port 5433:
initdb -D ./primary_data
pg_ctl -D ./primary_data -o "-p 5433" start

# Instance 2 (Replica) on Port 5434:
pg_basebackup -h 127.0.0.1 -p 5433 -U replicator -D ./replica_data -Fp -Xs -R
pg_ctl -D ./replica_data -o "-p 5434" start
```

**The takeaway:** Doing it locally without Docker made replication tangible. I could open `./replica_data`, inspect `standby.signal`, check `postgresql.auto.conf`, and see the raw WAL segments inside `pg_wal/`.

---

## 3. "Why `CREATE ROLE replicator WITH REPLICATION LOGIN...`? Why not just use the default admin user?"

### Where the doubt came from:
When testing locally, it's tempting to use `postgres` or whatever default superuser you have. Why bother creating a dedicated role, and what does `WITH REPLICATION` actually do that standard privileges don't?

### How it relates to replication:
PostgreSQL strictly isolates standard SQL execution from the streaming replication protocol. 
* A normal user with `SELECT`, `INSERT`, or even superuser rights can execute SQL queries inside a database.
* But physical streaming replication does **not** connect to a database schema; it connects to the raw WAL streaming engine.

The `REPLICATION` flag is an internal capability bit. Without it, Postgres rejects any attempt by a client to invoke streaming replication commands (like `START_REPLICATION`). 

**The takeaway:** It follows the principle of least privilege. In production, your streaming user shouldn't have arbitrary access to browse or modify application tables; it only needs permission to read the sequential WAL stream.

---

## 4. "What does `host replication replicator 127.0.0.1/32` mean in `pg_hba.conf`? There is no database named 'replication'!"

### Where the doubt came from:
When editing `pg_hba.conf` (Postgres's internal firewall), the columns usually look like this:
```text
# TYPE   DATABASE   USER        ADDRESS          METHOD
host     appdb      app_user    127.0.0.1/32     scram-sha-256
```
So when adding a replication rule, it looks like you're granting access to a database called `replication`:
```text
host     replication replicator  127.0.0.1/32     scram-sha-256
```
If you search for a database called `replication` in `psql -l`, it doesn't exist.

### How it relates to replication:
In `pg_hba.conf`, the word `replication` is a **reserved system keyword**, not a database name.

It tells PostgreSQL: *"When a connection request arrives for the physical streaming protocol (which bypasses standard SQL databases), apply this rule."* 

**The takeaway:** If you try to put your actual database name (like `appdb`) in that line, streaming will immediately fail with an authentication error because the replication handshake never asks to open `appdb`.

---

## 5. "What's actually happening in memory while replication runs, and how does production handle it?"

### Where the doubt came from:
Running `pg_ctl start` brings up the replica, and queries work. But what processes are keeping them in sync? And what happens when the primary crashes at 2 AM?

### How it relates to replication:
Behind the scenes, three specific processes keep the cluster alive:
1. **`walsender` (Primary):** Tails `pg_wal/` and pushes raw bytes over TCP.
2. **`walreceiver` (Replica):** Listens on the socket and writes incoming bytes to the replica's local `pg_wal/`.
3. **`startup` (Replica):** The recovery worker that reads the replica's WAL and applies disk page modifications directly.

In a toy project, this is all you need. But in production, you quickly run into two major realities:
* **The WAL Purge Trap:** If the replica goes down for 30 minutes, the primary might rotate and delete old WAL files to free up disk. When the replica comes back, it's permanently broken. Production uses **Replication Slots** (`pg_create_physical_replication_slot`) to force the primary to keep WAL files until the replica confirms receipt.
* **Failover:** Postgres has **no built-in leader election**. If the primary dies, nobody promotes the replica automatically. Real companies use tools like **Patroni** (with `etcd` or Consul) to handle automated health checks and failover via Raft consensus.

---

## 6. "Should we do Read/Write splitting in the Application or through a Proxy?"

### Where the doubt came from:
Once you have a read replica on port 5434 and a primary on port 5433, how should the application talk to them? Should the backend manage two connection pools, or should we drop a proxy like **PgCat** or **ProxySQL** in between?

### How it relates to replication:
This is one of the biggest architectural choices when scaling a database layer:

| | Application-Level Routing | Proxy-Level Routing (e.g. PgCat) |
| :--- | :--- | :--- |
| **How it routes** | The backend initializes two pools: `writerPool` (:5433) and `readerPool` (:5434). Code explicitly calls the right pool. | The app talks to one URL. The proxy parses SQL strings: `SELECT` goes to replica, `INSERT`/`UPDATE` to primary. |
| **Context** | **High.** The app knows who the user is, what page they are on, and if they just performed a write. | **Low.** The proxy only sees raw SQL text over a TCP socket. |
| **Latency** | Direct connection (zero extra hops). | Extra network hop (`App -> Proxy -> DB`). |
| **Connection Count**| Multiplied across instances (can exhaust Postgres connection limits). | **Excellent.** Multiplexes hundreds of app connections into a tight DB pool. |

**The takeaway:** Standard web apps and microservices (Rails, Django, Node.js) usually start with **Application-Level Routing** because ORMs already support multiple pools. When fleets of containers start overwhelming Postgres's process limits, teams introduce proxies like **PgBouncer** or **PgCat**.

---

## 7. "How do you solve Read-After-Write Consistency in both approaches?"

### Where the doubt came from:
This is the classic bug:
1. User updates their profile name to "Robert" (`POST /profile` $\rightarrow$ Primary).
2. The page redirects to `GET /profile` ($\rightarrow$ Replica).
3. The replica is 20 milliseconds behind.
4. The user sees "Bob" again and thinks the save button didn't work.

How do you prevent this when reads and writes are split?

### How it relates to replication:
Replication lag is unavoidable in asynchronous setups. Here is how both worlds solve it:

#### In Application Routing: The Session Sticky Window
Companies like GitHub and Shopify solve this at the session level:
* When a user performs a write (`POST`/`PUT`/`DELETE`), the backend sets a temporary timestamp: `last_write_at = now()` in Redis or their cookie.
* For the next **2 to 3 seconds**, any `GET` query from **that specific user** is forced to the **Primary**.
* Everyone else continues reading from the replicas. Once the 3-second window expires, the user quietly switches back to the replicas too.

#### In Proxy Routing: Hints & Connection Pinning
Because a proxy can't read user session cookies:
* **Query Hints:** The app passes a SQL comment: `/* pgcat: primary */ SELECT * FROM profile;`. The proxy parses this and forces the read to the primary.
* **Dirty Connection Pinning:** When an `INSERT` passes through a connection, the proxy marks that connection as "dirty" and routes any subsequent `SELECT` on that same connection to the primary for the next 2 seconds.

---

## Summary

Setting up replication manually turns abstract database jargon into concrete mental models:
* The **WAL** isn't a theory; it's a folder of binary log files on disk.
* **Replication** isn't magical synchronization; it's two background processes passing byte deltas over a socket.
* And **Read/Write splitting** isn't just about pointing reads to another port—it's about designing your app so users never notice the few milliseconds it takes for the bytes to arrive.
