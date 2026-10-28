# Zero-Downtime PostgreSQL Schema Migrations and `pgvector` HNSW Index Builds at Scale: Avoiding `AccessExclusiveLock` Cascades, MVCC Table Rewrites, and Memory Traps

Schema migrations and vector index rollouts on multi-hundred-gigabyte PostgreSQL tables rarely fail in staging. In a quiet test database, running `ALTER TABLE orders ADD COLUMN tenant_region text NOT NULL CHECK (tenant_region <> '')` or `CREATE INDEX ON document_chunks USING hnsw (embedding vector_cosine_ops)` finishes in seconds.

When that exact migration script hits a production primary handling 6,000 transactions per second, however, a single sub-second lock wait can trigger a catastrophic connection pileup inside five seconds. Even though PostgreSQL 11+ made adding a column with a non-volatile `DEFAULT` a fast metadata-only catalog update, the moment that `ALTER TABLE` statement requests an `AccessExclusiveLock` and waits behind a 4-second reporting query, **every subsequent `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on that table queues behind the waiting `ALTER TABLE` request**—exhausting your PgBouncer or application connection pool almost instantaneously.

With AI and hybrid-search workloads now living directly inside PostgreSQL alongside transactional tables via `pgvector` (`0.7` / `0.8` on PostgreSQL 16 and 17), database engineers face a second class of production migration hazards: building and maintaining Hierarchical Navigable Small World (HNSW) vector indexes (`CREATE INDEX CONCURRENTLY ... USING hnsw`) without exhausting `maintenance_work_mem`, starving parallel worker pools, or stalling `autovacuum` during the multi-hour second scan phase.

In this hands-on production guide, we will walk through:
1. Why PostgreSQL's lock queue (`WaitOnLock`) turns a metadata-only `ALTER TABLE` into a total outage, and how to build a resilient `lock_timeout` retry loop that guarantees zero lock-queue cascades.
2. Which DDL changes still trigger full physical table rewrites or full-table verification scans in PostgreSQL 16/17—and the exact `NOT VALID` + `VALIDATE CONSTRAINT` state machine that replaces `AccessExclusiveLock` with online `ShareUpdateExclusiveLock` scans.
3. How `pgvector` builds HNSW graphs in memory and on disk during `CREATE INDEX CONCURRENTLY`, how `maintenance_work_mem`, `m`, and `ef_construction` determine exact byte memory consumption per million 1,536-dimensional embeddings, and how to prevent long-running `CONCURRENTLY` builds from holding back `xmin` and bloating high-churn OLTP tables.

---

## 1. Anatomy of an `AccessExclusiveLock` Queue Cascade

To understand why a "fast, metadata-only" `ALTER TABLE` takes down an OLTP service, examine how PostgreSQL's lock manager (`src/backend/storage/lmgr/proc.c`) enforces fairness to prevent lock starvation.

PostgreSQL defines eight table-level lock modes, ordered by restrictiveness:
1. `ACCESS SHARE` (acquired by plain `SELECT`)
2. `ROW SHARE` (`SELECT FOR UPDATE / FOR SHARE`)
3. `ROW EXCLUSIVE` (`INSERT`, `UPDATE`, `DELETE`)
4. `SHARE UPDATE EXCLUSIVE` (`VACUUM`, `ANALYZE`, `CREATE INDEX CONCURRENTLY`, `ALTER TABLE ... VALIDATE CONSTRAINT`)
5. `SHARE` (`CREATE INDEX` non-concurrent)
6. `SHARE ROW EXCLUSIVE` (`CREATE TRIGGER`)
7. `EXCLUSIVE` (`REFRESH MATERIALIZED VIEW CONCURRENTLY`)
8. `ACCESS EXCLUSIVE` (`ALTER TABLE ADD/DROP COLUMN`, `DROP TABLE`, `TRUNCATE`, `REINDEX TABLE`)

`ACCESS EXCLUSIVE` conflicts with **every other lock mode**, including `ACCESS SHARE` (`SELECT`). Now trace the exact timeline on a busy table `public.ledger_entries`:

- **T = 0.00s**: Backend PID `101` (an analytics dashboard widget or ORM background job) runs a `SELECT` query that takes `3.5 seconds`, holding an `AccessShareLock` on `public.ledger_entries`.
- **T = 0.20s**: Your deployment pipeline connects as PID `202` and executes:
  ```sql
  ALTER TABLE public.ledger_entries ADD COLUMN settlement_channel varchar(32) DEFAULT 'ach' NOT NULL;
  ```
  Even though executing this catalog update takes `< 2 milliseconds` once the lock is granted, PID `202` cannot acquire `AccessExclusiveLock` while PID `101` holds `AccessShareLock`. PID `202` enters the lock wait queue (`wait_event_type = 'Lock'`, `wait_event = 'relation'`).
- **T = 0.21s to 3.50s (The Cascade)**: While PID `202` is waiting in the lock queue, **6,000 new OLTP requests per second arrive** (`SELECT` needing `AccessShareLock`, `INSERT`/`UPDATE` needing `RowExclusiveLock`).
  - Normally, `AccessShareLock` and `RowExclusiveLock` do **not** conflict with PID `101`'s `AccessShareLock`.
  - However, PostgreSQL's lock manager prevents lock starvation: **no new lock request is granted if it conflicts with a lock request already waiting ahead of it in the queue!**
  - Because `AccessShareLock` and `RowExclusiveLock` conflict with PID `202`'s pending `AccessExclusiveLock`, **100% of incoming `SELECT`, `INSERT`, `UPDATE`, and `DELETE` queries on `public.ledger_entries` are blocked behind PID `202`**!
  - Within 100 milliseconds, all 200 connections in your application pool are wedged waiting on `Lock:relation`.

### Visualizing the Lock Queue Tree Live in SQL

Whenever a migration or DDL command is running, you can inspect the exact waiter chain—who holds the lock, which DDL session is waiting at the head of the queue, and how many OLTP queries are piled up behind the DDL session—using `pg_locks` and `pg_blocking_pids()`:

```sql
SELECT
    blocked.pid                                             AS waiting_pid,
    blocked.usename                                         AS waiting_user,
    blocked.state                                           AS waiting_state,
    blocked.wait_event_type || ':' || blocked.wait_event    AS wait_event,
    ROUND(EXTRACT(EPOCH FROM (now() - blocked.query_start))::numeric, 2) AS waiting_sec,
    pg_blocking_pids(blocked.pid)                           AS blocked_by_pids,
    LEFT(blocked.query, 90)                                 AS waiting_query
FROM pg_stat_activity AS blocked
WHERE cardinality(pg_blocking_pids(blocked.pid)) > 0
ORDER BY blocked.query_start ASC;
```

### The Mandatory Defense: Bounded `lock_timeout` Retry Loops

Never execute a DDL statement requiring `AccessExclusiveLock` or `ShareRowExclusiveLock` with the default `lock_timeout = 0` (wait forever). Instead, wrap every DDL statement in a bounded retry loop that sets `SET LOCAL lock_timeout = '50ms'`.

If the DDL session cannot acquire `AccessExclusiveLock` within 50 milliseconds, PostgreSQL immediately aborts the lock attempt (`ERROR: canceling statement due to lock timeout`, SQLSTATE `55P03`), **evicting the DDL request from the lock queue so OLTP `SELECT` and `INSERT` queries never wait more than 50 ms**, sleeps with jitter, and retries:

```sql
-- Safe, zero-cascade DDL wrapper in PL/pgSQL
DO $$
DECLARE
    v_attempt int := 1;
    v_max_attempts constant int := 40;
BEGIN
    LOOP
        BEGIN
            -- Never sit in the lock queue for more than 50ms!
            SET LOCAL lock_timeout = '50ms';
            SET LOCAL statement_timeout = '2000ms';

            ALTER TABLE public.ledger_entries
                ADD COLUMN IF NOT EXISTS settlement_channel varchar(32) DEFAULT 'ach' NOT NULL;

            RAISE NOTICE 'DDL succeeded on attempt %', v_attempt;
            EXIT;
        EXCEPTION
            WHEN lock_not_available THEN -- SQLSTATE 55P03 (lock_timeout expired)
                IF v_attempt >= v_max_attempts THEN
                    RAISE EXCEPTION 'Failed to acquire AccessExclusiveLock within 50ms after % attempts', v_max_attempts;
                END IF;
                RAISE NOTICE 'Attempt % timed out after 50ms; backing off so OLTP traffic is unaffected...', v_attempt;
                v_attempt := v_attempt + 1;
                -- Commit/release local lock state by sleeping briefly in a retry loop
                PERFORM pg_sleep(0.25 + (random() * 0.25));
        END;
    END LOOP;
END;
$$;
```

---

## 2. Separating Metadata-Only DDL from Hidden Table Rewrites and Scans in PostgreSQL 16/17

Even with `lock_timeout = '50ms'`, `lock_timeout` only caps **how long PostgreSQL waits to acquire the lock**—once the lock is acquired, the DDL statement holds `AccessExclusiveLock` until it finishes executing! If the operation rewrites or scans a 200 GB table, `AccessExclusiveLock` will remain held for 20 minutes.

Use this exact matrix for PostgreSQL 16 and 17 to guarantee that any statement holding `AccessExclusiveLock` completes in `< 5 ms`:

| Schema Change Operation | Naive Command (Dangerous on Large Tables) | Lock Held & Duration | Zero-Downtime Pattern in PostgreSQL 16/17 |
| :--- | :--- | :--- | :--- |
| **Add Column with Constant Default** | `ALTER TABLE t ADD COLUMN c int DEFAULT 0 NOT NULL;` | `AccessExclusiveLock` (**< 2 ms**, stores default in `pg_attribute.atthasmissing` / `attmissingval`) | Safe directly when wrapped in `lock_timeout = '50ms'`! |
| **Add Column with Volatile Default** | `ALTER TABLE t ADD COLUMN id uuid DEFAULT gen_random_uuid() NOT NULL;` | `AccessExclusiveLock` (**Full table rewrite!** Evaluates volatile function for every existing row) | 1. `ADD COLUMN id uuid NULL`<br>2. Backfill in batches<br>3. `ALTER COLUMN id SET DEFAULT gen_random_uuid()`<br>4. Add `CHECK (id IS NOT NULL) NOT VALID` + `VALIDATE` + `SET NOT NULL` |
| **Add `CHECK` Constraint** | `ALTER TABLE t ADD CONSTRAINT ck CHECK (amount >= 0);` | `AccessExclusiveLock` (**Full table scan!** Blocks all reads & writes during scan) | 1. `ALTER TABLE t ADD CONSTRAINT ck CHECK (amount >= 0) NOT VALID;` (`< 2 ms`)<br>2. `ALTER TABLE t VALIDATE CONSTRAINT ck;` (holds only `ShareUpdateExclusiveLock` — **allows concurrent reads & writes!**) |
| **Add `FOREIGN KEY`** | `ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES parent(id);` | `ShareRowExclusiveLock` on both tables (**Full table scan!** Blocks writes on both parent & child) | 1. `ADD CONSTRAINT fk FOREIGN KEY (...) NOT VALID;` (`< 3 ms` with `lock_timeout = '50ms'`)<br>2. `ALTER TABLE child VALIDATE CONSTRAINT fk;` (`ShareUpdateExclusiveLock` only) |
| **Set Column `NOT NULL`** | `ALTER TABLE t ALTER COLUMN c SET NOT NULL;` | `AccessExclusiveLock` (**Full table scan** unless proven non-null by an existing validated `CHECK` constraint!) | 1. `ADD CONSTRAINT ck_c_nn CHECK (c IS NOT NULL) NOT VALID;`<br>2. `VALIDATE CONSTRAINT ck_c_nn;` (online scan)<br>3. `ALTER COLUMN c SET NOT NULL;` (**< 2 ms instant catalog update** in PG 12+ because `ck_c_nn` proves no `NULL`s exist!)<br>4. `DROP CONSTRAINT ck_c_nn;` |
| **Widen `varchar(N)` to `varchar(M)` or `text`** | `ALTER TABLE t ALTER COLUMN c TYPE varchar(255);` | `AccessExclusiveLock` (**< 2 ms** metadata update if unconstrained by index/view) | Safe with `lock_timeout = '50ms'` (same binary `varlena` representation on disk). |
| **Change `int4` to `int8` (`bigint`)** | `ALTER TABLE t ALTER COLUMN id TYPE bigint;` | `AccessExclusiveLock` (**Full table + index rewrite!**) | Expand-and-contract pattern: add `id_v2 bigint`, dual-write via trigger, batched backfill, build concurrent index, swap column names in a single `< 5 ms` transaction. |

### Proving the Instant `SET NOT NULL` Optimization via `NOT VALID` + `VALIDATE`

Let's verify on PostgreSQL 17 how a validated `CHECK (col IS NOT NULL)` constraint eliminates the full-table scan during `ALTER COLUMN ... SET NOT NULL`:

```sql
-- Step 1: Add the NOT VALID constraint (metadata-only, < 2ms under lock_timeout)
SET lock_timeout = '50ms';
ALTER TABLE public.ledger_entries
    ADD CONSTRAINT chk_ledger_entries_channel_not_null
    CHECK (settlement_channel IS NOT NULL) NOT VALID;
RESET lock_timeout;

-- Step 2: Validate the constraint online.
-- Acquires only ShareUpdateExclusiveLock: concurrent SELECT, INSERT, UPDATE, DELETE continue unimpeded!
ALTER TABLE public.ledger_entries
    VALIDATE CONSTRAINT chk_ledger_entries_channel_not_null;

-- Step 3: Promote column to native NOT NULL and drop the redundant CHECK constraint in one <2ms transaction
BEGIN;
SET LOCAL lock_timeout = '50ms';
-- PostgreSQL inspects existing validated CHECK constraints on the table, sees chk_ledger_entries_channel_not_null,
-- and skips the full table scan completely!
ALTER TABLE public.ledger_entries
    ALTER COLUMN settlement_channel SET NOT NULL,
    DROP CONSTRAINT chk_ledger_entries_channel_not_null;
COMMIT;
```

---

## 3. Production `pgvector` Migrations: Sizing HNSW Index Builds Without Blowing Up Memory or MVCC Horizons

As teams integrate semantic search and retrieval-augmented generation (RAG) directly into transactional PostgreSQL databases using `pgvector` (`0.7+` / `0.8+`), the single hardest operational step is building or rebuilding an **HNSW (`Hierarchical Navigable Small World`) index** on millions of high-dimensional vector rows without degrading primary OLTP performance.

### Why HNSW Index Builds Behave Differently from B-Tree Builds

When PostgreSQL builds a standard B-tree index (`CREATE INDEX CONCURRENTLY`), it reads tuples from the heap, sorts keys using external merge sort tapes bounded neatly by `maintenance_work_mem`, and writes leaf and internal pages sequentially.

An HNSW index (`USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)`), by contrast, is a **multi-layer proximity graph**:
- Inserting each new vector into the HNSW graph requires a greedy beam search of width `ef_construction` across existing graph nodes and bidirectional updates to up to `2 * m` neighbor lists on layer 0 (plus `m` neighbors on higher layers).
- **As long as the entire HNSW graph fits inside `maintenance_work_mem`**, `pgvector` builds the graph in shared memory (using `max_parallel_maintenance_workers` worker processes), achieving **10x–25x higher insertion throughput**.
- **The moment the HNSW graph exceeds `maintenance_work_mem`**, `pgvector` logs:
  ```text
  NOTICE:  hnsw graph no longer fits into maintenance_work_mem after 1420850 tuples
  DETAIL:  Building will take significantly more time.
  HINT:  Increase maintenance_work_mem to speed up builds.
  ```
  Once `pgvector` switches from in-memory graph construction to on-disk buffer pool traversal, every single vector insertion performs random 8 KB buffer accesses across the partially built index, dropping build speed by an order of magnitude and saturating shared-buffer I/O (`pg_stat_io`).

### Exact Memory Formula for `pgvector` HNSW Graphs

Before running `CREATE INDEX CONCURRENTLY ... USING hnsw`, calculate the exact in-memory graph footprint required so the build completes 100% in memory without spilling to disk:

For $N$ vectors of dimension $D$ stored as:
- `vector(D)` (32-bit `float4`, $4 \times D$ bytes per vector), or
- `halfvec(D)` (16-bit `float16` introduced in `pgvector 0.7+`, $2 \times D$ bytes per vector—cutting memory and index size by nearly **50%** with negligible recall loss!), or
- `bit(D)` (binary quantization, $D / 8$ bytes per vector)

 combined with HNSW node metadata and neighbor pointers ($\approx 1.15 \times (2 \cdot m \cdot 4 + 64)$ bytes of graph edge + header overhead per tuple):

$$\text{Bytes per Tuple (float32)} \approx (4 \times D) + (9.2 \times m) + 80$$

$$\text{Bytes per Tuple (halfvec float16)} \approx (2 \times D) + (9.2 \times m) + 80$$

Let's compare the exact `maintenance_work_mem` required for **2,000,000 rows** of **1,536-dimensional embeddings** (`m = 16`, `ef_construction = 64`):

| Vector Storage Type | Raw Vector Payload / Row | Graph + Metadata Overhead / Row (`m = 16`) | Total Graph Memory for 2M Rows | Recommended Session `maintenance_work_mem` |
| :--- | :--- | :--- | :--- | :--- |
| **`vector(1536)` (`float32`)** | `6,144 B` | `~228 B` | **~11.87 GB** | `14GB` (with 20% safety headroom) |
| **`halfvec(1536)` (`float16`)** | `3,072 B` | `~228 B` | **~6.15 GB** | `8GB` (cuts RAM & disk size by **48%**) |
| **`vector(768)` (`float32`)** | `3,072 B` | `~228 B` | **~6.15 GB** | `8GB` |

> **Critical Production Rule**: Never increase `maintenance_work_mem = '14GB'` globally in `postgresql.conf`! Setting it globally allows every parallel `autovacuum` worker (`autovacuum_max_workers`) to allocate up to `autovacuum_work_mem` (which defaults to `maintenance_work_mem`), risking an immediate Linux OOM killer event. Always set `SET maintenance_work_mem = '8GB'` **strictly inside the dedicated migration session** executing `CREATE INDEX CONCURRENTLY`.

### Executing and Monitoring a Safe Online HNSW Build

When you run `CREATE INDEX CONCURRENTLY`, PostgreSQL executes three distinct phases:
1. **Phase 1**: Acquires `ShareUpdateExclusiveLock` (does not block reads or writes), registers the invalid index catalog entry, and waits for all transactions that started before the catalog update to finish.
2. **Phase 2 (`scanning table` / `loading tuples`)**: Scans the table using a single MVCC snapshot and builds the HNSW graph (in parallel across `max_parallel_maintenance_workers`).
3. **Phase 3 (`performing final scan`)**: Waits for all concurrent write transactions open during Phase 2 to commit, then performs a second pass to insert any tuples added or updated while Phase 2 was running.

Because Phases 2 and 3 hold an active MVCC `xmin` horizon on the table being indexed, an HNSW build that spills to disk and runs for 6 hours prevents `autovacuum` from reclaiming dead tuples on that table for 6 hours! By sizing `maintenance_work_mem` (or using `halfvec(1536)` expression indexing) so Phase 2 stays in memory, the build finishes in minutes instead of hours:

```sql
-- Dedicated migration session for zero-downtime HNSW index creation
-- 1. Scope memory and parallel workers strictly to this session
SET maintenance_work_mem = '8GB';
SET max_parallel_maintenance_workers = 4;
-- Guard against waiting forever on long-running transactions during Phase 1 / Phase 3 barriers
SET lock_timeout = '0';
SET statement_timeout = '45min';

-- 2. Build HNSW index concurrently using halfvec cast to halve RAM & disk footprint
-- without altering the underlying vector(1536) column!
CREATE INDEX CONCURRENTLY idx_doc_chunks_embedding_halfvec_hnsw
    ON public.document_chunks
    USING hnsw ((embedding::halfvec(1536)) halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);
```

Notice the expression index `((embedding::halfvec(1536)) halfvec_cosine_ops)`:
- Your table column remains standard `embedding vector(1536)` (preserving full 32-bit precision on disk for exact re-ranking).
- The HNSW index stores 16-bit `halfvec(1536)` vectors, cutting index build RAM from **11.9 GB to 6.1 GB**, cutting on-disk index size by **48%**, and doubling shared-buffer cache residency during similarity searches (`ORDER BY embedding::halfvec(1536) <=> $1::halfvec(1536) LIMIT 20`).

### Monitoring `CREATE INDEX CONCURRENTLY` Progress in Real Time

While the index build is running, monitor its exact phase, tuple percentage, and lock wait barriers from another session using `pg_stat_progress_create_index`:

```sql
SELECT
    p.pid,
    a.query_start,
    ROUND(EXTRACT(EPOCH FROM (now() - a.query_start))::numeric, 1) AS elapsed_sec,
    p.relid::regclass                                              AS table_name,
    p.index_relid::regclass                                        AS index_name,
    p.phase,
    p.blocks_done,
    p.blocks_total,
    ROUND(100.0 * p.blocks_done / NULLIF(p.blocks_total, 0), 2)    AS block_pct,
    p.tuples_done,
    p.tuples_total,
    ROUND(100.0 * p.tuples_done / NULLIF(p.tuples_total, 0), 2)    AS tuple_pct,
    p.lockers_done,
    p.lockers_total,
    p.current_locker_pid
FROM pg_stat_progress_create_index AS p
JOIN pg_stat_activity AS a ON a.pid = p.pid;
```

And if a `CREATE INDEX CONCURRENTLY` ever fails or is canceled mid-build, remember that PostgreSQL leaves behind an **`INVALID` index** that still incurs write overhead on every `INSERT`/`UPDATE` while never being used by the query planner! Always include an invalid-index check in your migration verification script:

```sql
-- Verify zero INVALID indexes remain after concurrent index migrations
SELECT
    n.nspname                  AS schema_name,
    t.relname                  AS table_name,
    i.relname                  AS invalid_index_name,
    pg_size_pretty(pg_relation_size(i.oid)) AS wasted_disk_size
FROM pg_index AS ix
JOIN pg_class AS i ON i.oid = ix.indexrelid
JOIN pg_class AS t ON t.oid = ix.indrelid
JOIN pg_namespace AS n ON n.oid = t.relnamespace
WHERE NOT ix.indisvalid;
```

---

## 4. Summary Checklist for Production PostgreSQL Migrations

1. **Wrap every `ALTER TABLE` in a `SET LOCAL lock_timeout = '50ms'` retry loop**: Never allow a pending `AccessExclusiveLock` to sit in the lock queue behind a slow `SELECT` and stall thousands of incoming OLTP requests.
2. **Use `NOT VALID` + `VALIDATE CONSTRAINT` for all new `CHECK`, `FOREIGN KEY`, and `NOT NULL` constraints**: Acquire `AccessExclusiveLock` for `< 2 ms` to register the unvalidated constraint, run the online validation scan under `ShareUpdateExclusiveLock`, and then promote to `SET NOT NULL` instantaneously.
3. **Calculate HNSW graph memory before running `CREATE INDEX CONCURRENTLY ... USING hnsw`**: Size session-local `maintenance_work_mem` so the entire graph builds in RAM without spilling to disk, and use `halfvec(D)` expression indexing on 1,536+ dimensional embeddings to halve memory and shared-buffer footprint.
4. **Audit `pg_index.indisvalid = false` after every deployment**: Drop or `REINDEX INDEX CONCURRENTLY` any invalid concurrent index left behind by a timed-out or canceled migration.
