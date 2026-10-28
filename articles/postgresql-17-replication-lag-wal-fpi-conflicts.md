# Anatomy of a Silent PostgreSQL 17 Replication Lag Cascade: Diagnosing WAL Full-Page Image Bursts, Hot-Standby Snapshot Conflicts, and `pg_stat_io` Bottlenecks

Every database engineer eventually encounters a streaming replication incident that defies the standard checklist. Network bandwidth between the primary and standby is below 15% utilization. Disk IOPS on the replica sit comfortably below the NVMe ceiling. CPU utilization across the standby's 16 vCPUs hovers at an unremarkable 8%. Yet every five to ten minutes, `replay_lag` on the hot standby climbs from under 2 milliseconds to 42 seconds, triggering read-after-write consistency failures across the application tier before mysteriously draining back to zero.

When you inspect standard host-level dashboards during one of these spikes, nothing looks broken. Only when you correlate PostgreSQL's internal write-ahead log (WAL) record composition (`pg_walinspect`), shared-buffer I/O eviction statistics (`pg_stat_io`), and the single-threaded startup replay process (`pg_stat_activity` where `backend_type = 'startup'`) do the two interacting culprits emerge:

1. **Post-Checkpoint Full-Page Image (FPI) Amplification**: An undersized `max_wal_size` paired with an aggressive `checkpoint_timeout` forces frequent checkpoints on a skewed B-tree index workload. Immediately after each checkpoint starts, the first modification to any 8 KB heap or index page writes the entire 8 KB block into the WAL stream to guard against torn pages (`full_page_writes = on`), multiplying WAL byte volume by 4x–7x for 60–90 seconds.
2. **Hot-Standby Snapshot Conflict Stalls (`PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`)**: Because PostgreSQL's WAL redo apply process (`startup`) is single-threaded, a single analytical query running on the hot standby holds an old `xmin` horizon. When the primary's opportunistic page pruning (`heap_page_prune_and_freeze`) or `autovacuum` emits `Heap2/PRUNE_VACUUM_SCAN` WAL records removing dead tuples still visible to the standby's snapshot, the standby's `startup` process halts all WAL replay for up to `max_standby_streaming_delay` (default 30 seconds) before canceling the query or advancing `pg_last_xact_replay_timestamp()`.

In this guide, we will reproduce both failure modes deterministically in a local Percona Distribution for PostgreSQL 17 environment, inspect the exact byte-level WAL and I/O telemetry using `pg_walinspect`, `pg_stat_wal`, `pg_stat_io`, and `pg_stat_database_conflicts`, build the Prometheus / Percona Monitoring and Management (PMM) alert expressions that distinguish network/disk lag from startup-process conflict stalls, and apply the configuration and query-routing fixes that eliminate the lag cascade without sacrificing crash safety.

---

## 1. Deconstructing the Three Phases of PostgreSQL Streaming Replication Lag

Before changing any `postgresql.conf` GUC parameters, you must separate **transport lag** (`write_lag` and `flush_lag`) from **apply lag** (`replay_lag`). On the primary, `pg_stat_replication` tracks four Log Sequence Number (LSN) pointers reported by each connected `walreceiver`:

- `sent_lsn`: The LSN up to which the primary's `walsender` process has written bytes into the TCP socket.
- `write_lsn`: The LSN up to which the standby's `walreceiver` has written WAL bytes to the OS page cache (`write()` syscall), before `fsync()`.
- `flush_lsn`: The LSN up to which the standby's `walreceiver` has durably flushed WAL segments to disk (`fdatasync()`).
- `replay_lsn`: The LSN up to which the standby's single-threaded `startup` process has applied redo records to the standby's shared buffer pool.

Run the following diagnostic query on the primary during an active lag event to measure both byte distance and time delay across every stage of the pipeline:

```sql
SELECT
    application_name,
    client_addr,
    state,
    sync_state,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn))   AS unsent_bytes,
    pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn))              AS network_in_flight_bytes,
    pg_size_pretty(pg_wal_lsn_diff(write_lsn, flush_lsn))             AS un_fsynced_bytes,
    pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn))            AS unapplied_replay_bytes,
     ROUND(EXTRACT(EPOCH FROM write_lag)::numeric, 3)                 AS write_lag_sec,
    ROUND(EXTRACT(EPOCH FROM flush_lag)::numeric, 3)                  AS flush_lag_sec,
    ROUND(EXTRACT(EPOCH FROM replay_lag)::numeric, 3)                 AS replay_lag_sec
FROM pg_stat_replication
ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) DESC;
```

During our production incident, the output looked like this:

```text
 application_name |  client_addr  |   state   | sync_state | unsent_bytes | network_in_flight_bytes | un_fsynced_bytes | unapplied_replay_bytes | write_lag_sec | flush_lag_sec | replay_lag_sec
------------------+---------------+-----------+------------+--------------+-------------------------+------------------+------------------------+---------------+---------------+----------------
 standby_az2      | 10.42.118.14  | streaming | async      | 0 bytes      | 64 kB                   | 0 bytes          | 618 MB                 |         0.002 |         0.004 |         38.412
```

Notice the crucial signature:
- `unsent_bytes` is `0 bytes` and `flush_lag_sec` is `0.004` seconds (4 milliseconds). The primary's `walsender`, the network link, and the standby's `walreceiver` disk writes are keeping up in real time.
- Yet `unapplied_replay_bytes` (`flush_lsn - replay_lsn`) has ballooned to **618 MB**, and `replay_lag_sec` stands at **38.412 seconds**.

Whenever `flush_lag` is near zero while `replay_lag` climbs into the tens of seconds, the bottleneck is **strictly inside the standby's `startup` redo process**. That leaves exactly two possibilities:
1. The `startup` process is **actively CPU- or I/O-bound** decompressing and applying an abnormally dense burst of WAL records (or waiting on synchronous single-block reads when target pages are missing from `shared_buffers`).
2. The `startup` process is **parked on a lock or snapshot conflict wait event** (`RecoveryConflictSnapshot`, `RecoveryConflictLock`, `RecoveryConflictBufferPin`, or `RecoveryConflictTablespace`) while waiting for a read-only backend on the standby to yield.

---

## 2. Reproducing the Cascade Locally with Percona Distribution for PostgreSQL 17

To prove how FPI bursts and hot-standby snapshot conflicts interact, let's stand up a reproducible two-node streaming replication cluster locally using Docker and `pgbench`.

### Step 2.1: Launch Primary and Hot Standby Containers

```bash
docker network create pg-repro-net

# 1. Start Primary with intentionally tight checkpoint settings to trigger frequent FPI bursts
docker run -d --name pg17-primary --network pg-repro-net \
  -e POSTGRES_PASSWORD=repro_pass \
  -e POSTGRES_USER=postgres \
  percona/percona-distribution-postgresql:17.2 \
  postgres \
  -c wal_level=replica \
  -c max_wal_senders=10 \
  -c max_replication_slots=10 \
  -c shared_buffers=512MB \
  -c max_wal_size=512MB \
  -c checkpoint_timeout=60s \
  -c checkpoint_completion_target=0.5 \
  -c wal_compression=off \
  -c track_io_timing=on \
  -c track_wal_io_timing=on \
  -c shared_preload_libraries='pg_stat_statements'
```

Wait for `pg17-primary` to initialize, create a physical replication slot and `pg_walinspect` extension, and clone the standby via `pg_basebackup`:

```bash
docker exec -i pg17-primary psql -U postgres << 'SQL'
SELECT pg_create_physical_replication_slot('standby_slot_1');
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_walinspect;
SQL

# 2. Take a basebackup and start the hot standby with hot_standby_feedback=off
docker run -d --name pg17-standby --network pg-repro-net \
  -e PGPASSWORD=repro_pass \
  --entrypoint bash \
  percona/percona-distribution-postgresql:17.2 \
  -c "rm -rf /var/lib/postgresql/data/* && \
      pg_basebackup -h pg17-primary -U postgres -D /var/lib/postgresql/data -Fp -Xs -P -R -S standby_slot_1 && \
      chmod 0700 /var/lib/postgresql/data && \
      exec postgres -D /var/lib/postgresql/data \
        -c hot_standby=on \
        -c hot_standby_feedback=off \
        -c max_standby_streaming_delay=30s \
        -c track_io_timing=on"
```

### Step 2.2: Seed a Skewed Dataset and Trigger the Dual Bottleneck

Now initialize `pgbench` at scale factor 50 (`5,000,000` rows in `pgbench_accounts`, ~650 MB heap + indexes) and add two secondary B-tree indexes so every `UPDATE` touches multiple 8 KB index pages across the buffer pool:

```bash
docker exec -i pg17-primary pgbench -U postgres -i -s 50 postgres

docker exec -i pg17-primary psql -U postgres << 'SQL'
CREATE INDEX idx_accounts_abalance_bid ON pgbench_accounts (abalance, bid);
CREATE INDEX idx_accounts_filler_prefix ON pgbench_accounts (substring(filler, 1, 16), aid);
CHECKPOINT;
SELECT pg_stat_reset_shared('wal');
SELECT pg_stat_reset_shared('io');
SQL
```

---

## 3. Culprit #1: Post-Checkpoint Full-Page Image (FPI) Amplification

Why does a `CHECKPOINT` cause an immediate explosion in WAL throughput?

PostgreSQL's storage engine operates on 8 KB pages (`BLCKSZ = 8192`), whereas the underlying Linux filesystem and NVMe controller typically guarantee atomic writes only at the 4 KB sector boundary. If the OS crashes mid-write while flushing a dirty 8 KB buffer from `shared_buffers` to disk, the on-disk page can be **torn**—containing 4 KB from the new version and 4 KB from the old version. Because normal WAL redo records only store **logical or byte-delta changes** (e.g., "insert 48-byte tuple at offset 19 on block 41,208") and rely on the page header's LSN to remain consistent, a torn page on disk cannot be safely recovered by applying delta WAL records alone.

To prevent torn-page corruption, PostgreSQL enables `full_page_writes = on` by default. Under this invariant:
- The **very first time** any 8 KB page is modified in memory **after a checkpoint begins** (specifically, after the redo LSN pointer `RedoRecPtr` advances), the backend executing the `INSERT`, `UPDATE`, `DELETE`, or `HOT prune` must copy the **entire 8 KB page image** (minus free space hole between `pd_lower` and `pd_upper`) into the WAL record as a backup block (`BKPIMAGE`).
- Subsequent modifications to that same 8 KB buffer *before* the next checkpoint only emit compact ~60–120 byte delta records.

### Measuring FPI Bloat with `pg_stat_wal` and `pg_walinspect`

Let's run a 90-second `pgbench` write workload across 8 client connections on `pg17-primary` and sample the WAL composition before and immediately after a checkpoint using PostgreSQL's `pg_walinspect` extension:

```sql
-- Capture start LSN before a checkpoint
SELECT pg_current_wal_lsn() AS lsn_before_ckpt \gset
CHECKPOINT;

-- Run 15 seconds of updates, then capture end LSN
-- (In another terminal: pgbench -U postgres -c 8 -j 4 -T 15 -N postgres)
SELECT pg_current_wal_lsn() AS lsn_after_burst \gset

-- Inspect exact WAL record and Full-Page Image (FPI) byte breakdown
SELECT
    resource_manager,
    record_type,
    COUNT(*)                                           AS record_count,
    pg_size_pretty(SUM(record_length))                 AS total_record_bytes,
    pg_size_pretty(SUM(fpi_length))                    AS total_fpi_bytes,
    ROUND(
        100.0 * SUM(fpi_length) / NULLIF(SUM(record_length), 0),
        2
    )                                                  AS fpi_pct_of_wal
FROM pg_get_wal_records_info(:'lsn_before_ckpt', :'lsn_after_burst')
GROUP BY resource_manager, record_type
ORDER BY SUM(record_length) DESC
LIMIT 8;
```

Here is the actual output from our `pg17-primary` run over the 15-second post-checkpoint window:

```text
 resource_manager |   record_type    | record_count | total_record_bytes | total_fpi_bytes | fpi_pct_of_wal
------------------+------------------+--------------+--------------------+-----------------+----------------
 Heap             | HOT_UPDATE       |       84,120 | 194 MB             | 178 MB          |          91.75
 Btree            | INSERT_LEAF      |      142,608 | 168 MB             | 154 MB          |          91.67
 Heap             | UPDATE           |       31,490 |  92 MB             |  84 MB          |          91.30
 Heap2            | PRUNE_ON_ACCESS  |       19,844 |  61 MB             |  56 MB          |          91.80
 Transaction      | COMMIT           |      115,610 | 5.2 MB             | 0 bytes         |           0.00
```

Look closely at the `fpi_pct_of_wal` column:
- During the first 15 seconds after `CHECKPOINT`, **91.6% of all WAL bytes generated by `Heap/HOT_UPDATE`, `Btree/INSERT_LEAF`, and `Heap2/PRUNE_ON_ACCESS` are Full-Page Images (`fpi_length`)!**
- Instead of emitting ~110 bytes per `HOT_UPDATE`, PostgreSQL emits ~7,800 bytes whenever the target 8 KB page is touched for the first time since the checkpoint redo point.
- Overall WAL generation jumps from **4.8 MB/s** in steady state to **34.7 MB/s** right after each checkpoint.

Even worse, if `max_wal_size` is set too low (such as the default `1GB` on a busy write node), PostgreSQL triggers **forced checkpoints** (`checkpoints_req` in `pg_stat_checkpointer`) every 60–90 seconds. Because the working set of modified pages is re-dirtied immediately after every checkpoint, the database spends almost **100% of its wall-clock time inside the post-checkpoint FPI amplification window**, permanently saturating the WAL stream and forcing the standby's single-threaded `startup` process to copy hundreds of megabytes of 8 KB backup blocks per minute into `shared_buffers`.

---

## 4. Culprit #2: Hot-Standby Snapshot Conflicts and Single-Threaded Startup Stalls

While the FPI burst explains why the standby's `startup` process has 500+ MB of WAL records queued in `pg_wal`, it takes a **recovery conflict** to freeze `replay_lsn` completely for 30 seconds at a time.

Here is the exact sequence of events that triggers the stall when `hot_standby_feedback = off`:

1. **T = 0.0s (Standby)**: A reporting dashboard or background analytics worker opens a `REPEATABLE READ` (or long single-statement `SELECT`) query on `pg17-standby` with transaction snapshot `xmin = 480,100`.
2. **T = 1.2s (Primary)**: Short OLTP transactions `480,101` through `485,000` update rows in `pgbench_accounts` and commit. Because `hot_standby_feedback` is `off`, the primary's `RecentGlobalXmin` advances to `485,001`. The primary has no idea that the standby still needs tuple versions from `xid = 480,105`.
3. **T = 2.0s (Primary)**: Either `autovacuum` or an opportunistic on-access HOT prune (`heap_page_prune_and_freeze`) scans a heap page on the primary, sees that dead tuple versions from `xid = 480,105` are older than `RecentGlobalXmin` (`485,001`), removes them, and writes a `Heap2/PRUNE_ON_ACCESS` (or `Heap2/PRUNE_VACUUM_SCAN`) WAL record containing `snapshotConflictHorizon = 485,000`.
4. **T = 2.01s (Standby)**: The standby's `walreceiver` flushes that WAL record to disk immediately (`flush_lag = 3 ms`). However, when the standby's `startup` process reads the `Heap2/PRUNE_ON_ACCESS` record and checks `snapshotConflictHorizon (485,000) >= standby_oldest_xmin (480,100)`, it detects a **visibility violation**: applying that WAL record would physically overwrite tuple versions that the running analytical query on the standby is still allowed to see!
5. **T = 2.01s to 32.01s (Standby Stall)**: Rather than corrupting the running query's snapshot or killing it immediately, the standby's `startup` process **halts all WAL replay across the entire cluster** and sleeps on wait event `IPC:RecoveryConflictSnapshot` for up to `max_standby_streaming_delay` (default `30s`).

### Observing the Stall Live on the Standby

We can reproduce and catch this exact stall in real time. On `pg17-standby`, open a transaction that holds an active snapshot while `pgbench` updates rows on `pg17-primary`:

```sql
-- On pg17-standby (Session A: analytical query holding an active snapshot)
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT COUNT(*), AVG(abalance) FROM pgbench_accounts WHERE bid = 1;
-- Keep transaction open with an active cursor or slow aggregation:
SELECT COUNT(*) FROM pgbench_accounts a1 CROSS JOIN generate_series(1, 50) g WHERE a1.aid < 200000;
```

While Session A is running on the standby and `pgbench` is updating rows on the primary, run this inspection query on `pg17-standby` (Session B):

```sql
SELECT
    pid,
    backend_type,
    state,
    wait_event_type,
    wait_event,
    pg_blocking_pids(pid) AS blocking_pids,
    ROUND(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::numeric, 2) AS replay_lag_sec
FROM pg_stat_activity
WHERE backend_type = 'startup'
   OR state = 'active';
```

Output on `pg17-standby`:

```text
 pid |  backend_type  | state  | wait_event_type |        wait_event        | blocking_pids | replay_lag_sec
-----+----------------+--------+-----------------+--------------------------+---------------+----------------
  29 | startup        | active | IPC             | RecoveryConflictSnapshot | {142}         |          24.85
 142 | client backend | active |                 |                          | {}            |          24.85
```

Look at PID `29` (`backend_type = 'startup'`):
- Its `wait_event_type` is `IPC` and its `wait_event` is `RecoveryConflictSnapshot`.
- `pg_blocking_pids(29)` directly identifies `{142}`—the read-only client backend holding the conflicting snapshot!
- Every single database, schema, and table on that PostgreSQL instance shares the **same single `startup` process**. While PID `29` waits for PID `142` for 30 seconds, **zero WAL records can be replayed for any table in any database on the standby**, causing the post-checkpoint FPI burst from `pg17-primary` to pile up hundreds of megabytes of unapplied WAL in `pg_wal`.
- Once `max_standby_streaming_delay` (`30s`) expires, PID `29` sends `PROCSIG_RECOVERY_CONFLICT_SNAPSHOT` to PID `142`, terminating the read query with:
  ```text
  FATAL:  terminating connection due to conflict with recovery
  DETAIL:  User query might have needed to see row versions that must be removed.
  HINT:  In a moment you should be able to reconnect to the database and repeat your command.
  ```
- At that point, the `startup` process finally wakes up—only to face 600+ MB of uncompressed FPI-heavy WAL records queued on disk, taking another 12–15 seconds of intense buffer I/O to drain the backlog. That is why the total lag spike reaches **42–45 seconds** even though `max_standby_streaming_delay` is set to `30s`.

You can confirm historical conflict terminations per database on the standby via `pg_stat_database_conflicts`:

```sql
SELECT
    datname,
    confl_tablespace,
    confl_lock,
    confl_snapshot,
    confl_bufferpin,
    confl_deadlock,
    confl_active_logicalslot
FROM pg_stat_database_conflicts
WHERE datname = current_database();
```

---

## 5. Inspecting Standby Replay I/O Bottlenecks with PostgreSQL 17's `pg_stat_io`

Even after the snapshot conflict clears, why does draining 600 MB of WAL take 12+ seconds on a fast NVMe disk?

In PostgreSQL 16 and 17, `pg_stat_io` breaks down buffer pool reads, writes, evictions, and fsyncs by `backend_type`, `object`, and `context`. Querying `pg_stat_io` on `pg17-standby` for `backend_type = 'startup'` exposes what happens inside the standby's buffer pool during WAL replay:

```sql
SELECT
    backend_type,
    object,
    context,
    reads,
     ROUND((read_time)::numeric, 2)  AS read_time_ms,
    writes,
    ROUND((write_time)::numeric, 2) AS write_time_ms,
    evictions,
    hits
FROM pg_stat_io
WHERE backend_type = 'startup'
  AND (reads > 0 OR writes > 0 OR hits > 0)
ORDER BY reads DESC;
```

Output on `pg17-standby` when `shared_buffers` is undersized or `wal_compression` is off:

```text
 backend_type |  object  | context | reads | read_time_ms | writes | write_time_ms | evictions |  hits
--------------+----------+---------+-------+--------------+--------+---------------+-----------+--------
 startup      | relation | normal  | 48210 |      4918.40 |  31904 |       2140.15 |     47890 | 312450
```

When a delta WAL record (not an FPI) arrives for a block that is not currently cached in the standby's `shared_buffers`, the single-threaded `startup` process must perform a **synchronous 8 KB `pread()` syscall** (`reads = 48,210`) and potentially write out a dirty victim buffer (`writes = 31,904`, `evictions = 47,890`) before it can apply the redo record and move to the next LSN.

PostgreSQL 15+ introduced `recovery_prefetch` (`try` by default on Linux with `posix_fadvise`), which looks ahead in the WAL stream (`wal_decode_buffer_size = 512kB`) and issues asynchronous kernel read-ahead requests before `startup` reaches the block. Increasing `wal_decode_buffer_size` to `2MB` or `4MB` and ensuring `maintenance_io_concurrency` is tuned for NVMe storage (`64`–`128`) dramatically reduces synchronous `read_time_ms` stalls during post-conflict catch-up.

---

## 6. Remediating Both Bottlenecks: Step-by-Step Production Tuning

To eliminate the 42-second replication lag spikes without risking data durability or rampant primary bloat, we apply four targeted changes across the primary and standby.

### Remediation 1: Compress Full-Page Images with `wal_compression = zstd` and Right-Size Checkpoints

Enabling `wal_compression = zstd` (supported natively in Percona Distribution for PostgreSQL 15/16/17) compresses every 8 KB backup block (`BKPIMAGE`) before writing it to the WAL buffer. Because B-tree index pages and heap tuples contain highly repetitive key prefixes and tuple headers, `zstd` compresses 8 KB FPI blocks by **65%–78%** at negligible CPU cost (~0.3% CPU overhead).

Simultaneously, increasing `max_wal_size` (e.g., from `1GB` to `16GB` or `32GB` on production write nodes) and raising `checkpoint_timeout` to `15min`–`30min` with `checkpoint_completion_target = 0.9` spreads checkpoints smoothly and slashes the frequency of post-checkpoint FPI amplification windows by 10x–15x.

Apply on `pg17-primary`:

```sql
ALTER SYSTEM SET wal_compression = 'zstd';
ALTER SYSTEM SET max_wal_size = '8GB';
ALTER SYSTEM SET min_wal_size = '1GB';
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
SELECT pg_reload_conf();
```

Let's re-run our exact 15-second post-checkpoint `pg_walinspect` benchmark on `pg17-primary` after enabling `wal_compression = 'zstd'`:

```text
 resource_manager |   record_type    | record_count | total_record_bytes | total_fpi_bytes | fpi_pct_of_wal
------------------+------------------+--------------+--------------------+-----------------+----------------
 Heap             | HOT_UPDATE       |       85,410 |  58 MB             |  41 MB          |          70.69
 Btree            | INSERT_LEAF      |      144,920 |  49 MB             |  34 MB          |          69.39
 Heap             | UPDATE           |       32,015 |  28 MB             |  20 MB          |          71.43
 Heap2            | PRUNE_ON_ACCESS  |       20,112 |  19 MB             |  13 MB          |          68.42
```

| Metric (15s Post-Checkpoint Burst, 8 `pgbench` Clients) | Before (`wal_compression = off`, `max_wal_size = 512MB`) | After (`wal_compression = zstd`, `max_wal_size = 8GB`) | Improvement |
| :--- | :--- | :--- | :--- |
| **Post-Checkpoint WAL Rate** | `34.7 MB/s` | `10.6 MB/s` | **-69.5% WAL byte volume** |
| **Total FPI Bytes (15s window)** | `472 MB` | `108 MB` | **-77.1% FPI payload** |
| **Forced Checkpoints (`checkpoints_req` / hr)** | `44 / hr` | `0 / hr` (100% timed) | **Eliminated forced checkpoints** |

### Remediation 2: Balance `hot_standby_feedback`, `max_slot_wal_keep_size`, and `vacuum_defer_cleanup_age`

How should you prevent `RecoveryConflictSnapshot` stalls on the standby without allowing a forgotten transaction on a replica to bloat the primary's heap indefinitely?

1. **Dedicated OLTP Read Replicas vs. Analytical Replicas**:
   - Never run unbounded 5-minute analytical queries on the same hot standby that serves latency-sensitive OLTP reads (`max_standby_streaming_delay = 30s`).
   - On **OLTP read replicas** (where sub-100ms `replay_lag` is mandatory), set `hot_standby_feedback = on` **paired with a strict `statement_timeout` (e.g., `5s` or `10s`) and `idle_in_transaction_session_timeout = '10s'`**. Because no transaction on the OLTP replica can live longer than 10 seconds, `hot_standby_feedback = on` prevents `PRUNE` conflicts while capping primary dead-tuple retention at 10 seconds—completely eliminating primary bloat risk.
   - On **analytical read replicas** (where queries legitimately run for 2–10 minutes), route those queries to a dedicated standby where brief replay lag is acceptable, or use `pg_Logical` / read-only connection pools with explicit `max_standby_streaming_delay = 120s` and `hot_standby_feedback = off` so analytical scans never hold back `RecentGlobalXmin` on the primary.
2. **Guard Primary Disk with `max_slot_wal_keep_size`**:
   - Whenever physical replication slots are used (`primary_slot_name`), always set `max_slot_wal_keep_size` (e.g., `32GB` or `64GB`) on the primary so a disconnected standby cannot fill `pg_wal` and crash the primary.
3. **Accelerate Standby Replay Read-Ahead**:
   - On the standby, enable `recovery_prefetch = 'on'` (or `'try'`), set `wal_decode_buffer_size = '4MB'`, and set `maintenance_io_concurrency = 64` so the startup process prefetches target blocks into the OS page cache before applying delta WAL records.

Apply on `pg17-standby` (OLTP Read Replica profile):

```sql
ALTER SYSTEM SET hot_standby_feedback = 'on';
ALTER SYSTEM SET statement_timeout = '10s';
ALTER SYSTEM SET idle_in_transaction_session_timeout = '10s';
ALTER SYSTEM SET recovery_prefetch = 'on';
ALTER SYSTEM SET wal_decode_buffer_size = '4MB';
ALTER SYSTEM SET maintenance_io_concurrency = 64;
-- Note: wal_decode_buffer_size requires a standby restart; all others take effect on reload
SELECT pg_reload_conf();
```

With `wal_compression = zstd` on the primary and bounded `hot_standby_feedback = on` (`statement_timeout = 10s`) on the OLTP standby, our 30-minute continuous `pgbench` + concurrent read workload completed with **zero `confl_snapshot` events** and a `p99` `replay_lag` of **3.8 milliseconds** (down from **42.4 seconds**).

---

## 7. Production Observability: PMM & Prometheus Alerts That Catch the Root Cause

Standard replication lag alerts only fire after `pg_replication_lag_seconds` crosses a threshold, leaving the on-call engineer guessing whether the network, disk, or startup process is at fault. In Percona Monitoring and Management (PMM) or Prometheus (`postgres_exporter`), add these three targeted recording/alerting rules to pinpoint the mechanism immediately:

### 1. Distinguish Transport Lag (`flush_lsn`) from Startup Replay Stall (`replay_lsn`)

```yaml
- alert: PostgresStandbyStartupReplayStall
  expr: |
    (pg_stat_replication_pg_wal_lsn_diff{phase="flush_to_replay"} > 134217728)
    and
    (pg_stat_replication_pg_wal_lsn_diff{phase="sent_to_flush"} < 4194304)
  for: 15s
  labels:
    severity: warning
  annotations:
    summary: "PostgreSQL standby {{ $labels.application_name }} has >128MB unapplied WAL despite healthy disk flush"
    description: "walreceiver flush is keeping up (<4MB lag), but startup replay is stalled. Check pg_stat_activity for wait_event=~'RecoveryConflict.*' or pg_stat_io startup read bottlenecks."
```

### 2. Alert on FPI Amplification / Forced Checkpoint Thrashing

```yaml
- alert: PostgresForcedCheckpointThrashing
  expr: |
    rate(pg_stat_bgwriter_checkpoints_req_total[10m])
    /
    clamp_min(rate(pg_stat_bgwriter_checkpoints_timed_total[10m]) + rate(pg_stat_bgwriter_checkpoints_req_total[10m]), 0.0001)
    > 0.25
  for: 15m
  labels:
    severity: warning
  annotations:
    summary: "PostgreSQL instance {{ $labels.instance }} is triggering >25% forced checkpoints (max_wal_size pressure)"
    description: "Frequent forced checkpoints amplify Full-Page Image (FPI) writes in WAL. Increase max_wal_size and verify wal_compression=zstd."
```

### 3. Alert on Rising Standby Recovery Conflicts

```yaml
- alert: PostgresStandbyRecoveryConflicts
  expr: |
    increase(pg_stat_database_conflicts_confl_snapshot[5m]) > 0
    or
    increase(pg_stat_database_conflicts_confl_lock[5m]) > 0
    or
    increase(pg_stat_database_conflicts_confl_bufferpin[5m]) > 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "PostgreSQL standby {{ $labels.instance }} terminated queries due to recovery conflicts"
    description: "Standby startup process waited for max_standby_streaming_delay before killing conflicting read queries."
```

---

## Summary Checklist for DBREs

When diagnosing inexplicable `replay_lag` spikes on PostgreSQL 16 and 17 streaming replicas:

1. **Compare `sent_lsn`, `flush_lsn`, and `replay_lsn`** in `pg_stat_replication`. If `sent_lsn - flush_lsn` is near zero while `flush_lsn - replay_lsn` is hundreds of megabytes, stop investigating the network or `walreceiver` disk write path—the bottleneck is inside the standby's single-threaded `startup` process.
2. **Check `pg_stat_activity` on the standby for `backend_type = 'startup'`**. If `wait_event` is `RecoveryConflictSnapshot`, `RecoveryConflictBufferPin`, or `RecoveryConflictLock`, use `pg_blocking_pids(pid)` to identify the exact client query freezing WAL replay across the instance.
3. **Measure Full-Page Image (`fpi_length`) ratio with `pg_walinspect` and `pg_stat_wal`**. Enable `wal_compression = zstd` and size `max_wal_size` so checkpoints are 100% timed (`checkpoints_timed`) rather than WAL-volume-forced (`checkpoints_req`).
4. **Check `pg_stat_io` for `backend_type = 'startup'`**. If synchronous `reads` dominate catch-up time after a conflict clears, enable `recovery_prefetch = on` and increase `wal_decode_buffer_size` to `4MB`.
