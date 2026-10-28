# Systems Engineering Notes

Production write-ups, root-cause investigations, and reproducible benchmarks across PostgreSQL internals, Node.js V8 runtime observability, OpenTelemetry distributed tracing, Kubernetes zero-downtime networking, eBPF kernel telemetry, and Solana validator/DEX execution pipelines.

## Articles

1. **[Anatomy of a Silent PostgreSQL 17 Replication Lag Cascade: Diagnosing WAL Full-Page Image Bursts, Hot-Standby Snapshot Conflicts, and `pg_stat_io` Bottlenecks](articles/postgresql-17-replication-lag-wal-fpi-conflicts.md)** *(3,743 words)*
   - Reproducible two-node Percona Distribution for PostgreSQL 17 benchmark showing how post-checkpoint Full-Page Image (`BKPIMAGE`) amplification (`91.7%` FPI ratio via `pg_walinspect`) and hot-standby `IPC:RecoveryConflictSnapshot` stalls combine into 42-second `replay_lag` spikes, and how `wal_compression = zstd` + `recovery_prefetch = on` eliminates them.
2. **[Zero-Downtime PostgreSQL Schema Migrations and `pgvector` HNSW Index Builds at Scale: Avoiding `AccessExclusiveLock` Cascades, MVCC Table Rewrites, and Memory Traps](articles/postgresql-zero-downtime-migrations-pgvector-hnsw.md)** *(2,902 words)*
   - Why a `< 2 ms` metadata-only `ALTER TABLE` waiting behind a slow `SELECT` wedges 100% of OLTP traffic via `WaitOnLock`, how `NOT VALID` + `VALIDATE CONSTRAINT` enables instant `< 2 ms` `SET NOT NULL` promotions, and how to size `maintenance_work_mem` and `halfvec(1536)` expression indexes for in-memory `pgvector` HNSW builds.
3. **[Hunting Invisible Event-Loop Stalls in Node.js 22: Diagnosing V8 JSON Contention, `AsyncLocalStorage` Context Retention, and Undici Pool Exhaustion with `perf_hooks` and `diagnostics_channel`](articles/nodejs-22-event-loop-stalls-undici-diagnostics.md)** *(3,533 words)*
   - Combining `performance.eventLoopUtilization()` and `monitorEventLoopDelay()`, subscribing to `undici` lifecycle events via `node:diagnostics_channel` to separate local socket pool queue wait from upstream TTFB, preventing `AsyncLocalStorage` memory leaks with `AsyncLocalStorage.snapshot()`, and streaming bulk JSON via yielding async generators (`-97%` `p99` tail latency).
4. **[OpenTelemetry Tail Sampling Without OOMs: Sizing `decision_wait`, `num_traces`, and Load-Balanced Exporter Tiers](articles/opentelemetry-tail-sampling-memory-bounds.md)** *(2,381 words)*
   - Why running `tailsamplingprocessor` behind a round-robin Service produces fragmented trace decisions, how to configure two-tier `loadbalancingexporter` routing (`routing_key: traceID`), and how to bound collector heap usage with `memory_limiter` + `expected_new_traces_per_sec`.
5. **[Zero-Downtime Kubernetes Rollouts on Civo K3s: Why `SIGTERM` Alone Drops Requests and How `EndpointSlice` Propagation Really Works](articles/kubernetes-zero-downtime-rollouts-endpointslice-drain.md)** *(2,167 words)*
   - Control-plane race between `kubelet` `SIGTERM` and `EndpointSlice` propagation, `vegeta` 1,000 RPS benchmark results, and production `preStop` + keep-alive drain patterns.
6. **[Building Self-Healing Change Data Capture (CDC) Pipelines with Kestra, PostgreSQL Logical Replication, and DuckDB](articles/event-driven-cdc-pipelines-kestra-postgres-duckdb.md)** *(2,111 words)*
   - End-to-end orchestration of `wal2json` logical replication slots, idempotent Parquet staging, DuckDB MERGE compaction, and automated replication-slot lag circuit breakers.
7. **[Tracing Hidden Kubernetes Tail Latency with eBPF: Separating Kernel Socket Queueing from Application Stalls](articles/ebpf-socket-latency-tracing-kubernetes.md)** *(2,112 words)*
   - Using `libbpf` / `bpftrace` hooks on `tcp_v4_rcv`, `skb_copy_datagram_iter`, and epoll wakeup paths to measure time spent sitting in `sk_receive_queue` before the application thread reads the socket.
8. **[Diagnosing Hidden Memory Leaks in Node.js `Readable.from()` and Async Generator Pipelines](articles/nodejs-readable-from-async-generator-backpressure.md)** *(1,158 words)*
   - Reproducible heap benchmark comparing fire-and-forget `writable.write()`, `stream/promises` `pipeline()`, and `AbortSignal` cancellation under slow downstream sinks.
9. **[Solana DEX Execution & RaptorX Routing Analysis](articles/solana-dex-execution-and-raptorx-routing-analysis.md)** *(1,648 words)*
   - Empirical execution analysis of RaptorX (`raptorx.ai`), Claw AI 5.0 Trenching Agent, and RexScreener across Solana DEX pools.
10. **[Inside the Block: How Solana Validators Order Transactions](articles/solana-validator-transaction-ordering-pipeline.md)** *(2,247 words)*
    - Architecture and source-level analysis of Agave `BankingStage`, Jito-Solana `BlockEngineStage`, Stake-Weighted QoS (`SwQoS`), and Compute Unit priority-fee scheduling.
