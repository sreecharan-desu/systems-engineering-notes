# Systems Engineering Notes

Production write-ups and reproducible benchmarks on OpenTelemetry distributed tracing and Node.js stream backpressure.

## Articles

1. **[OpenTelemetry Tail Sampling Without OOMs: Sizing `decision_wait`, `num_traces`, and Load-Balanced Exporter Tiers](articles/opentelemetry-tail-sampling-memory-bounds.md)**
   - Why running `tailsamplingprocessor` behind a round-robin Service produces fragmented trace decisions, how to configure two-tier `loadbalancingexporter` routing (`routing_key: traceID`), and how to bound collector heap usage with `memory_limiter` + `expected_new_traces_per_sec`.
2. **[Diagnosing Hidden Memory Leaks in Node.js `Readable.from()` and Async Generator Pipelines](articles/nodejs-readable-from-async-generator-backpressure.md)**
   - Reproducible heap benchmark comparing fire-and-forget `writable.write()`, `stream/promises` `pipeline()`, and `AbortSignal` cancellation under slow downstream sinks.
