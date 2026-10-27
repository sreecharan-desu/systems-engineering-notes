# SigNoz Technical Writing Program Pitch ($150 USD)

**Applicant:** Desu Sreecharan (`sreecharan309@gmail.com`)  
**GitHub:** https://github.com/sreecharan-desu  
**Production Reference Repo:** https://github.com/sreecharan-desu/promptimizer  
**Target Compensation:** $150 USD (SigNoz Community Technical Writer Program)

---

## 1. Pitch Email / Submission Form Copy

**Subject:** Technical Article Pitch: End-to-End OpenTelemetry Tracing & Token Cost Attribution for Multi-Provider LLM Gateways (FastAPI + Next.js + SigNoz)

Hi SigNoz Content Team,

I'd love to write a hands-on, benchmark-backed tutorial for the SigNoz engineering blog:

> **"Tracing Multi-Provider LLM Gateways with OpenTelemetry, FastAPI, Next.js, and SigNoz: Token Cost Attribution, Streaming TTFT, and Semantic Cache Hit Rates Under Load"**

Most OpenTelemetry + LLM tutorials stop at auto-instrumenting a single `openai.chat.completions.create()` call in a toy script. In production AI gateways (like **Promptimizer**, my multi-Stage LLM optimization pipeline running on FastAPI, Next.js 16, and SQLite/Redis with Groq, Gemini, OpenAI, and Anthropic backends), engineers face three much harder observability problems:
1. **Cross-service context propagation across streaming SSE boundaries**: Propagating W3C `traceparent` headers from a Next.js App Router frontend through a FastAPI async streaming proxy without losing span context when the generator yields chunks after the initial HTTP 200 headers are flushed.
2. **GenAI Semantic Conventions (`gen_ai.*`) for multi-stage pipelines**: Recording `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.server.time_to_first_token` (TTFT), `gen_ai.server.time_per_output_token` (TPOT), and semantic cache hit/miss attributes so SigNoz ClickHouse queries can break down **P95 latency and USD cost per 1,000 requests by model and optimization mode**.
3. **Load testing & tail-latency root cause analysis**: Using a reproducible **k6** workload (50 concurrent virtual users firing multi-provider prompt optimization requests) to trigger provider rate-limit fallbacks (`HTTP 429` on Groq -> automatic fallback to Gemini 2.5 Flash) and diagnosing the exact latency spike in SigNoz's Flamegraph, Trace Funnel, and Exceptions views.

All code comes directly from a working Docker Compose + FastAPI + Next.js repository that readers can clone and run against SigNoz Cloud or self-hosted SigNoz in under 5 minutes.

Best regards,  
**Desu Sreecharan**  
GitHub: https://github.com/sreecharan-desu  
Reference Project: https://github.com/sreecharan-desu/promptimizer

---

## 2. Detailed Article Outline

1. **The Observability Gap in Production LLM Gateways**
   - Why standard HTTP latency metrics (`http.server.duration`) lie for Server-Sent Events (SSE) and token-streaming endpoints.
   - The 4 metrics every LLM gateway actually needs: **Time to First Token (TTFT)**, **Time Per Output Token (TPOT)**, **Token Compression / Cost Delta**, and **Provider Fallback Rate**.
   - Architecture overview (Mermaid diagram): `Next.js 16 Edge/Node Proxy -> W3C TraceContext -> FastAPI Async Pipeline (Detect -> Compress -> Route -> Evaluate) -> OTLP gRPC Exporter -> OpenTelemetry Collector -> SigNoz (ClickHouse)`.

2. **Setting Up SigNoz + OpenTelemetry Collector for `gen_ai.*` Telemetry**
   - Configuring `otel-collector-config.yaml` with batching, memory limiter, and resource detection processors.
   - Exporting custom span attributes and histogram instruments (`gen_ai.client.token.usage`, `gen_ai.server.time_to_first_token`, `gen_ai.pipeline.compression_ratio`).

3. **Instrumenting the FastAPI Multi-Provider LLM Pipeline**
   - Complete, copy-pasteable `telemetry.py` and `llm_gateway.py` using `opentelemetry-sdk`, `opentelemetry-instrumentation-fastapi`, and `opentelemetry-instrumentation-httpx`.
   - Wrapping an async streaming generator (`AsyncIterator[str]`) so the parent span remains open until the final `[DONE]` SSE frame is emitted, capturing exact TTFT on first chunk and total output tokens on stream completion.
   - Recording provider fallback events (`span.add_event("provider.fallback", {"from": "groq/llama-3.3-70b", "to": "gemini/gemini-2.5-flash", "reason": "rate_limit_429"})`).

4. **Propagating W3C `traceparent` from Next.js 16 to FastAPI**
   - Instrumenting Next.js (`instrumentation.ts` with `@vercel/otel` / `@opentelemetry/sdk-node`) so user-initiated prompt optimization requests carry a single unified `trace_id` from browser route handler to FastAPI worker and upstream LLM provider.

5. **Stress-Testing the Pipeline with k6 & Diagnosing Bottlenecks in SigNoz**
   - Running a 3-stage `k6` load script (ramp to 50 VUs, inject synthetic token bursts, force upstream 429 retries).
   - Building a **SigNoz Dashboard** with 4 production panels (ClickHouse queries + PromQL/Metrics Builder included):
     1. *TTFT P50 / P95 / P99 by Provider (`gen_ai.system`)*
     2. *Token Savings & USD Cost Saved per Hour (`promptimizer.tokens_saved_total`)*
     3. *Fallback & Retry Rate during Rate-Limit Storms*
     4. *End-to-End Trace Flamegraph showing Semantic Cache Hit (2.4 ms) vs Cold LLM Call (640 ms)*

---

## 3. Working Code Snippets Included in the Article

### 3.1 OpenTelemetry Async Streaming Span Wrapper (`app/telemetry.py`)

```python
import time
from typing import AsyncIterator, Dict, Any
from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.trace import Status, StatusCode

resource = Resource.create({
    "service.name": "promptimizer-api",
    "service.version": "2.1.0",
    "deployment.environment": "production",
})

tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(tracer_provider)

metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=5000)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

tracer = trace.get_tracer("promptimizer.gateway")
meter = metrics.get_meter("promptimizer.gateway")

ttft_histogram = meter.create_histogram(
    name="gen_ai.server.time_to_first_token",
    unit="s",
    description="Time from request start to first streamed token",
)
token_counter = meter.create_counter(
    name="gen_ai.client.token.usage",
    unit="{token}",
    description="Input and output tokens consumed per provider",
)
tokens_saved_counter = meter.create_counter(
    name="promptimizer.tokens.saved",
    unit="{token}",
    description="Tokens eliminated by prompt compression before upstream call",
)


async def trace_Order_streaming_completion(
    stream: AsyncIterator[Dict[str, Any]],
    *,
    provider: str,
    model: str,
    optimization_mode: str,
    original_tokens: int,
    compressed_tokens: int,
) -> AsyncIterator[str]:
    """Keeps the OpenTelemetry span active across the full async SSE lifecycle."""
    start_ns = time.perf_counter_ns()
    first_token_ns: int | None = None
    output_tokens = 0

    with tracer.start_as_current_span(
        f"chat {model}",
        kind=trace.SpanKind.CLIENT,
    ) as span:
        span.set_attributes({
            "gen_ai.system": provider,
            "gen_ai.request.model": model,
            "gen_ai.operation.name": "chat",
            "promptimizer.mode": optimization_mode,
            "promptimizer.tokens.original": original_tokens,
            "promptimizer.tokens.compressed": compressed_tokens,
            "promptimizer.tokens.saved": max(0, original_tokens - compressed_tokens),
        })

        try:
            async for chunk in stream:
                if first_token_ns is None:
                    first_token_ns = time.perf_counter_ns()
                    ttft_s = (first_token_ns - start_ns) / 1e9
                    span.set_attribute("gen_ai.server.time_to_first_token", ttft_s)
                    ttft_histogram.record(
                        ttft_s,
                        {"gen_ai.system": provider, "gen_ai.request.model": model},
                    )
                text_delta = chunk.get("delta", "")
                output_tokens += chunk.get("tokens", 1)
                yield f"data: {text_delta}\n\n"

            total_s = (time.perf_counter_ns() - start_ns) / 1e9
            tpot_s = (
                ((time.perf_counter_ns() - first_token_ns) / 1e9) / max(1, output_tokens - 1)
                if first_token_ns and output_tokens > 1
                else 0.0
            )
            span.set_attributes({
                "gen_ai.usage.input_tokens": compressed_tokens,
                "gen_ai.usage.output_tokens": output_tokens,
                "gen_ai.server.time_per_output_token": tpot_s,
                "gen_ai.server.request_duration": total_s,
            })
            token_counter.add(
                compressed_tokens,
                {"gen_ai.system": provider, "gen_ai.token.type": "input", "gen_ai.request.model": model},
            )
            token_counter.add(
                output_tokens,
                {"gen_ai.system": provider, "gen_ai.token.type": "output", "gen_ai.request.model": model},
            )
            tokens_saved_counter.add(
                max(0, original_tokens - compressed_tokens),
                {"promptimizer.mode": optimization_mode, "gen_ai.system": provider},
            )
            span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
```

### 3.2 k6 Load & Provider-Fallback Benchmark (`benchmarks/k6_llm_gateway.js`)

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    steady_and_burst: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '30s', target: 20 },
        { duration: '60s', target: 50 }, // Triggers Groq 429 -> Gemini 2.5 Flash fallback
        { duration: '20s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1800'],
  },
};

const MODES = ['clarity', 'token_efficiency', 'precision', 'guardrails'];

export default function () {
  const mode = MODES[Math.floor(Math.random() * MODES.length)];
  const payload = JSON.stringify({
    prompt: 'You are an expert SRE. Analyze these Kubernetes pod eviction logs and return a JSON remediation plan...',
    mode,
    provider: 'auto',
  });

  const res = http.post('http://localhost:8000/api/v1/optimize/stream', payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 200': (r) => r.status === 200,
    'contains trace id header': (r) => r.headers['X-Trace-Id'] !== undefined,
  });
  sleep(0.3);
}
```
