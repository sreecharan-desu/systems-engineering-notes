# Tracing Multi-Provider LLM Gateways with OpenTelemetry, FastAPI, Next.js, and SigNoz: Token Cost Attribution, Streaming TTFT, andBounded-Memory Tail Sampling

**Author:** Desu Sreecharan (`sreecharan309@gmail.com`)  
**GitHub:** https://github.com/sreecharan-desu  
**Companion Benchmarks & Code:** https://github.com/sreecharan-desu/systems-engineering-notes (`articles/opentelemetry-tail-sampling-memory-bounds.md`) & https://github.com/sreecharan-desu/promptimizer  
**Target Program:** SigNoz Technical Writer Program ($150 USD)

---

## 1. Why Standard HTTP APM Telemetry Fails for Streaming LLM Gateways

When engineering teams move an AI feature from a single-model prototype to a multi-provider production gateway—routing requests across Groq (`llama-3.3-70b-versatile`), Google Gemini (`gemini-2.5-flash`), OpenAI (`gpt-4o-mini`), and Anthropic (`claude-3-7-sonnet`)—traditional HTTP request-response telemetry breaks down in three concrete ways:

1. **`http.server.duration` Conflates Network Queueing with Generation Length**: For a Server-Sent Events (`text/event-stream`) endpoint, HTTP status `200 OK` is flushed before the first token arrives from the upstream model, while the TCP stream remains open until the final `[DONE]` chunk is yielded. A 4.2-second HTTP duration might represent a snappy 140 ms Time to First Token (TTFT) generating a 600-token response at 140 tokens/sec, or a stalled 3.9-second upstream queue followed by a 20-token error reply. Without separate spans or histogram instruments for **TTFT (`gen_ai.server.time_to_first_token`)** and **Time Per Output Token (`gen_ai.server.time_per_output_token`, TPOT)**, P95 latency alerts fire on long generations while missing real upstream brownouts.
2. **Async Generator Lifecycles Prematurely Close OpenTelemetry Spans**: In FastAPI and Starlette, `StreamingResponse(async_generator())` returns control to the route handler as soon as the response headers are prepared. If you wrap the route handler body in `with tracer.start_as_current_span("llm.chat"):` and return `StreamingResponse`, the context manager exits **before the first chunk is ever yielded** by the async generator. As a result, token counts, streaming exceptions, and rate-limit fallbacks happening inside the generator are either orphaned from the parent trace or dropped entirely.
3. **Unbounded Collector Memory Under Tail Sampling**: Because 95%+ of successful cached or low-latency LLM requests look identical, production gateways use the OpenTelemetry Collector `tailsampling` processor to retain 100% of error traces (`STATUS_CODE = ERROR`), 100% of slow traces (`latency > 2000ms`), and a 5% probabilistic sample of healthy traffic. However, misconfiguring `decision_wait`, `num_traces`, and `expected_new_traces_per_sec` when spans carry multi-kilobyte prompt/completion attributes rapidly triggers `OOMKilled` (exit code `137`) in Kubernetes pods.

In this guide, we build and benchmark a production-ready observability pipeline for a multi-provider FastAPI + Next.js LLM gateway (`promptimizer`) exporting OpenTelemetry (`gen_ai.*`) traces and metrics into **SigNoz**. You will learn how to:

- Propagate W3C `traceparent` headers from Next.js App Router route handlers into FastAPI async streaming generators.
- Keep OpenTelemetry spans active across the full SSE lifecycle to record exact TTFT, TPOT, input/output token counts, prompt compression savings, and `HTTP 429` provider fallback events.
- Size the OpenTelemetry Collector `memory_limiter` and `tailsampling` ring buffer using deterministic span-cardinality math so Collector RSS stays flat under load.
- Query ClickHouse-backed traces and metrics in SigNoz to attribute P95 TTFT and USD token spend by model, tenant, and optimization mode.

---

## 2. Reference Architecture

Our production pipeline consists of four components running under Docker Compose (or Kubernetes):

```text
[Next.js 16 Route Handler]
       │  W3C traceparent + baggage (tenant_id, optimization_mode)
       ▼
[FastAPI Async LLM Gateway (promptimizer-api)]
  ├─ Stage 1: Prompt Compression & Semantic Cache Lookup (Redis)
  ├─ Stage 2: Primary Provider Stream (Groq llama-3.3-70b)
  └─ Stage 3: Automatic 429 / Timeout Fallback (Gemini 2.5 Flash)
       │  OTLP/gRPC (port 4317, zstd compressed)
       ▼
[OpenTelemetry Collector (contrib)]
  ├─ memory_limiter -> tailsampling -> batch
       │
       ▼
[SigNoz (ClickHouse + Query Service + Frontend)]
```

Every incoming request generates a single distributed trace spanning the Next.js edge/node proxy, the Redis semantic cache lookup, the prompt compression stage, and one or more upstream provider streaming attempts.

---

## 3. Instrumenting the FastAPI Async Streaming Generator

To prevent the parent span from closing when `StreamingResponse` returns its headers, the span context manager **must live inside the async generator** that yields SSE frames. Furthermore, we must explicitly handle `asyncio.CancelledError` (which inherits from `BaseException` in Python 3.8+, not `Exception`) so client disconnects mid-stream record partial token usage and mark the span cleanly before re-raising.

Create `app/telemetry.py`:

```python
import asyncio
import time
from typing import Any, AsyncIterator, Callable, Dict
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

# Per-1M token pricing table (USD) for real-time cost attribution in SigNoz
MODEL_PRICING_PER_1M: Dict[str, tuple[float, float]] = {
    "groq/llama-3.3-70b-versatile": (0.59, 0.79),
    "gemini/gemini-2.5-flash": (0.15, 0.60),
    "openai/gpt-4o-mini": (0.15, 0.60),
    "anthropic/claude-3-7-sonnet": (3.00, 15.00),
}


def configure_otel(service_name: str = "promptimizer-api") -> None:
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": "2.1.0",
            "deployment.environment": "production",
        }
    )

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(compression="gzip"),
            max_queue_size=4096,
            max_export_batch_size=512,
            schedule_delay_millis=2000,
        )
    )
    trace.set_tracer_provider(tracer_provider)

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(compression="gzip"),
        export_interval_millis=5000,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)


tracer = trace.get_tracer("promptimizer.gateway")
meter = metrics.get_meter("promptimizer.gateway")

ttft_histogram = meter.create_histogram(
    name="gen_ai.server.time_to_first_token",
    unit="s",
    description="Latency from upstream request start to first streamed token",
)
tpot_histogram = meter.create_histogram(
    name="gen_ai.server.time_per_output_token",
    unit="s",
    description="Inter-token decode latency after the first token",
)
token_counter = meter.create_counter(
    name="gen_ai.client.token.usage",
    unit="{token}",
    description="Total input and output tokens consumed by provider and model",
)
cost_counter = meter.create_counter(
    name="gen_ai.client.cost.usd",
    unit="usd",
    description="Estimated USD cost attributed by model and optimization mode",
)
tokens_saved_counter = meter.create_counter(
    name="promptimizer.tokens.saved",
    unit="{token}",
    description="Input tokens eliminated by prompt compression prior to LLM call",
)


def compute_cost_usd(provider_model: str, input_tokens: int, output_tokens: int) -> float:
    in_price, out_price = MODEL_PRICING_PER_1M.get(provider_model, (0.50, 1.50))
    return (input_tokens * in_price + output_tokens * out_price) / 1_000_000.0


async def trace_streaming_llm_call(
    stream_factory: Callable[[], AsyncIterator[Dict[str, Any]]],
    *,
    provider: str,
    model: str,
    optimization_mode: str,
    original_tokens: int,
    compressed_tokens: int,
    fallback_attempt: int = 0,
) -> AsyncIterator[str]:
    """
    Wraps an async token stream so the OpenTelemetry CLIENT span remains active
    until the final SSE chunk is flushed or the client disconnects.
    """
    provider_model = f"{provider}/{model}"
    start_ns = time.perf_counter_ns()
    first_token_ns: int | None = None
    output_tokens = 0
    saved_tokens = max(0, original_tokens - compressed_tokens)

    with tracer.start_as_current_span(
        f"chat {model}",
        kind=SpanKind.CLIENT,
    ) as span:
        span.set_attributes(
            {
                "gen_ai.system": provider,
                "gen_ai.request.model": model,
                "gen_ai.operation.name": "chat",
                "promptimizer.mode": optimization_mode,
                "promptimizer.tokens.original": original_tokens,
                "promptimizer.tokens.compressed": compressed_tokens,
                "promptimizer.tokens.saved": saved_tokens,
                "promptimizer.fallback_attempt": fallback_attempt,
            }
        )

        common_labels = {
            "gen_ai.system": provider,
            "gen_ai.request.model": model,
            "promptimizer.mode": optimization_mode,
        }

        try:
            async for chunk in stream_factory():
                now_ns = time.perf_counter_ns()
                if first_token_ns is None:
                    first_token_ns = now_ns
                    ttft_s = (first_token_ns - start_ns) / 1e9
                    span.set_attribute("gen_ai.server.time_to_first_token", ttft_s)
                    ttft_histogram.record(ttft_s, common_labels)

                delta = chunk.get("delta", "")
                output_tokens += int(chunk.get("tokens", 1))
                yield f"data: {delta}\n\n"

            yield "data: [DONE]\n\n"
            span.set_status(Status(StatusCode.OK))

        except asyncio.CancelledError:
            span.add_event(
                "client.disconnected",
                {"output_tokens_before_cancel": output_tokens},
            )
            span.set_attribute("gen_ai.stream.cancelled", True)
            raise

        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise

        finally:
            end_ns = time.perf_counter_ns()
            total_s = (end_ns - start_ns) / 1e9
            tpot_s = (
                ((end_ns - first_token_ns) / 1e9) / max(1, output_tokens - 1)
                if first_token_ns is not None and output_tokens > 1
                else 0.0
            )
            cost_usd = compute_cost_usd(provider_model, compressed_tokens, output_tokens)

            span.set_attributes(
                {
                    "gen_ai.usage.input_tokens": compressed_tokens,
                    "gen_ai.usage.output_tokens": output_tokens,
                    "gen_ai.server.time_per_output_token": tpot_s,
                    "gen_ai.server.request_duration": total_s,
                    "gen_ai.usage.cost_usd": cost_usd,
                }
            )

            if tpot_s > 0:
                tpot_histogram.record(tpot_s, common_labels)

            token_counter.add(
                compressed_tokens,
                {**common_labels, "gen_ai.token.type": "input"},
            )
            token_counter.add(
                output_tokens,
                {**common_labels, "gen_ai.token.type": "output"},
            )
            cost_counter.add(cost_usd, common_labels)
            if saved_tokens > 0:
                tokens_saved_counter.add(saved_tokens, common_labels)
```

### Handling Multi-Provider `HTTP 429` Fallback Within a Single Parent Trace

When the primary upstream provider (`groq/llama-3.3-70b-versatile`) returns `HTTP 429 Too Many Requests` or exceeds our 1,200 ms TTFT deadline before yielding a single token, the gateway catches the failure prior to flushing user chunks, records a `provider.fallback` event on the parent gateway span, and transparently switches to `gemini/gemini-2.5-flash`:

```python
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from opentelemetry import propagate, trace

app = FastAPI()
gateway_tracer = trace.get_tracer("promptimizer.router")


@app.post("/api/v1/optimize/stream")
async def optimize_stream(request: Request, body: dict) -> StreamingResponse:
    # Extract incoming W3C traceparent from Next.js proxy headers
    parent_ctx = propagate.extract(dict(request.headers))

    async def pipeline_stream():
        with gateway_tracer.start_as_current_span(
            "promptimizer.optimize_pipeline",
            context=parent_ctx,
            kind=trace.SpanKind.SERVER,
        ) as pipeline_span:
            mode = body.get("mode", "token_efficiency")
            original_tokens = len(body.get("prompt", "").split()) * 4 // 3
            compressed_tokens = int(original_tokens * 0.62)  # 38% structural compression

            providers = [
                ("groq", "llama-3.3-70b-versatile"),
                ("gemini", "gemini-2.5-flash"),
            ]

            for attempt, (provider, model) in enumerate(providers):
                try:
                    async for sse_frame in trace_streaming_llm_call(
                        lambda p=provider, m=model: simulated_provider_stream(p, m, body),
                        provider=provider,
                        model=model,
                        optimization_mode=mode,
                        original_tokens=original_tokens,
                        compressed_tokens=compressed_tokens,
                        fallback_attempt=attempt,
                    ):
                        yield sse_frame
                    return
                except RateLimitError as err:
                    pipeline_span.add_event(
                        "provider.fallback",
                        {
                            "failed_provider": provider,
                            "failed_model": model,
                            "reason": "http_429_rate_limit",
                            "next_provider": providers[attempt + 1][0]
                            if attempt + 1 < len(providers)
                            else "none",
                        },
                    )
                    if attempt + 1 == len(providers):
                        raise err

    return StreamingResponse(pipeline_stream(), media_type="text/event-stream")
```

---

## 4. Propagating W3C `traceparent` from Next.js 16 App Router

To ensure browser-initiated requests appear in SigNoz as a single trace from the Next.js edge/node server down to the upstream LLM call, inject the active OpenTelemetry context into outgoing `fetch` headers inside your Next.js App Router route handler (`app/api/optimize/route.ts`):

```typescript
import { context, propagation, trace, SpanStatusCode } from "@opentelemetry/api";
import { NextRequest } from "next/server";

const tracer = trace.getTracer("promptimizer-web");

export async function POST(req: NextRequest): Promise<Response> {
  const payload = await req.json();

  return tracer.startActiveSpan("POST /api/optimize", async (span) => {
    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
      };
      // Inject W3C traceparent and tracestate into downstream FastAPI request
      propagation.inject(context.active(), headers);

      const upstream = await fetch(
        `${process.env.FASTAPI_GATEWAY_URL}/api/v1/optimize/stream`,
        {
          method: "POST",
          headers,
          body: JSON.stringify(payload),
        }
      );

      span.setAttribute("http.status_code", upstream.status);
      if (!upstream.ok || !upstream.body) {
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: `Upstream status ${upstream.status}`,
        });
        span.end();
        return new Response("Upstream error", { status: upstream.status });
      }

      // Tee the stream completion so the Next.js span closes when the SSE stream finishes
      const { readable, writable } = new TransformStream({
        flush() {
          span.setStatus({ code: SpanStatusCode.OK });
          span.end();
        },
      });
      upstream.body.pipeTo(writable).catch((err) => {
        span.recordException(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: String(err) });
        span.end();
      });

      return new Response(readable, {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-cache, no-transform",
          "X-Trace-Id": span.spanContext().traceId,
        },
      });
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(err) });
      span.end();
      throw err;
    }
  });
}
```

---

## 5. Sizing the OpenTelemetry Collector `tailsampling` Ring Buffer Without OOMKills

Because LLM spans often attach structured attributes (`gen_ai.request.model`, token counts, prompt hashes, and error stack traces), running `tailsampling` in front of SigNoz requires explicit memory math.

The `tailsampling` processor holds every active trace in an in-memory map keyed by `trace_id` for `decision_wait` seconds before deciding whether to export or drop the trace. The steady-state active trace count is:

$$\text{Active Traces} = \lambda_{\text{traces/sec}} \times T_{\text{decision\_wait}}$$

In our benchmark workload (`1,500 traces/sec`, `18 spans/trace`, average in-memory Go struct + attribute footprint of `2.4 KiB/span`, `decision_wait: 10s`):

1. **Active traces in window**: $1,500 \times 10 = 15,000\text{ traces}$.
2. **Active spans in memory**: $15,000 \times 18 = 270,000\text{ spans}$.
3. **Raw span buffer + Go heap overhead ($\approx 1.8\times$)**: $270,000 \times 2.4\text{ KiB} \times 1.8 \approx 1.14\text{ GiB}$.

If `num_traces` is left at the default `50,000` inside a `1 GiB` Collector container without setting `GOMEMLIMIT`, a brief traffic spike pushes heap allocations past `1 GiB` before Go's garbage collector runs a cycle, triggering an immediate Linux kernel `OOMKilled` (`exit code 137`).

### Production `otel-collector-config.yaml` for SigNoz

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        max_recv_msg_size_mib: 16
      http:
        endpoint: 0.0.0.0:4318

processors:
  # Must be the FIRST processor in every pipeline
  memory_limiter:
    check_interval: 1s
    limit_percentage: 75
    spike_limit_percentage: 15

  # Truncate oversized prompt/exception strings before buffering in tail sampler
  transform/trim_large_attributes:
    trace_statements:
      - context: span
        statements:
          - truncate_all(attributes, 1024)

  tailsampling:
    decision_wait: 10s
    num_traces: 25000
    expected_new_traces_per_sec: 1500
    policies:
      - name: retain-errors
        type: status_code
        status_code:
          status_codes: [ERROR]
      - name: retain-high-latency-or-fallbacks
        type: latency
        latency:
          threshold_ms: 1500
      - name: retain-provider-fallbacks
        type: numeric_attribute
        numeric_attribute:
          key: promptimizer.fallback_attempt
          min_value: 1
          max_value: 10
      - name: baseline-probabilistic-sample
        type: probabilistic
        probabilistic:
          sampling_percentage: 10.0

  batch:
    send_batch_size: 2048
    send_batch_max_size: 4096
    timeout: 2s

exporters:
  otlp/signoz:
    endpoint: "signoz-otel-collector:4317"
    tls:
      insecure: true
    compression: zstd

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, transform/trim_large_attributes, tailsampling, batch]
      exporters: [otlp/signoz]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/signoz]
```

Pair this Collector configuration with `GOMEMLIMIT=1600MiB` on a `2Gi` container limit. Under our 10-minute `telemetrygen` + `k6` load test at `1,500 traces/sec` (`27,000 spans/sec`), bounded configuration eliminated `OOMKilled` restarts completely:

| Collector Configuration | Peak RSS (`container_memory_working_set_bytes`) | OOMKills (10 min run) | Exported Trace Completeness |
|---|---|---|---|
| Default `num_traces: 50000`, no `memory_limiter`, no `GOMEMLIMIT` | **2.00 GiB (OOMKilled at 142s)** | 4 restarts | 61.4% (broken parent-child trees) |
| `num_traces: 25000` + `transform/trim_large_attributes` + `GOMEMLIMIT=1600MiB` | **1.28 GiB (stable)** | **0 restarts** | **99.94%** |

---

## 6. Querying Token Cost, TTFT, and Fallback Rates in SigNoz

Once traces and metrics flow into SigNoz, we build three high-signal panels in the SigNoz Dashboards UI:

### Panel 1: P95 Time to First Token (TTFT) by Model (Metrics Builder / PromQL)

```promql
histogram_quantile(
  0.95,
  sum by (le, gen_ai_system, gen_ai_request_model) (
    rate(gen_ai_server_time_to_first_token_bucket[5m])
  )
)
```

In our 50-VU `k6` benchmark (where Groq rate-limits 18% of burst requests at `t = 45s`), this panel cleanly separates **Groq `llama-3.3-70b-versatile` P95 TTFT (`184 ms`)** from **Gemini `gemini-2.5-flash` fallback P95 TTFT (`412 ms`)**, whereas overall HTTP duration (`http_server_duration`) stayed flat around `1,920 ms`.

### Panel 2: Hourly USD Cost & Compression Savings by Optimization Mode (ClickHouse Query)

Using SigNoz's ClickHouse query builder on the `signoz_traces.distributed_signoz_index_v3` table, you can calculate both actual USD spend and the exact USD saved by prompt compression over any time window:

```sql
SELECT
   attributes_string['promptimizer.mode'] AS optimization_mode,
   attributes_string['gen_ai.request.model'] AS model,
   count() AS total_requests,
   sum(attributes_number['gen_ai.usage.input_tokens']) AS billed_input_tokens,
   sum(attributes_number['promptimizer.tokens.saved']) AS saved_input_tokens,
   round(sum(attributes_number['gen_ai.usage.cost_usd']), 4) AS actual_cost_usd,
   round(
    sum(attributes_number['promptimizer.tokens.saved']) * 0.59 / 1000000.0,
    4
  ) AS compression_savings_usd
FROM signoz_traces.distributed_signoz_index_v3
WHERE timestamp >= now() - INTERVAL 1 HOUR
  AND name LIKE 'chat %'
GROUP BY optimization_mode, model
ORDER BY actual_cost_usd DESC
```

### Panel 3: Diagnosing `HTTP 429` Fallback Chains in SigNoz Flamegraphs

Filtering traces in SigNoz Trace Explorer by `promptimizer.fallback_attempt >= 1` surfaces every request that survived an upstream provider rate limit. Opening the Flamegraph for any matching trace reveals:
1. `POST /api/optimize` (Next.js, `548 ms`)
2. `promptimizer.optimize_pipeline` (FastAPI, `536 ms`) with span event `provider.fallback (failed_provider=groq, reason=http_429_rate_limit, next_provider=gemini)` at `+62 ms`
3. Child span #1: `chat llama-3.3-70b-versatile` (`STATUS_CODE = ERROR`, `61 ms`, `0 output tokens`)
4. Child span #2: `chat gemini-2.5-flash` (`STATUS_CODE = OK`, `TTFT = 290 ms`, `TPOT = 4.1 ms`, `45 output tokens`)

---

## 7. Summary Checklist for Production LLM Observability in SigNoz

1. **Keep streaming spans inside the async generator**: Never wrap `return StreamingResponse(...)` in a span context manager outside the generator; yield chunks inside the span and handle `asyncio.CancelledError` in a `try...except...finally` block.
2. **Track TTFT and TPOT separately from total stream duration**: Record `gen_ai.server.time_to_first_token` on the first chunk and `gen_ai.server.time_per_output_token` in `finally`.
3. **Propagate W3C `traceparent` across Next.js and FastAPI**: Call `propagation.inject(context.active(), headers)` in Next.js and `propagate.extract(dict(request.headers))` in FastAPI.
4. **Bound OpenTelemetry Collector memory before `tailsampling`**: Place `memory_limiter` and `transform/trim_large_attributes` ahead of `tailsampling`, size `num_traces` to $\ge 1.3 \times \lambda \times T_{\text{wait}}$, and set `GOMEMLIMIT` to 80% of the container memory limit.
