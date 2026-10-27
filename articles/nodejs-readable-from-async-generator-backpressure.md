# AppSignal Guest Author Program Pitch ($300 USD)

**Applicant:** Desu Sreecharan (`sreecharan309@gmail.com`)  
**GitHub:** https://github.com/sreecharan-desu  
**Target Compensation:** $300 USD (AppSignal Technical Blog — Python / Node.js / TypeScript Track)

---

## 1. Pitch Email / Submission Copy

**Subject:** Article Pitch (Node.js/TypeScript & Python): Hunting Hidden Event-Loop Stalls and Memory Leaks in High-Throughput Server-Sent Events (SSE) & Async Generators

Hi AppSignal Editorial Team,

I'd like to pitch an in-depth, code-first engineering article for the AppSignal blog (happy to tailor the primary runtime to either your **Node.js / TypeScript** or **Python** track, as I have reproducible benchmarks in both):

> **"Why Your Streaming API Leaks Memory Under Backpressure: Diagnosing Slow Consumers, Unclosed Async Generators, and Event-Loop Stalls in Node.js and Python"**

With SSE (`text/event-stream`) and `ReadableStream` / `AsyncGenerator` now the default transport for AI streaming, live dashboards, and real-time notifications, a subtle class of production outages has emerged: **slow or silently disconnected clients that never trigger stream cleanup**.
- In **Node.js / TypeScript**, pushing chunks via `controller.enqueue()` or `res.write()` without respecting `desiredSize` / `'drain'` buffers unbounded `Uint8Array` chunks in V8 heap memory while synchronous JSON serialization inside hot stream loops spikes event-loop lag above 250 ms.
- In **Python (FastAPI / Starlette / ASGI)**, catching broad `Exception` (or omitting `try...finally` inside an `async def` generator when `asyncio.CancelledError` inherits from `BaseException`) prevents upstream `httpx.AsyncClient` streams and database cursors from closing when a client drops mid-stream—exhausting the connection pool within minutes.

This article walks readers through reproducing both bugs with a 30-line slow-consumer simulator, spotting the exact memory staircase and event-loop / GC pressure in **AppSignal**, and fixing them with bounded backpressure queues, `AbortSignal` propagation, and deterministic async generator cleanup.

Best regards,  
**Desu Sreecharan**  
GitHub: https://github.com/sreecharan-desu

---

## 2. Detailed Article Outline

1. **The Anatomy of a Streaming Outage**
   - Why traditional request-response APM percentiles miss streaming backpressure bugs until OOMKiller terminates the container.
   - How slow mobile/proxy clients turn a 2 KB/sec SSE endpoint into a 1.4 GB heap leak across 500 concurrent connections.

2. **Trap #1 (Node.js / TypeScript): Ignoring `ReadableStreamDefaultController.desiredSize` and `'drain'`**
   - Minimal reproduction: A Next.js / Fastify / Express SSE endpoint streaming JSON deltas to a throttled client reading at 1 KB/s.
   - Why `controller.enqueue()` does not `await` consumer reads and how buffered chunks accumulate in V8 Old Space.
   - The Fix: Pull-based `ReadableStream({ pull(controller) })`, respecting `res.write()` return values with `once(res, 'drain')`, and wiring `req.socket.on('close')` to an `AbortController`.

3. **Trap #2 (Python / ASGI): `asyncio.CancelledError` and Leaked Upstream Connections in Async Generators**
   - Why `except Exception:` inside a FastAPI `StreamingResponse` generator does **not** catch `asyncio.CancelledError` in Python 3.8+, and why catching `BaseException` without re-raising breaks ASGI disconnect handling.
   - Using `contextlib.aclosing()` so partially iterated async generators always execute their `finally:` block cleanly.

4. **Trap #3: Synchronous JSON & Regex Work Blocking the Event Loop Mid-Stream**
   - Measuring event-loop utilization (ELU) and event-loop lag when parsing/validating 200 chunks/sec across 100 streams.
   - Offloading or batching token/chunk serialization (`setImmediate` / micro-batching every 16 ms) to cut P99 event-loop lag from 180 ms to <4 ms.

5. **Observing Stream Health in AppSignal**
   - Instrumenting active stream gauges (`active_sse_streams`), backpressure drop counters, and heap/event-loop anomaly alerts in AppSignal.
   - Before-and-after benchmark table (heap usage, P99 event-loop lag, leaked socket count).

---

## 3. Production Code Examples

### 3.1 Node.js / TypeScript: Backpressure-Safe SSE Stream with `AbortController` and `drain` Handling

```typescript
import { once } from "node:events";
import type { IncomingMessage, ServerResponse } from "node:http";
import { Appsignal } from "@appsignal/nodejs";

const MAX_BUFFER_HIGH_WATER_MARK = 64 * 1024; // 64 KiB per client stream

export async function streamEventsWithBackpressure(
  req: IncomingMessage,
  res: ServerResponse,
  source: AsyncGenerator<Record<string, unknown>, void, unknown>
): Promise<void> {
  const meter = Appsignal.client()?.metrics();
  const abortController = new AbortController();

  const onClose = () => abortController.abort(new Error("client_disconnected"));
  req.on("close", onClose);

  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });

  meter?.incrementCounter("sse.streams.opened", 1);

  try {
    for await (const event of source) {
      if (abortController.signal.aborted || res.writableEnded) {
        break;
      }

      const frame = `data: ${JSON.stringify(event)}\n\n`;
      const canContinue = res.write(frame);

      if (!canContinue) {
        meter?.incrementCounter("sse.backpressure.pauses", 1);
        // If the client's kernel send buffer is backed up beyond our safety threshold,
        // wait for 'drain' with a strict timeout so slow loris clients cannot pin heap memory.
        await Promise.race([
          once(res, "drain", { signal: abortController.signal }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("slow_consumer_timeout")), 5_000)
          ),
        ]);
      }
    }
  } catch (err) {
    if (!abortController.signal.aborted) {
      Appsignal.sendError(err instanceof Error ? err : new Error(String(err)));
    }
  } finally {
    req.off("close", onClose);
    // Explicitly return() the upstream async generator so its finally{} block runs immediately
    await source.return(undefined);
    if (!res.writableEnded) {
      res.end();
    }
    meter?.incrementCounter("sse.streams.closed", 1);
  }
}
```

### 3.2 Python (FastAPI / ASGI): Leak-Free Async Generator Streaming with `contextlib.aclosing`

```python
import asyncio
from contextlib import aclosing
from typing import AsyncIterator
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from appsignal import set_category, set_custom_data, send_error

app = FastAPI()
http_client = httpx.AsyncClient(
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    timeout=httpx.Timeout(30.0, connect=5.0),
)


async def upstream_token_stream(prompt: str) -> AsyncIterator[str]:
    """Streams chunks from an upstream service and guarantees socket release on disconnect."""
    async with http_client.stream(
        "POST",
        "https://api.example.com/v1/stream",
        json={"prompt": prompt},
    ) as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if line:
                yield f"data: {line}\n\n"


@app.post("/v1/stream")
async def stream_endpoint(request: Request, payload: dict) -> StreamingResponse:
    async def guarded_generator() -> AsyncIterator[str]:
        set_category("sse.stream")
        chunks_sent = 0
        try:
            # aclosing() guarantees upstream_token_stream.aclose() runs even if the consumer
            # breaks early or Starlette cancels the task on client disconnect.
            async with aclosing(upstream_token_stream(payload["prompt"])) as stream:
                async for frame in stream:
                    if await request.is_disconnected():
                        break
                    chunks_sent += 1
                    yield frame
        except asyncio.CancelledError:
            set_custom_data({"disconnect_reason": "client_cancelled", "chunks_sent": chunks_sent})
            raise  # Always re-raise CancelledError so ASGI task group cancellation completes
        except Exception as exc:
            send_error(exc)
            raise

    return StreamingResponse(guarded_generator(), media_type="text/event-stream")
```
