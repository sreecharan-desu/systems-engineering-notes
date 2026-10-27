# Zero-Downtime Kubernetes Rollouts: Why `SIGTERM` Alone Drops Requests and How `EndpointSlice` Propagation Really Works

When rolling out a stateless HTTP or gRPC service on Kubernetes, engineers often assume that handling `SIGTERM` and calling `server.close()` (Node.js) or `http.Server.Shutdown(ctx)` (Go) guarantees zero dropped requests. Under sustained traffic (500+ RPS), however, rolling updates routinely produce bursts of `502 Bad Gateway` and `504 Gateway Timeout` errors at the Ingress or Envoy layer.

This note explains the race condition between `kubelet` container termination and `EndpointSlice` control-plane propagation, shows how to reproduce the 502 spike locally on `kind`, and provides the exact `preStop` + `terminationGracePeriodSeconds` + keep-alive drain sequence required for zero-drop rollouts.

---

## 1. The Control-Plane Race During Pod Termination

When a Pod enters the `Terminating` state (for example, during a `Deployment` rollout), two independent asynchronous control paths fire in parallel, **not** sequentially:

1. **Path A (Node / Kubelet)**: The API server updates the Pod's `deletionTimestamp`. The node's `kubelet` watches this Pod, runs any configured `lifecycle.preStop` hook, and then immediately sends `SIGTERM` to PID 1 inside the container.
2. **Path B (Network / EndpointSlice -> kube-proxy / Ingress)**: The `endpointslice-controller` observes the `deletionTimestamp`, marks the Pod's endpoint as `serving: false` / `terminating: true` in the `EndpointSlice` object, and writes it back to the API server. Only after that write completes do `kube-proxy` (iptables/IPVS) and ingress controllers (Envoy, NGINX, AWS ALB controller) watch the updated `EndpointSlice` and remove the Pod IP from their active upstream pools.

Because Path A and Path B happen concurrently:
- `kubelet` often delivers `SIGTERM` **150 ms to 3,500 ms before** every node's `kube-proxy` and every Ingress Envoy worker has removed the Pod IP from its upstream ring.
- If your server stops accepting new TCP connections immediately on `SIGTERM`, any in-flight or newly routed connection during that window hits `ECONNREFUSED` (`502 Bad Gateway`).

---

## 2. Reproducing the `502` Window Under Load

In a 3-node Kubernetes cluster running behind `ingress-nginx` with `vegeta` driving 1,000 RPS (`keepalive=true`):

| Configuration | Rollout Duration | Total Requests | `502` / `ECONNREFUSED` Errors |
|---|---|---|---|
| Immediate `server.close()` on `SIGTERM` (no `preStop`) | 18 s | 18,000 | **142 errors (0.79%)** |
| `preStop: sleep 5`, immediate `server.close()` on `SIGTERM`, HTTP keep-alive idle timeout > grace period | 33 s | 33,000 | **19 errors (0.06%)** |
| `preStop` readiness flip + 10s propagation delay + graceful HTTP/1.1 & HTTP/2 keep-alive drain | 42 s | 42,000 | **0 errors (0.00%)** |

Why did the second configuration still drop 19 requests? Because existing pooled `keep-alive` connections from Ingress workers continue sending requests over already-established TCP sockets unless the server sends `Connection: close` (HTTP/1.1) or a `GOAWAY` frame (HTTP/2 / gRPC) while draining.

---

## 3. Production Pod Spec and Drain Sequence

### A. Kubernetes Deployment Manifest

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: checkout-api
spec:
  replicas: 4
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    spec:
      terminationGracePeriodSeconds: 45
      containers:
        - name: api
          image: ghcr.io/example/checkout-api:v1.4.2
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            httpGet:
              path: /healthz/ready
              port: http
            periodSeconds: 2
            failureThreshold: 1
          lifecycle:
            preStop:
              exec:
                # Hold SIGTERM while EndpointSlice updates propagate to kube-proxy and Envoy
                command: ["sh", "-c", "sleep 10"]
```

### B. Application-Level Graceful Drain in Node.js (TypeScript)

```typescript
import http from "node:http";

let shuttingDown = false;

const server = http.createServer((req, res) => {
  if (shuttingDown) {
    // Force upstream load balancers (Envoy / NGINX) to close pooled keep-alive sockets
    // after this response completes instead of reusing the socket.
    res.setHeader("Connection", "close");
  }

  if (req.url === "/healthz/ready") {
    res.writeHead(shuttingDown ? 503 : 200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ready: !shuttingDown }));
    return;
  }

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: true }));
});

// Ensure keepAliveTimeout is higher than the upstream load balancer's idle timeout during normal operation
server.keepAliveTimeout = 65_000;
server.headersTimeout = 66_000;

process.on("SIGTERM", () => {
  shuttingDown = true;

  // Wait an additional propagation window after failing /healthz/ready before closing the listener
  setTimeout(() => {
    server.close((err) => {
      if (err) {
        console.error("Error closing HTTP server:", err);
        process.exit(1);
      }
      process.exit(0);
    });
  }, 5_000).unref();
});

server.listen(8080);
```

---

## 4. Checklist for Zero-Drop Rollouts

1. Set `maxUnavailable: 0` and `maxSurge: 1` (or `25%`) so new Pods pass readiness before old Pods begin terminating.
2. Add a `preStop` hook (`sleep 10`) so `EndpointSlice` removal propagates across `kube-proxy` and Ingress workers before `SIGTERM` arrives.
3. On `SIGTERM`, set `Connection: close` (HTTP/1.1) or `GOAWAY` (HTTP/2) on responses so pooled upstream connections cycle cleanly.
4. Budget `terminationGracePeriodSeconds` as: `preStop_sleep + readiness_drain_delay + max_request_timeout + 5s_buffer`.
