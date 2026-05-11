---
name: connection-leak-http-grpc
description: Diagnose and fix HTTP and gRPC client connection leaks across Java, Kotlin, and Python. Use this skill whenever the user reports symptoms like socket count to a remote service climbing without bound, sockets stuck in CLOSE_WAIT or ESTABLISHED, Netty `LEAK: ByteBuf.release() was not called`, "ManagedChannel was not shutdown properly" warnings at process exit, gRPC stub creation per-request causing channel exhaustion, OkHttp `ResponseBody` not closed warnings, Apache HttpClient connection-pool exhaustion, aiohttp "Unclosed client session", httpx async client warnings, or `requests.Session` leaks. Covers source-code audit for response/body/channel close patterns and live diagnosis via `lsof`, `/proc/*/net/tcp`, Netty leak detector levels, async-profiler allocation tracing, and `py-spy` for Python.
---

# Connection Leak: HTTP / gRPC

Assumes cross-cutting triage from `connection-leak-hunt` is done — leak is confirmed and FDs are mostly sockets to non-DB ports.

HTTP/gRPC leaks have two distinct shapes:

1. **Per-request leak**: response body, response object, or stream not closed. FDs grow proportional to request count.
2. **Per-client leak**: a new `HttpClient`/`OkHttpClient`/`ManagedChannel`/`ClientSession` constructed per request and not closed. FDs grow proportional to call sites that recreate the client.

Both look the same in `lsof`. Diagnose by checking client-instance counts in a heap dump or by inspecting allocation sites — the fixes are different.

## Source-code audit

### Java — OkHttp

`Response`, `ResponseBody`, and `Call` all implement `Closeable`. `ResponseBody` close releases the connection back to the pool.

```sh
# Response not in try-with-resources
rg -n '\.execute\(\)' --type java | rg -i okhttp | rg -v 'try\s*\('

# Response body read without close (e.g., .string() leaves stream open if not consumed fully on error)
rg -n '\.body\(\)\.(string|bytes|byteStream)\(\)' --type java
```

Idiomatic:

```java
Request req = new Request.Builder().url(url).build();
try (Response r = client.newCall(req).execute()) {
    if (!r.isSuccessful()) throw new IOException("status " + r.code());
    return r.body().string();
}
```

`ResponseBody` close is automatic via the `Response` close. The leak surface is when the `Response` is held without try-with-resources and an exception path skips close.

Async OkHttp:

```java
client.newCall(req).enqueue(new Callback() {
    @Override public void onResponse(Call call, Response r) throws IOException {
        try (r) { /* consume */ }   // MUST close in onResponse, even on success
    }
    @Override public void onFailure(Call call, IOException e) { /* no Response to close */ }
});
```

OkHttp client itself (`OkHttpClient`) is intended to be a **singleton per process**. Constructing one per request leaks the connection pool and dispatcher executor. Grep:

```sh
rg -n 'new OkHttpClient\(\)' --type java
```

Anything inside a method body (vs. a static field or DI singleton) is suspect.

### Java — Apache HttpClient

`CloseableHttpResponse` must be closed. `HttpEntity` content stream must be fully consumed or `EntityUtils.consume` called.

```sh
rg -n '\.execute\(.*\)' --type java | rg -i 'httpclient|httpasyncclient' | rg -v 'try\s*\('
```

Idiomatic (HttpClient 5.x):

```java
try (CloseableHttpResponse r = httpClient.execute(req)) {
    HttpEntity entity = r.getEntity();
    String body = EntityUtils.toString(entity);
    EntityUtils.consume(entity);
    return body;
}
```

HttpClient 4.x is similar; missing `EntityUtils.consume` on the error path is the most common leak.

### Java — Netty / `ResourceLeakDetector`

Netty tracks `ByteBuf` refcounts. A leak prints:

```
LEAK: ByteBuf.release() was not called before it's garbage-collected.
Recent access records:
  ...stack...
```

That stack identifies the consumer that didn't release. By default the detector samples ~1% of allocations; raise the level for diagnosis (see live diagnosis section below).

### Java — gRPC

`ManagedChannel` is the heavy object — the equivalent of `OkHttpClient`. Stubs are cheap; channels are expensive. Anti-pattern:

```sh
rg -n 'ManagedChannelBuilder\.forAddress|NettyChannelBuilder\.forAddress' --type java
```

Each call site should be in a `@Bean`/static init, not in a request handler.

`ManagedChannel.shutdown()` must be called at process exit. In a Flink operator, in `RichFunction.close()`. In Spring, in a `@PreDestroy`.

```java
@PreDestroy
public void shutdown() throws InterruptedException {
    if (!channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
    }
}
```

Streaming RPCs require explicit cancellation on the client side if the client returns early. Server-streaming and bidi streams that are abandoned leak HTTP/2 streams (each consumes a slot in the channel's stream concurrency limit, not a separate FD — but exhaustion shows up as new RPCs blocking).

```java
// abandoning a server-streaming call without cancel — LEAK
Iterator<Response> it = stub.streamThings(req);
if (somePredicate) return;  // it is never drained or cancelled

// FIX
Context.CancellableContext ctx = Context.current().withCancellation();
ctx.run(() -> {
    Iterator<Response> it = stub.streamThings(req);
    try {
        while (it.hasNext()) { ... }
    } finally {
        ctx.cancel(null);
    }
});
```

### Kotlin — ktor

`HttpClient` is `Closeable`. A `HttpResponse` is associated with a coroutine scope; cancellation of that scope releases the connection. Pitfalls:

```sh
rg -n 'HttpClient\(' --type kotlin
```

A `HttpClient` constructed inside a function leaks unless `.use { }` wraps it. Keep it as a singleton.

```kotlin
// LEAK
suspend fun fetch(url: String): String {
    val client = HttpClient(CIO)
    return client.get(url).bodyAsText()
}

// FIX - reuse the client
class FetcherService(private val client: HttpClient) {
    suspend fun fetch(url: String) = client.get(url).bodyAsText()
}
```

`HttpStatement.execute { ... }` block-scoped form is the right pattern for streaming responses — the response is closed when the block exits.

### Python — aiohttp

`ClientSession` must be closed; on async exit warning prints `Unclosed client session`. The loud one is `Unclosed connector`.

```sh
rg -n 'ClientSession\(' --type py
```

Idiomatic:

```python
async with aiohttp.ClientSession() as session:
    async with session.get(url) as resp:
        return await resp.text()
```

A long-lived service should construct one `ClientSession` at startup, store it, and close it at shutdown — not construct per request. Per-request construction is the dominant aiohttp leak in production code.

`Response` objects must also be context-managed; `await resp.text()` without `async with session.get(...)` leaks the underlying socket back into the pool only on GC.

### Python — httpx

```sh
rg -n 'httpx\.AsyncClient\(' --type py
rg -n 'httpx\.Client\(' --type py
```

Same pattern: context manager or explicit close, singleton per service.

```python
# at startup
self._client = httpx.AsyncClient(timeout=5.0, limits=httpx.Limits(max_connections=100))

# at shutdown
await self._client.aclose()
```

### Python — requests

`requests.Session` must be closed (or used as a context manager) — without it, the underlying urllib3 connection pool leaks at process exit.

```sh
# bare requests.get — uses an internal session, leaks at process scale
rg -n 'requests\.(get|post|put|delete|patch)\(' --type py
```

Bare `requests.get(...)` is fine for scripts but a leak in long-lived services. Use a `Session` per service.

### Python — grpc

`grpc.aio.Channel` and `grpc.Channel` must be closed. Stubs share a channel — same pattern as Java.

```sh
rg -n 'grpc\.(aio\.)?(secure_channel|insecure_channel)' --type py
```

Per-request channel construction is the big leak. Combine with the sync API gotcha: `grpc.insecure_channel(...)` returns an object whose `__del__` runs `close()` only on GC — under load, FDs accumulate before GC catches up.

```python
# at startup
self.channel = grpc.aio.insecure_channel(target)
self.stub = MyServiceStub(self.channel)

# at shutdown
await self.channel.close()
```

## Live diagnosis

### Socket inventory

```sh
# total ESTABLISHED count by remote
kubectl exec -it <pod> -- sh -c '
  ss -tn state established |
  awk "NR>1 {print \$5}" |
  awk -F: "{print \$1}" |
  sort | uniq -c | sort -rn | head -20
'
```

Climbing count to a single remote = leak to that service. Climbing across many remotes = client-construction leak.

CLOSE_WAIT specifically:

```sh
kubectl exec -it <pod> -- ss -tn state close-wait | wc -l
```

A growing CLOSE_WAIT count means the **remote** closed the connection but the **local app** never called close — that's almost always a missing `Response`/`ResponseBody`/`Channel` close on the application side.

### Distinguishing per-request vs per-client leak

```sh
# JVM heap dump
kubectl exec -it <pod> -- jcmd 1 GC.heap_dump /tmp/h.hprof
kubectl cp <pod>:/tmp/h.hprof ./h.hprof
```

In MAT, count instances of:

- `okhttp3.OkHttpClient`
- `org.apache.hc.client5.http.impl.classic.CloseableHttpClient`
- `io.grpc.internal.ManagedChannelImpl`
- `io.netty.channel.nio.NioEventLoopGroup`

If the count exceeds the number of intentional clients (usually one per upstream service), it's a per-client leak — find construction sites with the source audit.

If counts are stable but FDs grow, it's a per-request leak — find the missing close.

### Netty `ResourceLeakDetector` — paranoid mode

```sh
# JVM flag
-Dio.netty.leakDetectionLevel=paranoid
```

Paranoid samples 100%. The log lines look like:

```
LEAK: ByteBuf.release() was not called before it's garbage-collected. Enable advanced leak reporting...
Recent access records:
#1: io.netty.handler.codec.http.DefaultHttpContent.<init>(...)
   ...your application stack...
```

Stack identifies the consumer. Disable paranoid after diagnosis — overhead is meaningful.

### async-profiler — allocation site for sockets

```sh
kubectl exec -it <pod> -- /tmp/async-profiler-3.0/bin/asprof \
  -e java.net.Socket.<init>,sun.nio.ch.SocketChannelImpl.<init> \
  -d 60 -f /tmp/sockets.html 1
kubectl cp <pod>:/tmp/sockets.html ./sockets.html
```

The dominant stack is the call site allocating sockets faster than they're closing.

For Netty channels:

```sh
asprof -e io.netty.channel.AbstractChannel.<init> -d 60 -f /tmp/netty.html 1
```

### gRPC channel state

```java
// add to a debug endpoint
public Map<String, Object> channelDebug() {
    return Map.of(
        "state", channel.getState(false).name(),
        "authority", channel.authority()
    );
}
```

A channel in `TRANSIENT_FAILURE` for sustained periods while still being used means each RPC creates a fresh subchannel; old ones may not be reclaimed promptly.

### Python — `lsof` and `py-spy`

```sh
kubectl exec -it <pod> -- sh -c 'ls -l /proc/1/fd | grep socket | wc -l'
kubectl exec -it <pod> -- py-spy dump --pid 1
```

`py-spy dump` shows every coroutine's stack. Look for many coroutines parked inside `aiohttp` request methods — that means requests are in-flight and not completing (often a missing timeout, which then masquerades as a leak as the client waits forever).

For aiohttp specifically:

```python
# instrument session creation to print stack
import traceback
_orig_init = aiohttp.ClientSession.__init__
def _logged_init(self, *a, **kw):
    print("ClientSession created at:")
    traceback.print_stack()
    return _orig_init(self, *a, **kw)
aiohttp.ClientSession.__init__ = _logged_init
```

Run for a minute, see how many session-create stacks print. More than your service's known startup count == per-request construction.

For asyncio task accounting:

```python
import asyncio
print(len(asyncio.all_tasks()))  # if growing, tasks are leaking — possibly each holding a connection
```

## Fix patterns

### Singleton clients

| Library | Singleton scope |
|---|---|
| `OkHttpClient` | per process, optionally per-upstream with `.newBuilder()` |
| Apache `HttpClient` | per process |
| `ManagedChannel` | per upstream service (one channel, many stubs) |
| ktor `HttpClient` | per process |
| `aiohttp.ClientSession` | per process or per long-lived task |
| `httpx.AsyncClient` | per process |
| `requests.Session` | per process or per worker |
| Python `grpc.aio.Channel` | per upstream service |

### Always-close response patterns

```java
// OkHttp
try (Response r = client.newCall(req).execute()) { ... }

// Apache HttpClient 5
try (CloseableHttpResponse r = client.execute(req)) {
    EntityUtils.consume(r.getEntity());
}

// gRPC client streaming abandonment
Context.CancellableContext ctx = Context.current().withCancellation();
ctx.run(() -> { ... });  // ctx.cancel(null) in finally
```

```kotlin
// ktor
client.prepareGet(url).execute { resp ->
    resp.bodyAsChannel().copyTo(out)
}
```

```python
# aiohttp
async with session.get(url) as resp:
    return await resp.text()

# httpx
async with httpx.AsyncClient() as client:
    resp = await client.get(url)
    return resp.text
```

### Shutdown hooks

Every long-lived client needs a teardown path tied to the process lifecycle:

| Runtime | Hook |
|---|---|
| Spring | `@PreDestroy` |
| Quarkus | `@Observes ShutdownEvent` |
| plain JVM | `Runtime.getRuntime().addShutdownHook(...)` |
| Flink operator | `RichFunction.close()` (see flink sub-skill) |
| FastAPI | `lifespan` async context manager |
| asyncio main | finally block on outermost task |

### Verification

After the fix:

1. Drive the suspected leak path under load for several minutes.
2. Sample `ss -tn state established | wc -l` and `lsof | wc -l` over time.
3. The counts should oscillate around steady state, not climb monotonically.

For per-client leaks, additionally heap-dump and verify singleton counts match expected.
