---
name: connection-leak-flink
description: Diagnose and fix connection and resource leaks in Apache Flink 1.18 jobs running on the Flink Kubernetes Operator. Use this skill whenever the user reports symptoms like climbing TaskManager FD count, "Too many open files" on TMs, slow `cancel` or `stop`, "Task did not exit gracefully", checkpoint duration steadily increasing, RocksDB iterator counts climbing, leaked Kafka producers/consumers across job restarts, AsyncIO function clients not shut down, or JDBC sinks holding connections after operator close. Covers `RichFunction` lifecycle audit (`open`/`close` symmetry), AsyncIO client lifecycle, Kafka and JDBC sink connectors, RocksDB iterator hygiene, and live diagnosis via TaskManager metrics, in-pod `jstack`, async-profiler, and `/proc` inspection.
---

# Connection Leak: Flink Connectors / `RichFunction` Lifecycle

Assumes cross-cutting triage from `connection-leak-hunt` is done — leak is confirmed on a TaskManager (TM), not the JobManager.

**Runtime assumed:** Apache Flink 1.18 on Java 17. Code samples use Java 17 idioms (e.g. JEP 394 pattern-matching `instanceof`). Flink 1.18 also supports Java 8/11; on those, rewrite pattern variables to classic `instanceof` + cast.

Flink leaks are almost always lifecycle bugs in operator code: a resource opened in `open()` (or lazily) and not released in `close()`. The Flink runtime calls `close()` on graceful shutdown but **does not** call it on `cancel`, on TM kill, or on uncaught exceptions during checkpoint — so leaked resources from prior job attempts can pile up across restarts on the same TM JVM if the TM is reused.

## Source-code audit

### Mental model: chaining, slot sharing, TM JVM reuse

Three runtime shape decisions change who owns a resource and when it gets released. Get these right before reading any `open`/`close` code.

**Operator chaining.** Operators in the same `OperatorChain` run on a single task thread, and the chain calls `close()` on each operator **in reverse order**. If operator A's `close()` throws, operators B and C in the chain are still closed, but the exception propagates and the surrounding `StreamTask.cleanup()` may skip downstream cleanup. The suppressed-exception chain pattern (see below) matters more in chained operators because one bad `close()` poisons the whole chain's release path. Disable chaining around high-risk operators with `.startNewChain()` or `.disableChaining()` if needed.

**Slot sharing.** By default all operators in a job share one `SlotSharingGroup`, so they land in the same TM JVM slot — sharing the JVM, the classloader, and any static state. A leaked thread pool in operator X pins the slot for operators Y and Z too. Fence high-risk operators (custom AsyncIO clients, custom JDBC sinks) into their own group:

```java
stream
    .map(safeOp)
    .keyBy(...)
    .process(suspectAsyncOp)
        .slotSharingGroup("isolated-async");  // suspect operator gets its own slot
```

**TM JVM reuse on the Flink Kubernetes Operator.** Pod recycling policy on the operator controls whether a TM JVM survives a job restart. If `taskmanager.process.size` and the operator's `flinkVersion` settings keep TMs alive across restarts (the default for reactive mode and standalone deployments), **resources leaked from prior job attempts pile up in the same JVM**. Every leak audit on the K8s operator must distinguish "leak during run" from "leak across restarts" — they have the same FD-graph shape but different fix paths. Force a TM restart between job runs to isolate:

```sh
# in-pod: trigger a restart between job attempts when reproducing
kubectl delete pod <tm-pod> --grace-period=30
```

### `RichFunction.open` / `close` symmetry

Audit every subclass of `RichFunction`, `RichMapFunction`, `RichFlatMapFunction`, `KeyedProcessFunction`, `ProcessFunction`, `RichSinkFunction`, `RichAsyncFunction`, etc.

```sh
# List every Flink user function
rg -l 'extends Rich(Map|FlatMap|Filter|Sink|Source|Async)?Function|extends KeyedProcessFunction|extends ProcessFunction' --type java --type kotlin

# Functions that override open() but not close()
rg -l 'override fun open|public void open' --type java --type kotlin > /tmp/has_open.txt
rg -l 'override fun close|public void close' --type java --type kotlin > /tmp/has_close.txt
comm -23 <(sort /tmp/has_open.txt) <(sort /tmp/has_close.txt)
```

Any file that opens but does not close is a strong candidate.

For files with both, verify `close()` actually releases everything `open()` allocated. The pattern that bites:

```java
// LEAK - second resource leaks if first close() throws
@Override
public void close() throws Exception {
    httpClient.close();   // throws
    kafkaProducer.close();  // never runs
    super.close();
}

// CORRECT - chain via suppressed exceptions
@Override
public void close() throws Exception {
    Throwable first = null;
    try { httpClient.close(); } catch (Throwable t) { first = t; }
    try { kafkaProducer.close(); } catch (Throwable t) {
        if (first == null) first = t; else first.addSuppressed(t);
    }
    try { super.close(); } catch (Throwable t) {
        if (first == null) first = t; else first.addSuppressed(t);
    }
    if (first != null) throw new Exception(first);
}
```

For Kotlin, `runCatching` chained with `getOrElse` works equivalently, but the explicit pattern is clearer in operator code.

### Idempotency of `close()`

Flink can call `close()` multiple times in some failure paths. Make it safe:

```java
private volatile boolean closed = false;

@Override
public void close() throws Exception {
    if (closed) return;
    closed = true;
    // release...
}
```

Without this, the second call hits a half-disposed object and throws — which the caller treats as the resource failing to close, not as already-closed.

### Lazy initialization gotcha

Resources lazily initialized inside `processElement` and not in `open()` are easy to forget about in `close()`:

```sh
# fields assigned inside processElement are suspicious
rg -n 'this\.\w+\s*=' --type java -g '*RichFunction*' -g '*ProcessFunction*' | rg -v '@Override.*open'
```

If the field is assigned outside `open()`, search for it in `close()`.

### `RichAsyncFunction` and AsyncIO

`RichAsyncFunction` clients leak more often than synchronous ones because:

- The async client's lifecycle outlives a single `asyncInvoke` call by design.
- Failures from the async callback don't propagate through `close()` — exceptions in `ResultFuture.completeExceptionally` happen on a different thread.
- `AsyncDataStream.unorderedWait(... timeout, ...)` timeouts can leave in-flight requests dangling; their underlying connections may not return to the pool.

Audit:

- Client initialized in `open()`, closed in `close()`.
- Timeout handler explicitly cancels/closes the in-flight resource. Override `timeout()`:

```java
@Override
public void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
    // cancel the underlying request — without this, the client may hold the socket
    // until its own timeout, far beyond the Flink timeout
    CompletableFuture<?> pending = pendingRequests.get(input);
    if (pending != null) pending.cancel(true);
    resultFuture.complete(Collections.emptyList());
}
```

### Kafka producer / consumer lifecycle

Old `FlinkKafkaProducer` / `FlinkKafkaConsumer` are deprecated; `KafkaSink` and `KafkaSource` (1.14+) handle lifecycle for you. If the codebase still uses the legacy ones:

- Custom serializers/partitioners must not capture extra producers.
- `FlinkKafkaProducer` in EXACTLY_ONCE mode opens a transactional producer per checkpoint; aborted checkpoints can leave orphan producer IDs visible to the Kafka cluster (not the TM, but worth flagging).

For new connectors:

- Custom `KafkaRecordSerializationSchema` that wraps a `Producer` (extra producer outside the connector) is a common leak source — that producer has no managed lifecycle.

### JDBC sink (`JdbcSink`)

Built-in `JdbcSink.sink(...)` manages the connection. Custom `RichSinkFunction` writing to JDBC commonly leaks because:

- `Connection` opened in `open()`, used per-record, closed in `close()` — fine.
- `Connection` opened **per record** to handle reconnect — leaks if not closed on every error path.
- Connection pinned across job restarts because `close()` was missed during cancel.

Prefer `JdbcSink.exactlyOnceSink(...)` or wrap your own carefully:

```java
@Override
public void invoke(Row value, Context ctx) throws Exception {
    if (connection == null || connection.isClosed()) {
        connection = ds.getConnection();
        statement = connection.prepareStatement(SQL);
    }
    bind(statement, value);
    statement.executeUpdate();
}
```

The `executeUpdate` failure path must not leave `connection` non-null and closed — that creates a half-state where subsequent records think the connection exists. Set both to null in the catch.

### `AsyncSinkBase` and SinkV2 async sinks

Flink 1.18 SinkV2 includes `AsyncSinkBase` (used by Kinesis, Firehose, DynamoDB, OpenSearch SinkV2). Lifecycle is different from `RichSinkFunction`:

- `submitRequestEntries()` is called when a batch is ready — your code must release the HTTP/SDK client returned from the in-flight call when the batch resolves.
- On checkpoint, `prepareCommit()` blocks until in-flight requests resolve. A request that never resolves (no timeout on the underlying SDK client) blocks the checkpoint indefinitely — that looks like "checkpoint duration steadily increasing" and is the easiest leak to misattribute.
- On `close()`, in-flight batches are **not** drained by default. Records buffered but never submitted are dropped. If the SDK client doesn't `close()` either, the underlying HTTP connection pool leaks.

```sh
# find AsyncSinkBase subclasses that don't override close
rg -l 'extends AsyncSinkWriter|extends AsyncSinkBase' --type java
rg -L 'override fun close|public void close' <files-from-above>
```

Wrap the SDK client in your `AsyncSinkWriter.close()`:

```java
@Override
public void close() {
    // drain in-flight batches before closing the client
    flush(true);   // forces immediate flush, blocking
    sdkClient.close();
    super.close();
}
```

`flush(true)` is the SinkV2 affordance for draining; skipping it on `close()` is the common bug.

### Connector-specific callouts

Beyond the built-ins, these third-party connectors have repeatable leak modes:

**Elasticsearch / OpenSearch sink.** `RestHighLevelClient` and `RestClient` wrap an Apache `HttpAsyncClient`. The connector closes the high-level client in `close()`, but custom `RestClientBuilder.HttpClientConfigCallback` callbacks that inject extra HTTP filters often hold references to the underlying I/O reactor — those don't get closed.

```sh
rg -n 'RestClientBuilder|HttpClientConfigCallback' --type java
```

If the callback wraps `httpClientBuilder` and stores the result somewhere, the I/O reactor leaks at job cancel.

**Hive sink (`flink-connector-hive`).** `HiveMetastoreClient` opens a Thrift socket per task. The connector closes it in `close()`, but the underlying Thrift pool (`HiveMetaStoreClient.reconnect()`) can re-open silently after a Hive metastore restart. Pin the leak with:

```sh
kubectl exec -it <tm-pod> -- ss -tn | grep ':9083'   # Hive metastore default
```

Counts that climb during a Hive metastore flap and don't recover after the job stabilizes = reconnect-without-close.

**Redis sink (Bahir / community).** Jedis pool leaks via per-record `jedis.close()` returning the connection to the pool — fine — but Jedis-cluster mode (`JedisCluster`) closes via `cluster.close()` which must run in operator `close()`, not on the pool. Lettuce-based sinks need `client.shutdown(Duration, Duration, TimeUnit)` with positive quiet and timeout values; calling `shutdownAsync().get()` without bounded timeout hangs forever on cancel.

```sh
rg -n 'JedisCluster|RedisClient|LettuceClient' --type java -A 3
```

### RocksDB iterators

Not network connections, but they consume FDs. RocksDB iterators are returned by Flink state APIs and **must be closed**.

```sh
rg -n '\.iterator\(\)' --type java --type kotlin -g '*ProcessFunction*' -g '*Operator*'
```

For state-backed iterators (`MapState.iterator()`, queryable state iterators), wrap in try-with-resources or `.use {}`. A long-lived iterator that survives a checkpoint has its underlying RocksDB snapshot pinned, blocking compaction.

### PyFlink note

PyFlink user functions follow the same lifecycle (`open`/`close` on `MapFunction`, etc.). Audit Python code with the same `open` without `close` grep:

```sh
rg -n 'def open\(self' --type py -g '*flink*' -A 30 | rg -B 30 'def close\(self' | rg 'def open\(self' | wc -l
```

## Live diagnosis

### TaskManager metrics

If Prometheus is wired up, watch these per-TM:

| Metric | Leak signature |
|---|---|
| `flink_taskmanager_Status_JVM_Threads_Count` | rises across restarts |
| `flink_taskmanager_Status_JVM_Memory_Direct_Count` | rises with sockets (Netty) |
| `taskmanager_job_task_operator_currentOutputWatermark` | freezes mid-run |
| `flink_taskmanager_job_lastCheckpointDuration` | climbs steadily |
| `numRecordsOutPerSecond` | drops while inputs steady |

Climbing `Threads_Count` across job restarts on the same TM JVM = thread pool inside an operator never being shut down.

### In-pod `jstack`

```sh
kubectl exec -it <tm-pod> -- jstack 1 > /tmp/tm.stack
```

Look for:

```sh
# operator threads still alive after job cancel
grep -E 'AsyncWaitOperator|StreamSink|Kafka.*Producer|HttpClient.*Pool' /tmp/tm.stack -A 5

# connection-pool threads — if a job is cancelled but these remain, the operator never closed its client
grep -E 'OkHttp ConnectionPool|HikariCP housekeeper|reactor-http-' /tmp/tm.stack
```

After running `cancel`, wait 30s, jstack again. Threads named after a connector or HTTP client that persist across the cancel are leaked from prior attempts.

### `/proc/1/fd` correlation with restart count

```sh
# track FD count alongside restart count over time
kubectl exec -it <tm-pod> -- sh -c 'while true; do
  echo "$(date +%s) fds=$(ls /proc/1/fd | wc -l)"
  sleep 60
done'
```

If FD count steps up with each job restart (visible from JobManager logs or `flink list`), the leak is in the cleanup path, not the steady-state path.

### async-profiler for allocation-site identification

To find what's allocating sockets:

```sh
kubectl exec -it <tm-pod> -- /tmp/async-profiler-3.0/bin/asprof \
  -e java.net.Socket.<init> -d 60 -f /tmp/sockets.html 1
kubectl cp <tm-pod>:/tmp/sockets.html ./sockets.html
```

Open the flame graph; the dominant stack is your culprit. For Netty leaks, profile `io.netty.channel.AbstractChannel.<init>` instead.

### Netty `ResourceLeakDetector`

Flink uses Netty internally and most HTTP clients in Flink jobs do too. Crank the detector to PARANOID temporarily:

```yaml
# flink-conf.yaml or env
env.java.opts.taskmanager: "-Dio.netty.leakDetectionLevel=paranoid"
```

PARANOID samples 100% of allocations and prints stack traces of leaked buffers. Remove after diagnosis — the overhead is significant.

### Heap dump path

```sh
kubectl exec -it <tm-pod> -- jcmd 1 GC.heap_dump /tmp/tm.hprof
kubectl cp <tm-pod>:/tmp/tm.hprof ./tm.hprof
```

In MAT, run the leak suspects report. Then:

- For HTTP/gRPC: search `OkHttpClient`, `HttpAsyncClient`, `ManagedChannelImpl`, `EventLoopGroup` instance counts. Counts greater than the number of currently running operator instances == leaked clients.
- For RocksDB iterators: search `RocksIterator`. If counts climb across checkpoints, you have an iterator leak — find the `MapState.iterator()` call site without a close.

## Fix patterns

### Lifecycle template

```java
public class MyEnrichmentFn extends RichAsyncFunction<In, Out> {
    private transient HttpClient client;
    private transient ConcurrentMap<In, CompletableFuture<?>> inFlight;
    private volatile boolean closed = false;

    @Override
    public void open(Configuration parameters) {
        client = HttpClient.newBuilder()
            .executor(Executors.newFixedThreadPool(8))
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        inFlight = new ConcurrentHashMap<>();
    }

    @Override
    public void asyncInvoke(In in, ResultFuture<Out> rf) {
        CompletableFuture<Out> f = doRequest(in);
        inFlight.put(in, f);
        f.whenComplete((r, t) -> {
            inFlight.remove(in);
            if (t != null) rf.completeExceptionally(t);
            else rf.complete(List.of(r));
        });
    }

    @Override
    public void timeout(In in, ResultFuture<Out> rf) {
        CompletableFuture<?> f = inFlight.remove(in);
        if (f != null) f.cancel(true);
        rf.complete(List.of());
    }

    @Override
    public void close() throws Exception {
        if (closed) return;
        closed = true;
        try {
            inFlight.values().forEach(f -> f.cancel(true));
        } finally {
            // java.net.http.HttpClient gained close() only in JDK 21;
            // on Java 17 you must shut down its executor explicitly.
            if (client.executor().orElse(null) instanceof ExecutorService es) {
                es.shutdownNow();
                es.awaitTermination(5, TimeUnit.SECONDS);
            }
            super.close();
        }
    }
}
```

### MiniCluster reproduction harness

Reproduce the leak locally before deploying. `MiniClusterWithClientResource` is the JUnit-bound test harness; it runs a real Flink runtime in-process with configurable parallelism.

```java
public class OperatorLeakReproTest {

    @RegisterExtension
    static final MiniClusterExtension MINI = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());

    private static long openFdCount() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean unix) {
            return unix.getOpenFileDescriptorCount();
        }
        throw new IllegalStateException("Not a Unix JVM — run this test on Linux/macOS");
    }

    @Test
    void operatorReleasesFdsAfterCancel() throws Exception {
        long baseline = openFdCount();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromSequence(1, 10_000)
            .keyBy(x -> x % 10)
            .process(new SuspectFunction())   // the operator under test
            .sinkTo(new DiscardingSink<>());

        JobClient client = env.executeAsync("leak-repro");

        // let it run long enough to open whatever resources it needs
        Thread.sleep(5_000);

        client.cancel().get(30, TimeUnit.SECONDS);

        // GC + housekeeping window — close() runs on cancel
        System.gc();
        Thread.sleep(2_000);

        long afterCancel = openFdCount();
        assertThat(afterCancel - baseline).as("FD growth after cancel").isLessThan(20);
    }
}
```

A few notes that matter:

- The "20" threshold isn't 0 — JVM warmup and the JUnit lifecycle open background FDs. Calibrate by running the test against a known-clean operator first; whatever delta that produces is your noise floor.
- `assertThat(... < threshold)` is the assertion the fix has to satisfy. A failing version of this test is your regression guard.
- For RocksDB iterator leaks, `getOpenFileDescriptorCount()` doesn't always catch them (RocksDB SST files are mmap'd, not necessarily counted as FDs). Use a heap-instance count instead: `assertThat(countLiveInstances(RocksIterator.class)).isZero()`.
- This harness reproduces **operator-level** leaks. TM JVM reuse leaks (resources surviving across restarts) need a different fixture — either run multiple jobs sequentially in the same `MiniCluster`, or do the test in-pod with `kubectl delete pod` between attempts.

### Verification

After the fix:

1. Restart the job 5 times in rapid succession on the same TM pod.
2. Sample `/proc/1/fd | wc -l` between each restart.
3. The FD count should return to baseline after each cancel — not step up.

For a local-only check, run the MiniCluster harness above 50 times in a loop and assert the FD delta stays bounded.

## Prevent

Standing alerts for Flink-specific leak signatures. Requires Flink's Prometheus reporter enabled (`metrics.reporters: prom`).

```yaml
# JVM threads climbing inside a TM across the steady state — operator never
# shutting down its executor
- alert: FlinkTMThreadsClimbing
  expr: |
    deriv(flink_taskmanager_Status_JVM_Threads_Count[15m]) > 0.1
  for: 30m
  annotations:
    summary: "{{ $labels.tm_id }} thread count climbing — operator leak suspected"
    runbook: "connection-leak-flink: jstack + grep thread names"

# Checkpoint duration trending up — usually a writer/buffer leak or an AsyncSink
# stuck waiting for a request that never resolves
- alert: FlinkCheckpointDurationClimbing
  expr: |
    avg_over_time(flink_jobmanager_job_lastCheckpointDuration[15m])
    > 2 * avg_over_time(flink_jobmanager_job_lastCheckpointDuration[6h] offset 6h)
  for: 30m

# Slow cancel — symptom of close() blocking on a resource that won't release
- alert: FlinkJobSlowCancel
  expr: |
    flink_jobmanager_job_uptime{job_state="CANCELLING"} > 120
  annotations:
    summary: "Job stuck in CANCELLING > 2m — close() is blocking somewhere"
```

`deriv` (not `rate`) on a monotonic gauge so the alert recovers when a real restart resets the counter.

## Related

- [`connection-leak-hunt`](../connection-leak-hunt/SKILL.md) — start here for cross-cutting triage (FD classification, leak-rate budget).
- [`connection-leak-jdbc`](../connection-leak-jdbc/SKILL.md) — open when the leaking operator is a JDBC sink or a `RichFunction` that holds a DB pool.
- [`connection-leak-http-grpc`](../connection-leak-http-grpc/SKILL.md) — open when an AsyncIO operator, custom HTTP sink, or Elasticsearch/Kafka REST connector is holding sockets.
