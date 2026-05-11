---
name: connection-leak-jdbc
description: Diagnose and fix JDBC and database connection pool leaks across Java, Kotlin, and Python. Use this skill whenever the user reports HikariCP `getConnection` timeouts, "Connection is not available, request timed out", `pg_stat_activity` showing idle-in-transaction climbing, MySQL `SHOW PROCESSLIST` filling with sleeping connections, SQLAlchemy "QueuePool limit overflow", asyncpg pool exhaustion, or any DB pool that drains under steady load. Covers source-code audit for missing close patterns and live-process diagnosis via HikariCP MBeans, pool metrics, server-side session inspection, and heap dump analysis.
---

# Connection Leak: JDBC / DB Pools

Assumes cross-cutting triage from `connection-leak-hunt` is done — leak is confirmed, FDs are mostly DB sockets.

Two paths run in parallel: **source-code audit** finds the bug; **live-process diagnosis** confirms the bug is the actual offender.

## Source-code audit

### Java — try-with-resources

Every `Connection`, `Statement`, `PreparedStatement`, `ResultSet` must be inside try-with-resources, or close() must run in a finally that handles exceptions in close itself.

Grep for the anti-patterns:

```sh
# Connection obtained but not in a try-with-resources head
rg -n 'getConnection\(\)' --type java | rg -v 'try\s*\('

# Statement created without try-with-resources
rg -n '\.(prepareStatement|createStatement)\(' --type java -A 1 | rg -B 1 -v 'try\s*\('

# ResultSet not closed (rs not in a try head)
rg -n '\.executeQuery\(' --type java | rg -v 'try\s*\('
```

Also flag:

- `Connection` stored as a field — almost always wrong unless it's a pooled `DataSource` reference.
- Method returns `Connection` to caller — close ownership becomes ambiguous and gets dropped on exception paths.
- `try { ... } finally { conn.close(); }` without nested try around the close itself — close can throw and mask the real exception, but more importantly any prior `Statement`/`ResultSet` close is skipped.

Idiomatic:

```java
try (Connection c = ds.getConnection();
     PreparedStatement ps = c.prepareStatement(sql)) {
    ps.setLong(1, id);
    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) { ... }
    }
}
```

### Kotlin — `.use { }`

`Closeable.use` is the equivalent of try-with-resources. Grep for connections obtained without it:

```sh
rg -n '\.connection\b|getConnection\(\)' --type kotlin | rg -v '\.use\s*\{'
rg -n 'prepareStatement|createStatement' --type kotlin | rg -v '\.use\s*\{'
```

Common Kotlin-specific bug: `let` instead of `use` — `let` does not close.

```kotlin
// LEAK
ds.connection.let { conn ->
    conn.prepareStatement(sql).executeQuery().let { rs ->
        while (rs.next()) { ... }
    }
}

// CORRECT
ds.connection.use { conn ->
    conn.prepareStatement(sql).use { ps ->
        ps.executeQuery().use { rs ->
            while (rs.next()) { ... }
        }
    }
}
```

Coroutines + JDBC: JDBC is blocking, so any suspending function that does JDBC work must dispatch to `Dispatchers.IO` and the connection must not escape the coroutine scope. A connection captured by a `launch` that outlives the parent scope is a leak waiting to happen — confirm structured concurrency.

### Python — context managers

`psycopg2` / `psycopg3`:

```sh
rg -n 'psycopg2?\.connect|psycopg\.connect' | rg -v 'with\s'
rg -n '\.cursor\(\)' --type py | rg -v 'with\s'
```

`asyncpg`:

```sh
# acquire without release / context manager
rg -n 'pool\.acquire\(\)' --type py | rg -v 'async with'
```

`SQLAlchemy`:

- Sessions must be closed. `Session()` constructed manually but never closed leaks the underlying connection back to the pool only on GC.
- `engine.connect()` outside a `with` block leaks.
- Watch for sessions stored on long-lived objects (request-scoped sessions stored on a singleton service).

```python
# LEAK
session = Session()
result = session.query(User).all()
return result   # session never closed; connection returns to pool only on GC

# CORRECT
with Session() as session:
    return session.query(User).all()

# OR for FastAPI-style dependency injection
def get_session():
    with Session() as s:
        yield s
```

`asyncpg` correct usage:

```python
async with pool.acquire() as conn:
    await conn.fetch("SELECT ...")
# connection released on exit, including exception paths
```

### Java/Kotlin — R2DBC and reactive pools

`r2dbc-pool` (`io.r2dbc.pool.ConnectionPool`) is the reactive analogue of HikariCP. The leak shapes are different because release is driven by **terminal signals** (`onComplete` / `onError` / `cancel`), not by `close()`. A `Mono<Connection>` that is never subscribed leaks no connection — but a `Connection` obtained inside a chain that never reaches a terminal signal pins the connection until the pool's `maxAcquireTime` (default 0 = forever).

```sh
# acquire without usingWhen/flatMap chain that terminates
rg -n '\.create\(\)\.flatMap' --type java --type kotlin -A 5 | rg -B 5 -v 'usingWhen|\.close\(\)|doFinally'
```

Leak patterns:

```java
// LEAK — early return drops the Mono, connection released only on GC of the Publisher
public Mono<User> findUser(long id) {
    return connectionFactory.create().flatMap(conn -> {
        if (id < 0) return Mono.empty();   // conn is leaked — no terminal signal reaches its release
        return Mono.from(conn.createStatement("SELECT ...")
            .bind("$1", id).execute())
            .flatMap(r -> Mono.from(r.map(this::toUser)));
        // missing: doFinally(s -> conn.close())
    });
}

// CORRECT — Mono.usingWhen guarantees release on every terminal path
public Mono<User> findUser(long id) {
    return Mono.usingWhen(
        connectionFactory.create(),
        conn -> Mono.from(conn.createStatement("SELECT ... WHERE id = $1")
            .bind("$1", id).execute())
            .flatMap(r -> Mono.from(r.map(this::toUser))),
        Connection::close);
}
```

With Spring Data R2DBC, the framework's `R2dbcEntityTemplate` handles release. Hand-written `ConnectionFactory.create()` chains are where the leaks live.

Pool inspection:

```java
ConnectionPool pool = (ConnectionPool) connectionFactory;
PoolMetrics m = pool.getMetrics().orElseThrow();
log.info("acquired={} allocated={} idle={} pending={} maxAllocated={}",
    m.acquiredSize(), m.allocatedSize(), m.idleSize(),
    m.pendingAcquireSize(), m.getMaxAllocatedSize());
```

`acquiredSize` climbing while `idleSize` drains and `pendingAcquireSize > 0` for sustained periods = same leak signature as HikariCP, different API.

For `reactor.netty.PoolAcquireTimeoutException` from R2DBC (rare — usually surfaces with a different message), it means a borrower waited longer than `maxAcquireTime` for a free connection: the pool is exhausted, exactly the same root cause as HikariCP `getConnection` timeout.

### Reproduction harness — TestContainers

Reproduce locally before deploying the fix. Pattern:

```java
@Testcontainers
class ConnectionLeakReproTest {
    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:15-alpine");

    @Test
    void suspectedLeakReturnsConnectionsToBaseline() throws Exception {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(pg.getJdbcUrl());
        cfg.setUsername(pg.getUsername());
        cfg.setPassword(pg.getPassword());
        cfg.setMaximumPoolSize(4);
        cfg.setLeakDetectionThreshold(2_000);  // 2s — fires fast in tests
        try (HikariDataSource ds = new HikariDataSource(cfg)) {
            UnderTest svc = new UnderTest(ds);
            for (int i = 0; i < 200; i++) {
                try { svc.doWorkThatMightLeak(i); } catch (Exception ignored) {}
            }
            // give housekeeper a tick to reap any in-flight close
            Thread.sleep(500);
            assertThat(ds.getHikariPoolMXBean().getActiveConnections()).isZero();
        }
    }
}
```

A test that **fails today** is the cheapest possible regression guard. Run the fix, watch the same test go green. Commit both.

## Live-process diagnosis

### HikariCP MBeans (Java/Kotlin)

Enable JMX MBean registration:

```yaml
# Spring Boot
spring.datasource.hikari.register-mbeans: true
spring.datasource.hikari.leak-detection-threshold: 30000  # 30s
```

Once enabled, leak detection logs a stack trace of the thread that acquired a connection and held it past the threshold — that stack is your culprit.

Read live pool state without a JMX client:

```sh
kubectl exec -it <pod> -- jcmd 1 ManagementAgent.start_local
kubectl exec -it <pod> -- jcmd 1 JFR.start name=hikari duration=2m settings=profile
# or pull MBean values via jmxterm
```

Key MBean attributes (`com.zaxxer.hikari:type=Pool (<name>)`):

| Attribute | Healthy | Leaking |
|---|---|---|
| `ActiveConnections` | oscillates around steady-state | climbs monotonically toward `maximumPoolSize` |
| `IdleConnections` | replenished after burst | drains and stays at 0 |
| `ThreadsAwaitingConnection` | mostly 0 | persistently > 0 |
| `TotalConnections` | == max once warmed | == max with all active |

If `ActiveConnections == maximumPoolSize` and `ThreadsAwaitingConnection > 0` for sustained periods, the pool is exhausted. Combine with leak-detection stack traces to pinpoint the holder.

#### Other DataSource implementations

Not every project uses HikariCP. Same diagnosis flow, different attribute names:

| Pool | MBean ObjectName | Active | Waiting | Leak-detection knob |
|---|---|---|---|---|
| HikariCP | `com.zaxxer.hikari:type=Pool (*)` | `ActiveConnections` | `ThreadsAwaitingConnection` | `leakDetectionThreshold` (ms) |
| Tomcat JDBC | `tomcat.jdbc:type=ConnectionPool,name=*` | `Active` | `WaitCount` | `removeAbandonedTimeout` + `removeAbandoned=true` + `logAbandoned=true` |
| Apache DBCP2 | `org.apache.commons.dbcp2:name=*,type=BasicDataSource` | `NumActive` | `NumWaiters` | `removeAbandonedOnBorrow` / `removeAbandonedOnMaintenance` + `abandonedUsageTracking` |
| c3p0 | `com.mchange.v2.c3p0:type=PooledDataSource[*]` | `numBusyConnectionsDefaultUser` | `numThreadsAwaitingCheckoutDefaultUser` | `unreturnedConnectionTimeout` + `debugUnreturnedConnectionStackTraces=true` |

The "leak-detection knob" column is the closest equivalent to HikariCP's stack-trace-on-overdue feature. Enable it in pre-prod, not prod — every overdue checkout pays the cost of an exception stack capture.

### Server-side session inspection

Postgres:

```sql
SELECT pid, state, wait_event_type, wait_event, state_change,
       now() - state_change AS idle_for, query
FROM pg_stat_activity
WHERE datname = '<your_db>'
  AND state IN ('idle in transaction', 'idle')
ORDER BY state_change ASC;
```

Sessions stuck `idle in transaction` for many seconds are the smoking gun — application acquired a connection, started a transaction, then drifted off (waiting on an HTTP call, blocked on a lock, returned without commit/rollback).

MySQL:

```sql
SELECT id, user, host, db, command, time, state, info
FROM information_schema.processlist
WHERE command = 'Sleep' AND time > 30
ORDER BY time DESC;
```

### Thread state from the JVM side

```sh
kubectl exec -it <pod> -- jstack 1 > stack.txt
# threads blocked on getConnection
grep -B 2 -A 10 'HikariPool.*getConnection' stack.txt
# threads holding a connection (look for JDBC driver frames + application frame)
grep -E -B 2 -A 10 'PgConnection|MysqlConnection' stack.txt
```

A thread parked deep inside an HTTP client call while holding a `PgConnection` reference on its stack is the canonical "long-running transaction" leak.

### Heap dump path (when leak detection threshold isn't enough)

```sh
kubectl exec -it <pod> -- jcmd 1 GC.heap_dump /tmp/heap.hprof
kubectl cp <pod>:/tmp/heap.hprof ./heap.hprof
```

In Eclipse MAT, run "Path to GC roots" on `HikariProxyConnection` instances. Filter for instances where the proxy is reachable from anything other than the pool itself — that path identifies the thread/object holding the leaked connection.

### Python live diagnosis

```sh
# socket count to DB
kubectl exec -it <pod> -- sh -c 'ss -tn | grep :5432 | wc -l'

# stack of every thread/coroutine
kubectl exec -it <pod> -- py-spy dump --pid 1
```

`py-spy dump` shows what every coroutine is waiting on. Look for coroutines parked inside an HTTP/IO call while holding an asyncpg connection or SQLAlchemy session — same pattern as the JVM side.

For SQLAlchemy connection accounting:

```python
from sqlalchemy import event
@event.listens_for(engine, "checkout")
def on_checkout(dbapi_conn, conn_record, conn_proxy):
    import traceback
    conn_record.info['stack'] = traceback.format_stack()
```

Then on a stuck pool, dump `pool._refs` or iterate `engine.pool.checkedout()` to print the stacks of who holds checked-out connections.

asyncpg pool inspection:

```python
print(pool._holders)  # list of holders
print(pool.get_size(), pool.get_idle_size())
```

## Fix patterns

### Shrink connection scope

The most common fix isn't adding `close()` — it's holding the connection for less time. A connection acquired inside an HTTP handler should not span an outbound HTTP call.

```java
// LEAK - holds DB connection across slow upstream call
try (Connection c = ds.getConnection()) {
    User u = loadUser(c, id);
    EnrichmentResponse er = httpClient.fetch(u);  // network I/O while holding DB conn
    saveEnrichment(c, er);
}

// FIX - two short transactions, no DB conn during HTTP I/O
User u;
try (Connection c = ds.getConnection()) { u = loadUser(c, id); }
EnrichmentResponse er = httpClient.fetch(u);
try (Connection c = ds.getConnection()) { saveEnrichment(c, er); }
```

### Pool sizing

Pool size larger than `(cores * 2) + effective_spindle_count` is rarely useful and often masks leaks. If raising `maximumPoolSize` "fixed" the symptom, the bug is still there.

### Idempotent transaction boundaries

Wrap with explicit `commit/rollback` in a finally; do not rely on autocommit semantics under exceptions.

### Java/Kotlin `@Transactional` and Hibernate pitfalls

Spring/JPA leaks rarely look like "missing close" — they look like a connection pinned for the duration of a request. The proxy/session/lazy-loading interactions are where engineers lose track of who owns the connection.

**Self-invocation skips the proxy.** `@Transactional` is implemented via a proxy (CGLIB or JDK). Calling `this.foo()` from within the same bean bypasses the proxy entirely, so no transaction starts. The method still runs — and any `EntityManager` or `JdbcTemplate` use inside will lazily acquire a connection on first query, but with no `@Transactional` boundary to release it, the connection lifetime is now tied to the surrounding (possibly absent) transaction or to the open-in-view filter.

```java
// LEAK shape — bar() runs without a TX, EntityManager acquires a conn that
// only releases when the HTTP request ends if OpenEntityManagerInView is on.
public void foo(long id) {
    this.bar(id);   // bypasses proxy — no TX
}

@Transactional
public void bar(long id) { entityManager.find(Entity.class, id); }
```

Fix: inject the bean into itself, or split into two beans.

**`REQUIRES_NEW` inside a loop.** Each iteration opens a brand-new physical transaction (and therefore a fresh `Connection` checkout from the pool while the outer TX still holds one). At `maximumPoolSize=10` and pool sized for the outer load, this self-DOSes the pool.

```java
@Transactional
public void importAll(List<Row> rows) {
    for (Row r : rows) importOne(r);   // each call needs a 2nd connection
}
@Transactional(propagation = REQUIRES_NEW)
public void importOne(Row r) { ... }
```

**`OpenEntityManagerInViewInterceptor` / `OpenSessionInViewInterceptor`.** Spring Boot enables `spring.jpa.open-in-view=true` by default. This binds an `EntityManager` (and therefore a checked-out connection) to the HTTP request for its entire duration — including time spent serializing JSON, calling upstreams from the controller, etc. Under a slow upstream, every in-flight request pins a connection; HikariCP exhausts in seconds. The "leak" is really a structural over-hold. Set `spring.jpa.open-in-view=false` and accept the `LazyInitializationException`s as the price of admission, then fetch eagerly or use DTO projections.

**`@Transactional(readOnly = true)` does not skip connection acquisition.** It hints to Hibernate to skip dirty-checking and to the driver to use a read replica if configured, but the connection is still checked out for the duration of the method. If the method is slow (e.g., paginating a million rows in memory), it pins the connection just as long.

**`EntityManager` as a field on a singleton bean.** A `@PersistenceContext` injected on a `@Service` is actually a thread-bound proxy — fine in Spring. But a *manually constructed* `EntityManagerFactory.createEntityManager()` stored as a field is **not** thread-safe and pins one physical session forever. Grep for `createEntityManager()` outside `@Bean` factory methods.

```sh
rg -n 'createEntityManager\(' --type java --type kotlin
```

**JDBC inside `@Async` methods.** `@Async` runs on a different thread, so the transaction context from the caller is **not** propagated. The async method runs without a transaction and, if it uses JDBC, falls into the same self-invocation trap. Always re-annotate the async method with `@Transactional` (and confirm the executor's threads are sized for the extra pool pressure).

### Verification

After the fix, sample the leak rate again with the cross-cutting triage method. The slope should be flat.

## Prevent

Standing alerts that catch JDBC pool leaks before the pager fires. Requires Micrometer/Prometheus exposing `hikaricp_*` metrics and the Postgres `postgres_exporter`.

```yaml
# Pool exhaustion — applies to any service with a non-trivial pool
- alert: HikariPoolExhausted
  expr: hikaricp_connections_pending > 0
  for: 1m
  annotations:
    summary: "{{ $labels.pool }} threads waiting >0 for 1m"
    runbook: "connection-leak-jdbc: live diagnosis → MBean inspection"

# Idle-in-transaction climb — Postgres side; catches connections pinned by long upstream calls
- alert: PgIdleInTransaction
  expr: |
    sum by (datname) (
      pg_stat_activity_count{state="idle in transaction"}
    ) > 5
  for: 5m
  annotations:
    summary: "{{ $labels.datname }} has >5 idle-in-transaction sessions"
    runbook: "connection-leak-jdbc: server-side session inspection"

# Active connections climbing across pool restart — definitive leak signature
- alert: HikariActiveClimbing
  expr: deriv(hikaricp_connections_active[15m]) > 0.05
  for: 30m
```

`hikaricp_connections_pending` is the cleanest leak signal — a healthy pool keeps it at 0 except during burst load.

## Related

- [`connection-leak-hunt`](../connection-leak-hunt/SKILL.md) — start here to confirm the leak is DB-side; routing flowchart and leak-rate budget live there.
- [`connection-leak-http-grpc`](../connection-leak-http-grpc/SKILL.md) — open when `pg_stat_activity` shows long `idle in transaction` sessions, because the holder is usually parked on an upstream HTTP/gRPC call.
- [`connection-leak-flink`](../connection-leak-flink/SKILL.md) — open when the leaking process is a Flink TaskManager and the JDBC sink is custom (not `JdbcSink.sink(...)`).
