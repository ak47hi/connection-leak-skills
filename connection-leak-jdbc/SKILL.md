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
grep -B 2 -A 10 'PgConnection\|MysqlConnection' stack.txt
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

### Java/Kotlin `@Transactional` pitfalls

- `@Transactional` on a private method or self-invocation does nothing — connection is acquired without proxy interception, so close timing is unmanaged.
- `@Transactional(propagation = REQUIRES_NEW)` inside a loop creates a new physical session per iteration. Often the leak is structural here.

### Verification

After the fix, sample the leak rate again with the cross-cutting triage method. The slope should be flat.
