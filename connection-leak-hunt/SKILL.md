---
name: connection-leak-hunt
description: Diagnose and fix connection leaks in JVM (Java/Kotlin) and Python services. Use this skill whenever the user reports symptoms like "Too many open files", FD exhaustion, "connection pool timeout", "HikariCP leak detection", "Netty LEAK", climbing TaskManager thread counts, slow Flink job shutdown, "ManagedChannel was not shutdown properly", `pg_stat_activity` idle-in-transaction climbing, or any indication that connections (DB, HTTP, gRPC, Kafka, Flink connectors) are not being released. Trigger on phrases like "leak", "leaking", "fd count climbing", "pool exhausted", "connection timeout under steady load", "sockets in CLOSE_WAIT", or whenever production graphs show connection counts trending up without bound. Routes to a domain sub-skill based on connection type.
---

# Connection Leak Hunt

Top-level router. Connection leaks come in three flavors with different signatures and fix patterns:

- **JDBC / DB pool leaks** → `connection-leak-jdbc`
- **Flink connector / `RichFunction` lifecycle leaks** → `connection-leak-flink`
- **HTTP / gRPC client leaks** → `connection-leak-http-grpc`

Symptoms mislead. A "Too many open files" alert can come from any of the three, and the diagnosis path is wildly different. Classify the leaked resource before drilling in.

## Cross-cutting triage (always run first)

Run these three steps before opening a sub-skill. They confirm the leak is real, bound the rate, and identify the resource class.

### 1. Confirm the FD trend

For a JVM or Python process in a pod (Flink TaskManager containers run the JVM as PID 1):

```sh
kubectl exec -it <pod> -- sh -c 'ls /proc/1/fd | wc -l; cat /proc/1/limits | grep "open files"'
```

Sample over several minutes. A flat count rules out a leak — the user is likely seeing pool contention, burst load, or a sizing problem, not a leak.

```sh
kubectl exec -it <pod> -- sh -c 'for i in $(seq 1 10); do ls /proc/1/fd | wc -l; sleep 30; done'
```

If the count is monotonically rising, note the slope (FDs/min). That number is the leak rate and you'll use it to verify the fix.

### 2. Classify the FDs

```sh
kubectl exec -it <pod> -- sh -c 'ls -l /proc/1/fd | awk "{print \$11}" | cut -d: -f1 | sort | uniq -c | sort -rn'
```

| Dominant FD type | Likely sub-skill |
|---|---|
| `socket` only, paired with `anon_inode` (epoll) climbing | http-grpc (Netty/event-loop) or jdbc |
| `socket` to DB port (5432, 3306, etc.) | jdbc |
| Regular files or `pipe` | flink (RocksDB iterators, log handles) |
| Mixed sockets across many remotes | http-grpc |

For sockets, get the remote endpoints to disambiguate jdbc vs http-grpc:

```sh
kubectl exec -it <pod> -- sh -c 'cat /proc/1/net/tcp | awk "NR>1 {print \$3}" | sort | uniq -c | sort -rn | head -20'
# remote addr is hex: 0100007F:1538 = 127.0.0.1:5432
```

### 3. Identify the runtime

| Runtime | Tools assumed by sub-skills |
|---|---|
| Java / Kotlin | `jcmd`, `jstack`, `jmap`, async-profiler, JFR, Eclipse MAT |
| Python (asyncio or sync) | `py-spy`, `psutil`, `lsof`, `tracemalloc` |

If async-profiler isn't on the image, it can be sideloaded:

```sh
kubectl cp async-profiler-3.0-linux-x64.tar.gz <pod>:/tmp/
kubectl exec -it <pod> -- tar -xzf /tmp/async-profiler-3.0-linux-x64.tar.gz -C /tmp/
```

## Routing

| Resource type | Symptom signature | Sub-skill |
|---|---|---|
| DB connections | HikariCP `getConnection` timeouts, idle-in-transaction climbing on the DB side, sink throughput collapse | `connection-leak-jdbc` |
| Flink operator state | FD count climbs in lockstep with checkpoint count or restart count, slow `cancel`, "Task did not exit gracefully" | `connection-leak-flink` |
| HTTP / gRPC sockets | `lsof` shows growing ESTABLISHED or CLOSE_WAIT to non-DB ports, Netty `LEAK:` lines in logs | `connection-leak-http-grpc` |

If the user is unsure which, run cross-cutting triage steps 1 and 2 to disambiguate before routing.

## Style

This skill family uses imperative, command-block style. Show the audit command, show the fix, move on. Skip primer-style explanations. Comments inside code blocks should be load-bearing — no decorative narration.
