# connection-leak-skills

Claude Code skills for diagnosing and fixing connection leaks across JVM (Java/Kotlin) and Python services.

## Layout

```
connection-leak-skills/
├── connection-leak-hunt/         # parent router — start here
├── connection-leak-jdbc/         # DB pool leaks (HikariCP, asyncpg, SQLAlchemy)
├── connection-leak-flink/        # Flink 1.18 operator lifecycle leaks
└── connection-leak-http-grpc/    # OkHttp / Apache HC / Netty / gRPC / aiohttp / httpx / requests
```

Each child skill assumes the cross-cutting triage in `connection-leak-hunt` has been run first.

## Install

Drop each skill folder into your Claude Code skills directory. They are independently installable; the parent calls out the children by name.

## Scope

- Languages: Java, Kotlin, Python
- Runtimes: JVM (Flink 1.18 + K8s Operator, Spring, plain JVM), Python (asyncio + sync)
- Diagnosis: source-code audit + live-process probes (jstack, async-profiler, /proc, lsof, py-spy)
