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

One-shot from a fresh clone:

```sh
git clone https://github.com/<your-fork>/connection-leak-skills.git
cd connection-leak-skills
./install.sh
```

This copies the four skill folders into `~/.claude/skills/`. Restart Claude Code to pick them up.

Flags:

| Flag | Behavior |
|---|---|
| `--link` | symlink instead of copy — edits in this repo are picked up live (good for development) |
| `--uninstall` | remove the four skills from the target directory |
| `--dry-run` | print actions without executing |

Override the target with `CLAUDE_SKILLS_DIR=/some/path ./install.sh` (e.g. for a non-default Claude Code install).

Manual fallback: copy each `connection-leak-*` folder into the skills directory by hand. The skills are independently installable; the parent calls out the children by name.

## Scope

- Languages: Java, Kotlin, Python
- Runtimes: JVM (Flink 1.18 + K8s Operator, Spring, plain JVM), Python (asyncio + sync)
- Diagnosis: source-code audit + live-process probes (jstack, async-profiler, /proc, lsof, py-spy)
