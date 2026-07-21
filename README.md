# mydis

A Redis-like in-memory key-value server, built from scratch in Rust.

`mydis` is a learning project, not a product. It exists to work through two
things from first principles:

1. **Low-level network programming** — raw sockets, an `epoll` event loop,
   non-blocking I/O, and stream-oriented protocol correctness (TCP
   fragmentation, incremental framing).
2. **Building an asynchronous runtime from scratch** — futures, wakers,
   executors, and reactors, *constructed* rather than consumed.

Redis is the reference point for behavior and wire protocol, but
feature-completeness is a deliberately deferred goal. The networking and runtime
work come first, because they are the point.

## Status

- **Phases 0–5 complete** — non-blocking TCP + epoll event loop, incremental
  RESP parsing, the in-memory store, pipelining, and key expiration/timers.
- **Phase 6 (persistence) nearly complete** — append-only file (AOF) with a
  background fsync thread and crash recovery; graceful shutdown is the remaining
  piece.
- **Phase 7 (the from-scratch async runtime) is the next milestone** — not yet
  started.

See [docs/ROADMAP.md](docs/ROADMAP.md) for the full phase plan and design
rationale.

## Quick start

```bash
# Start the server (listens on port 3490)
cargo run --bin server
```

The server speaks RESP, so it works with `redis-cli`:

```bash
redis-cli -p 3490
```

```
127.0.0.1:3490> PING
PONG
127.0.0.1:3490> SET greeting "hello world"
OK
127.0.0.1:3490> GET greeting
"hello world"
127.0.0.1:3490> EXPIRE greeting 60
(integer) 1
127.0.0.1:3490> TTL greeting
(integer) 60
127.0.0.1:3490> DEL greeting
(integer) 1
```

## Supported commands

The command vocabulary is intentionally small and grows phase by phase.

| Command | Form | Notes |
| --- | --- | --- |
| `PING` | `PING` | Replies `PONG`. |
| `GET` | `GET key` | |
| `SET` | `SET key value [EX\|PX\|EXAT\|PXAT ttl]` | `EX`/`PX` are relative (s/ms); `EXAT`/`PXAT` are absolute Unix timestamps (s/ms). |
| `DEL` | `DEL key` | |
| `EXPIRE` | `EXPIRE key seconds` | Relative. |
| `PEXPIRE` | `PEXPIRE key milliseconds` | Relative. |
| `EXPIREAT` | `EXPIREAT key unix-seconds` | Absolute. |
| `PEXPIREAT` | `PEXPIREAT key unix-milliseconds` | Absolute. |
| `TTL` | `TTL key` | Remaining time in seconds. |
| `PTTL` | `PTTL key` | Remaining time in milliseconds. |
| `PERSIST` | `PERSIST key` | Removes an existing expiration. |

Expiry is handled both lazily (on read paths) and via a timer-driven purge pass,
so timeouts fire without scanning the whole keyspace.

## Persistence

Writes are journaled to an append-only file (AOF). On startup the server replays
the AOF to reconstruct the keyspace, and `fsync` runs on a dedicated background
thread so durability does not block the event loop. Commands are normalized to
deterministic absolute forms before being written (e.g. relative `EXPIRE` →
`PEXPIREAT`), so replay is independent of when it happens.

The AOF path defaults to `appendonly.aof` and can be overridden:

```bash
MYDIS_AOF_PATH=/path/to/mydis.aof cargo run --bin server
```

## Build & test

```bash
# Build the workspace (debug)
cargo build

# Build with optimizations (use when measuring latency, e.g. fsync experiments)
cargo build --release

# Run all tests
cargo test

# Run tests for a single crate
cargo test -p db

# Run with logs visible (gated behind RUST_LOG)
RUST_LOG=debug cargo run --bin server

# Format and lint (the bar is -D warnings)
cargo fmt
cargo clippy --all-targets -- -D warnings
```

## Workspace layout

```
crates/
├── net/         # low-level networking: epoll, signalfd, TCP, over libc
├── protocol/    # RESP parser/serializer and Command/Response types
├── db/          # in-memory store, expiry, and command execution
├── server/      # binary: event loop, connection management, AOF
├── runtime/     # the from-scratch async runtime (Phase 7 — stub for now)
└── playground/  # scratch experiments
```

## Design principles

These invariants are non-negotiable; violating one is a regression even if tests
pass:

- **Parser/DB separation.** `protocol` knows nothing about command semantics;
  `db` knows nothing about bytes or framing; `server` wires them together.
- **Structured responses, not pre-serialized bytes.** Command handlers return
  `Response` values; the protocol layer does the serialization.
- **Buffers have single responsibilities.** The read buffer holds unread
  protocol bytes; the write buffer holds pending outbound bytes. Nothing else.
- **Single-threaded event loop.** The only thread spawned today is the fsync
  worker, and the future runtime is single-threaded by design — no speculative
  `Arc`/`Mutex`/`Send`.

## Dependencies

The dependency list is deliberately minimal. `libc` is used directly for
`epoll`/`eventfd`/`signalfd` rather than via a wrapper like `mio`. RESP and the
async runtime are written by hand — reaching for an existing crate would defeat
the purpose. See [CLAUDE.md](CLAUDE.md) for the full contribution and style
conventions.
