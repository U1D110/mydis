# Project Roadmap — In-Memory Key-Value Server

## Project Goals

Two learning objectives are the **spine** of this project and the reason it
exists:

1. **Low-level network programming** — sockets, epoll, non-blocking I/O,
   stream-oriented protocol correctness, TCP fragmentation.
2. **Building an asynchronous runtime from scratch** — futures, wakers,
   executors, reactors, cooperative scheduling, constructed rather than consumed.

**Terminus:** once the runtime exists, the server evolves into a **usable
Redis-like key-value server** with an intentionally limited but growing command
vocabulary. Redis is a reference point; feature-completeness is an explicit
*later* destination. Feature breadth is layered on **only after** the runtime
lands — the networking and runtime phases come first because they are the point.

**Nothing is permanently excluded.** Data structures, transactions, pub/sub,
eviction, INFO, replication, TLS, and cluster are sequenced as **deferred future
phases** (Phase 9+), ordered by dependency and by how much each stresses the
runtime.

## Guiding Principle

Each phase leaves behind a concrete problem the next phase solves; by the time
the runtime is designed, managing per-connection suspended state by hand should
feel painful enough that the runtime is *inevitable*. Once the runtime exists,
each new command family or data type becomes a new kind of task or storage
concern that exercises the runtime and re-tests the parser/DB separation.

## Architectural Invariants

- **Parser/DB separation is sacred.** Parser knows no command semantics; DB knows
  no bytes/framing; the server wires them.
- **Structured responses, not pre-serialized bytes.** Handlers return `Response`;
  the protocol layer serializes.
- **Buffers have single responsibilities.** `read_buf` = unread protocol bytes;
  `write_buf` = pending outbound bytes.
- **Single-threaded runtime.** The Phase 7 runtime is single-threaded. The only
  thread today is the Phase 6 fsync worker. Features that would force revisiting
  this (replication, cluster) are deliberately the far horizon.

---

## Current State

### Phase 0 — Foundation ✅
Non-blocking TCP sockets, epoll event loop, connection lifecycle.

### Phase 1 — Incremental Parsing ✅
Persistent read buffers, incremental framing, partial/batched command extraction.

### Phase 2 — Real Database ✅
GET/SET/DEL dispatch, shared state, semantic errors, parser/DB separation,
partial-write correctness, connection cleanup.

### Phase 3 — RESP Protocol ✅
Length-prefixed binary-safe framing, incremental RESP state machine,
`redis-cli`-compatible.

### Phase 4 — Pipelining ✅
Multiple in-flight requests, output buffering and queue management.

### Phase 5 — Expiration and Timers ✅
Time as a first-class event source alongside I/O readiness.

- [x] Sorted structure drives timeout (`BTreeSet<(u64, String)>`, not a scan)
- [x] Purge pass touches only expired entries
- [x] Lazy expiry on read paths (GET/TTL/PTTL/PERSIST/DEL)
- [x] `SET` resets/clears expiry on an already-expiring key
- [x] `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT`
- [x] `PERSIST`, `TTL`, `PTTL`
- [x] Dangling-entry handling
- **Data structure:** `BTreeSet` with direct removal (not `BinaryHeap` +
  tombstones) — removal is O(log n) and exact, so tombstones are unnecessary at
  this scale.

### Cross-cutting hardening — LANDED
- **`std::os::fd` adoption (complete).** Owning types (`Poll`, `Wakeup`,
  `TcpListener`, `TcpStream`, `Signals`) hold `OwnedFd` — manual `Drop` impls and
  `-1` sentinels removed; `AsFd`/`AsRawFd` implemented (incl. `Connection`);
  `Poll::register`/`reregister` and `set_nonblocking` take `BorrowedFd`;
  identifier fds are `RawFd`; `Notifier` stays a non-owning `RawFd` (cross-thread
  correctness governed by the shutdown ordering in `Aof::shutdown`).

---

## Phase 6 — Persistence as a Motivating Problem (CURRENT — nearly complete)

Purpose: force the first operation that cannot complete synchronously within one
event-loop iteration — the conceptual seed of the runtime.

### Status

- [x] Synchronous AOF works; latency cost measured and observed
- [x] Background flush thread handles `fsync`
- [x] Cross-thread signaling via `eventfd`
- [x] Crash recovery: AOF replay reconstructs the keyspace
  - AOF **normalizes** commands to deterministic absolute forms before writing
    (relative `EXPIRE` → `PEXPIREAT`, `SET ... EX` → `SET ... PXAT`) so replay is
    time-independent.
- [x] **Suspended client state correctly resumed after flush completes** —
  resume mechanism (`resume_after_flush`) plus the **generation-counter guard**
  that closes the fd-reuse hazard: the eventfd drain validates
  `connection.generation() == completion.generation()` and skips stale
  completions for recycled fds.
- [ ] **No data loss on graceful shutdown** — `Aof::shutdown()` drains and joins
  the worker, but [`Signals::drain`](crates/net/src/signals.rs) is `todo!()`, so
  the SIGINT/SIGTERM → `aof.shutdown()` path is unimplemented/unverified.

### Implementation notes

- **fd-reuse correlation fix (generation counter).** Implemented as `u32`:
  `Connection.generation` assigned at accept (threaded as `next_gen: &mut u32`
  from `main`), carried on `FlushRequest`/`Completion`, validated in the drain
  loop. Monotonic ⇒ no ABA; a recycled fd's new connection always differs.
  (`generation`, not `gen` — `gen` is reserved in edition 2024.)

### Remaining work

- **Graceful shutdown.** Implement `Signals::drain`, wire the signalfd branch to
  reach `aof.shutdown()` cleanly, and verify no queued writes are lost.

### Completion transport

The design uses a **centralized** shared `Receiver<Completion>` in `Aof` plus a
single `eventfd`, dispatching completions to connections by fd (validated by
generation). It already has the reactor/waker shape Phase 7 formalizes.

---

## Phase 6.5 — Make the Implicit Explicit

**Status: partially started.** A `FlushState` enum is already drafted in
[connection.rs](crates/server/src/connection.rs) —

```rust
enum FlushState {
    Pending { generation: u64, response: Vec<u8> },
    Ready(io::Result<()>),
}
```

— but it is **not yet wired in**: the connection still runs on
`pending_flush: Option<Vec<u8>>` with `block_on_flush`/`resume_after_flush`/
`discard_pending_flush`. The remaining work formalizes the suspended-flush state
on top of the centralized transport:

- Introduce a `Poll<T>` enum (`Ready(T)` / `Pending`) mirroring
  `std::task::Poll`, rediscovered deliberately.
- Put `poll(&mut self) -> Poll<io::Result<()>>` on **`FlushState`** (the
  "future"); give `Connection` a `flush: Option<FlushState>` field and a thin
  `poll_flush` driver (the "task"), replacing `pending_flush` and the three
  block/resume/discard methods.
- **Reactor step:** the eventfd drain (after the generation check) delivers the
  worker's result via `FlushState::complete(result)` (`Pending → Ready`).
- **Executor step:** the loop calls `connection.poll_flush()`; on
  `Ready(Ok(()))` it queues the response, resumes command processing, and
  flushes; on `Ready(Err(_))` it discards and closes.
- **Reconcile the generation type:** the draft's `Pending { generation: u64 }`
  disagrees with the live `u32` generation used everywhere else — unify on one.

The realization: this is `std::future::Future` without the vocabulary, and the
`complete`/`poll` split rehearses the reactor-delivers / executor-polls
separation that Phase 7 makes real. The back-to-back call looks like ceremony at
one connection — that is expected; the payoff is the *shape*. It also exposes the
gap the executor fills in Phase 7: no principled way to track/drive many pending
computations concurrently.

---

## Phase 7 — Build the Runtime

Formalize what was built organically. Read the real signatures of
`std::future::Future`, `std::task::{Waker, Context, RawWaker, RawWakerVTable}`
before writing code.

- **Reactor:** the existing epoll loop, reframed to call wakers for tasks that
  registered interest.
- **Waker:** formalize the eventfd trick into a proper `Waker` backed by
  `RawWaker`/`RawWakerVTable`.
- **Executor:** a queue of ready tasks; `Poll::Pending` tasks arrange re-enqueue
  via their waker.
- **Task:** a boxed `Future` + `Waker`; each connection becomes a task. The
  `FlushState`/`generation` work from 6.5 feeds directly in.

**Constraint:** single-threaded. No work-stealing, no `Send` bounds.

- [ ] `Future`/`Waker`/`Context` compatible with `std::task`
- [ ] Executor drives multiple tasks concurrently
- [ ] Reactor integrated: epoll events → waker invocations
- [ ] Timer futures (from Phase 5 deadlines) as a wakeup source
- [ ] Smoke test: spawn N tasks, observe concurrent progress

---

## Phase 8 — Consume the Runtime

Rewrite the server on the runtime. Each connection becomes an `async fn`; the
flush becomes an `async` operation; timer expiry becomes a timer future; the top
loop becomes `executor.run()`. The payoff is the rough edges.

- [ ] Connection handling as `async fn`
- [ ] Persistence flush as an awaited `async` operation
- [ ] Timer expiry via timer futures
- [ ] All prior functionality preserved (RESP, pipelining, expiry, persistence)
- [ ] Written reflection on which runtime decisions felt awkward and why

---

## Product Phases — Flesh Out the KV Server (built on the runtime)

These phases are **deferred, not excluded**. Each builds on the finished runtime
and must preserve the architectural invariants (parser/DB separation, structured
responses, single-threaded). Ordering is by dependency and runtime stress.

### Phase 9 — String & Keyspace command completeness
The "limited vocabulary" fleshed out first, staying within the current string
value model.
- Strings: `EXISTS`, `INCR`/`DECR`/`INCRBY`/`DECRBY`, `APPEND`, `STRLEN`,
  `GETSET`, `SETNX`, `GETDEL`, `MGET`/`MSET`/`MSETNX`, `GETRANGE`/`SETRANGE`
- Keyspace: `KEYS`, `SCAN` (cursor), `TYPE`, `RENAME`/`RENAMENX`, `RANDOMKEY`,
  `DBSIZE`, `FLUSHDB`/`FLUSHALL`, `COPY`
- Server/admin: `ECHO`, minimal `COMMAND`, minimal `CONFIG GET`

### Phase 10 — Additional data structures
The value type becomes an enum; `TYPE` gains meaning; storage model is stressed.
- Lists (`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LRANGE`/`LLEN`…)
- Hashes (`HSET`/`HGET`/`HDEL`/`HGETALL`…)
- Sets (`SADD`/`SREM`/`SMEMBERS`/`SISMEMBER`/`SCARD`…)
- Sorted Sets (`ZADD`/`ZRANGE`/`ZSCORE`… via `BTreeMap` or skip list)

### Phase 11 — Expanded persistence & durability
- RDB-style snapshotting; AOF rewrite/compaction; configurable fsync policy
  (`always`/`everysec`/`no`); `BGSAVE`/`BGREWRITEAOF` as async tasks on the
  runtime.

### Phase 12 — Transactions
- `MULTI`/`EXEC`/`DISCARD`/`WATCH` (optimistic locking).

### Phase 13 — Pub/Sub
- `SUBSCRIBE`/`PUBLISH`/`PSUBSCRIBE`; channel registry; each subscriber a task.

### Phase 14 — Eviction & memory policy
- `maxmemory`, approximate LRU/LFU via key sampling.

### Phase 15 — Introspection & observability
- `INFO`, `SLOWLOG`, `MONITOR`, `CLIENT LIST`, latency stats.

### Phase 16 — Multi-node (far horizon; may re-scope the single-threaded rule)
- Replication (leader/follower), then TLS, then cluster/sharding. These are the
  features that could force revisiting the single-threaded constraint, so they
  are deliberately last and will be re-planned when reached.
