# Project Roadmap: In-Memory Key-Value Server

## Project Goals

This project has two explicit learning objectives:

1. **Low-level network programming** — understanding sockets, epoll, non-blocking I/O, stream-oriented protocol correctness, and the realities of TCP fragmentation.
2. **Building an asynchronous runtime from scratch** — understanding futures, wakers, executors, reactors, and cooperative scheduling by constructing them rather than consuming them.

Redis is used only as a **reference point for the overarching shape of the project** — a known, well-documented in-memory key-value server provides a concrete target. This is **not an attempt to build a feature-complete Redis clone**.

## Non-Goals (Do Not Suggest)

These would enrich a Redis-clone project but do not serve the stated learning goals. Do not propose adding them:

- Non-string data structures (Lists, Hashes, Sets, Sorted Sets, skip lists)
- Transactions (MULTI/EXEC)
- Pub/Sub
- Memory eviction policies (LRU, LFU, maxmemory)
- Server introspection commands (INFO)
- Replication (leader/follower)
- TLS
- Cluster mode / sharding

If a design decision could be made simpler by ignoring one of these, ignore it.

## Guiding Principle

Each phase should leave behind a concrete problem that the next phase solves. By the time the async runtime is being designed, the developer should already be tired of managing per-connection suspended state by hand. The runtime should feel *inevitable*, not arbitrary.

---

## Current State (Completed Phases)

### Phase 0 — Foundation ✅
- Non-blocking TCP sockets
- epoll event loop
- Connection lifecycle management

### Phase 1 — Incremental Parsing ✅
- Persistent read buffers
- Incremental parsing with framing
- Command extraction handling partial reads and batched commands

### Phase 2 — Real Database ✅
- Command dispatch (GET/SET/DEL)
- Shared in-memory state across connections
- Semantic error responses
- Clean separation: parser does not know command semantics; DB does not know bytes/framing
- Stable writable-event handling and partial-write correctness
- Correct connection cleanup (FD deregistration, buffer drop)

### Phase 3 — RESP Protocol ✅
- Length-prefixed, binary-safe framing
- Incremental RESP parser as state machine
- Compatible with `redis-cli`

### Phase 4 — Pipelining ✅
- Multiple in-flight requests without waiting for responses
- Output buffering and queue management

---

## Phase 5 — Expiration and Timers 

### Conceptual Significance

This phase introduces **time as a first-class event source** alongside I/O readiness. The event loop now serves two kinds of wakeups: file descriptor readiness and deadline expiration. This duality directly foreshadows the runtime's need to manage both I/O futures and timer futures. Do not rush past this conceptually.

### Design Decisions Already Made

- **Active expiry**: event loop dynamically computes the `epoll_wait` timeout based on the next expiring key, then runs a purge pass after handling events.
- **Data structure**: `BinaryHeap<Reverse<(Instant, Key)>>` or `BTreeMap<Instant, Key>` for O(1) deadline peek and efficient range-based purge. A **full timer wheel is unnecessary at this scale** — timer wheels are for kernel-level use cases with millions of concurrent timers.
- **Lazy expiry**: in addition to the active purge loop, every read operation (GET, EXISTS, TTL, etc.) checks expiry and treats expired keys as absent. This closes the gap between a key's logical expiry time and the next `epoll_wait` wakeup.
- **Tombstones over eager cleanup**: if a key is explicitly deleted while it has a pending expiry entry in the heap/map, leave the heap entry in place. When the purge pass encounters a heap entry pointing to a key that no longer exists (or has a different expiry), it is simply skipped. This avoids the complexity of removing arbitrary entries from a heap.

### Phase 5 Completion Checklist

- [ ] Heap or sorted map drives timeout calculation (not a full keyspace scan)
- [ ] Purge pass only touches expired entries (not all keys)
- [ ] Lazy expiry on all read paths
- [ ] `SET` on an already-expiring key correctly resets or clears its expiry
- [ ] `EXPIRE` (set TTL in seconds on existing key)
- [ ] `PEXPIRE` (set TTL in milliseconds)
- [ ] `EXPIREAT` (set absolute deadline in seconds since epoch)
- [ ] `PEXPIREAT` (set absolute deadline in milliseconds)
- [ ] `PERSIST` (strip expiry from a key)
- [ ] `TTL` (query remaining lifetime in seconds)
- [ ] `PTTL` (query remaining lifetime in milliseconds)
- [ ] Dangling heap entries on `DEL` handled correctly via tombstone check on purge

---

## Phase 6 — Persistence as a Motivating Problem (CURRENT)

### Purpose

The purpose of this phase is **not** to implement durable storage for its own sake. The purpose is to force a specific experience: the first operation in the project that cannot complete synchronously within a single event loop iteration. This experience is the conceptual seed for the entire async runtime.

### Implementation Sequence

**Step 1 — Naive synchronous AOF.** On every write command, append the command to an append-only file (AOF) and call `fsync` before responding to the client. Measure the latency impact. Every client stalls while the disk catches up. This is the event loop's fundamental weakness made visceral. **Do not skip this step** — feeling the pain matters.

**Step 2 — Background flush thread.** Move the `fsync` to a dedicated background thread. The event loop now has a new problem: how does it know when the flush is done so it can send the client's response?

**Step 3 — Cross-thread signaling via eventfd/pipe.** The natural answer is for the background thread to write a byte to a `pipe` or `eventfd` when the flush completes. The read end is registered with epoll. When epoll wakes up on that descriptor, the event loop reads the result and resumes the suspended client.

### The Realization

After implementing step 3, stop and observe what has been built:

- Something that *starts* work and cannot *immediately* complete it — this is a **Future**.
- The byte written to the pipe on completion, causing epoll to wake the loop and resume — this is a **Waker**.
- The event loop deciding what to do next when work is ready — this is an **Executor**.

All three runtime concepts have been reinvented manually, without the vocabulary. This is the point of the phase.

### Phase 6 Completion Criteria

- [X] Synchronous AOF works and its latency cost has been measured and observed
- [X] Background flush thread handles `fsync`
- [X] Cross-thread signaling via `eventfd` (preferred on Linux) or `pipe`
- [ ] Suspended client state correctly resumed after flush completes
- [ ] No data loss on graceful shutdown
- [X] Crash recovery: replaying AOF on startup reconstructs the keyspace correctly

---

## Phase 6.5 — Make the Implicit Explicit

A short bridging phase. Take the suspended-flush state from Phase 6 and model it formally:

```rust
enum FlushState {
    Pending(Receiver<io::Result<()>>),
    Complete(io::Result<()>),
}
```

Write a `poll` method on it. Return values analogous to `Poll::Pending` and `Poll::Ready`. Call it from the event loop.

The realization: this is `std::future::Future` without the vocabulary. The phase should take at most a day. The point is to arrive at the formal interface deliberately, not to be handed it.

This phase also exposes a real architectural problem: the current event loop has no principled way to track which pending computations belong to which client connections, or to drive multiple of them concurrently. That gap is precisely what the executor fills in Phase 7.

---

## Phase 7 — Build the Runtime

Formalize what was built organically. Before writing code, spend a day reading the actual signatures of `std::future::Future`, `std::task::Waker`, `std::task::Context`, `std::task::RawWaker`, and `std::task::RawWakerVTable`. Understanding the real ABI before building means designing toward it rather than producing something that has to be discarded.

### Components

- **Reactor**: the existing epoll loop. Its job becomes watching OS events and calling wakers for tasks that registered interest in those events. Not new code so much as a reframing of existing code.
- **Waker**: formalize the `eventfd`/`pipe` trick into a proper `Waker` type backed by `RawWaker` and `RawWakerVTable`. This is gnarly Rust; the concrete experience from Phase 6 maps directly onto the formal interface.
- **Executor**: a queue of tasks ready to be polled. Tasks that return `Poll::Pending` leave themselves off the queue and arrange (via their waker) to be re-enqueued when progress is possible. This is the cooperative scheduling mechanism.
- **Task**: a boxed `Future` paired with a `Waker`. Each client connection becomes a task.

### Constraint

Build a **single-threaded** runtime. Do not introduce work-stealing, `Send` bounds, or multi-threaded executors. A correct single-threaded runtime is the foundation; multi-threading is a possible future extension, not part of this phase.

### Phase 7 Completion Criteria

- [ ] `Future`, `Waker`, `Context` implementations compatible with `std::task` types
- [ ] Executor that drives multiple tasks concurrently
- [ ] Reactor integrated with executor: epoll events translate to waker invocations
- [ ] Timer futures (from Phase 5's deadline tracking) integrated as a wakeup source
- [ ] Simple smoke test: spawn N async tasks, observe correct concurrent progress

---

## Phase 8 — Consume the Runtime

Rewrite the server on top of the runtime built in Phase 7.

- Each client connection becomes a task: an `async fn` that reads requests, dispatches commands, and writes responses.
- The persistence flush becomes an `async` operation.
- Timer expiry becomes a timer future.
- The top-level event loop becomes a single call to `executor.run()`.

The educational payoff of this phase is in the **rough edges**. Places where the runtime feels awkward are places where production runtimes (Tokio, async-std, smol) made non-obvious design decisions. Those decisions can now be understood from first principles.

### Phase 8 Completion Criteria

- [ ] Connection handling rewritten as `async fn`
- [ ] Persistence flush exposed as an `async` operation, awaited from the command handler
- [ ] Timer expiry handled via timer futures
- [ ] All previous functionality (RESP, pipelining, expiry, persistence) preserved
- [ ] A written reflection on which design decisions in the runtime felt awkward and why

---

## Style & Architecture Notes

- **Parser/DB separation is sacred.** The parser does not know command semantics. The DB does not know bytes or framing. This separation was established in Phase 2 and should be preserved through every subsequent phase.
- **Structured responses, not pre-serialized bytes.** Command handlers return structured `Response` values. The protocol layer serializes. Do not regress on this.
- **Buffers have single responsibilities.** `read_buf` holds unread protocol bytes only. `write_buf` holds pending outbound bytes only. No scratch semantics.
- **Single-threaded until proven otherwise.** The async runtime in Phase 7 is single-threaded. Multi-threading is out of scope unless explicitly added later.
