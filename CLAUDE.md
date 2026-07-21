Project Context
In-memory key-value server built from scratch in Rust. Two learning goals:

Low-level network programming (epoll, non-blocking I/O, stream-oriented protocols)
Building an asynchronous runtime from scratch

Read docs/ROADMAP.md before suggesting architectural changes or new features. It contains the phase plan and current state.

Build & Test
bash# Build the workspace (debug)
cargo build

# Build with optimizations (use when measuring latency, e.g. Phase 6 fsync experiments)
cargo build --release

# Run the server
cargo run --bin server

# Run all tests
cargo test

# Run a single test by name
cargo test test_partial_command_parsing

# Run tests in a specific crate
cargo test -p protocol

# Run with logs visible (logs are gated behind RUST_LOG)
RUST_LOG=debug cargo run --bin server

# Format and lint before committing
cargo fmt
cargo clippy --all-targets -- -D warnings

Workspace Layout
server/
├── Cargo.toml              # workspace manifest
├── CLAUDE.md               # this file
├── docs/
│   └── ROADMAP.md          # phase plan and design philosophy
└── crates/
    ├── net/                # low level networking
    ├── server/             # binary: event loop, connection management
    ├── protocol/           # RESP parser and serializer
    ├── db/                 # in-memory store, expiry, command execution
    └── runtime/            # (Phase 7+) custom async runtime

Architectural Invariants
These are non-negotiable. Violating any of them is a regression, even if tests pass.

Parser/DB separation. The protocol crate does not know command semantics. The db crate does not know bytes or framing. The server crate wires them together.
Structured responses, not pre-serialized bytes. Command handlers return structured Response values. The protocol crate serializes. Never have a command handler return Vec<u8>.
Buffers have single responsibilities. read_buf holds unread protocol bytes only. write_buf holds pending outbound bytes only. No scratch use of either.
Single-threaded until proven otherwise. The event loop is single-threaded. The only thread spawned today is the Phase 6 fsync worker. The Phase 7 runtime will be single-threaded. Do not introduce Arc, Mutex, or Send bounds speculatively.

Code Style

Edition: 2024. MSRV is current stable; do not gate on older versions.
Error handling: Never unwrap() or expect() in non-test code except where a comment explains why the invariant holds. panic! is for invariant violations, not for I/O errors.
Naming: Redis commands are uppercase in user-facing strings ("GET", "EXPIRE") and snake_case in Rust identifiers (fn cmd_get, fn cmd_expire).
No async keyword yet. This codebase is synchronous through Phase 6. The async runtime is built from scratch in Phase 7; do not pull in tokio, async-std, smol, or futures as dependencies before then.

Dependencies
The dependency list is deliberately minimal. Before adding a crate, justify why it cannot reasonably be written in this project. Currently approved:

libc for epoll/eventfd — the project intentionally uses this directly rather than wrappers like mio

Not approved without discussion:

Any async runtime (tokio, async-std, smol, futures)
Any networking abstraction layer (mio, socket2 beyond raw FD utilities)
Any Redis-protocol crate — RESP is implemented from scratch as a learning exercise

Testing Conventions

Time-dependent tests use injected clocks. The db crate's expiry logic takes a Clock trait, not Instant::now() directly. Tests use TestClock. Do not call Instant::now() directly in db.

What to Do When Stuck

If a design question doesn't have an obvious answer from this file or the roadmap, ask before guessing. The roadmap exists precisely so that architectural decisions are deliberate.
If a test failure looks like a flake, it isn't. The codebase has no known sources of nondeterminism. Investigate the actual cause.
If cargo clippy warns about something that seems pedantic, fix it anyway. The bar is -D warnings and that bar is held.