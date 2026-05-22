# Track1/B1: move per-room fan-out off the single-thread arbiter onto a multi-thread pool

Source: ADR-0009 Part A + performance review. THE v1 late-joiner fix (E2EE-preserving).
Perf review confirms Track 1 ALONE sustains 20 presenters × 400 listeners (NIC-bound
~1560 receivers, not CPU). Do FIRST — foundational; B2/B3 depend on it.

## Scope
- The per-room dispatcher (`spawn_room_dispatcher`, `chat_server.rs:~2900`) currently
  runs on the room's single-thread arbiter (vc-8txq `Arbiter::new()` current-thread
  runtime, `chat_server.rs:1021`; bare `tokio::spawn` at `:2926` inherits it). Move
  fan-out execution onto a PROCESS-WIDE multi-thread tokio runtime owned by
  `ChatServerPool` (use a pool `Handle`, not bare spawn).
- **Pool size = #cores - 1** (NOT #cores — arbiters/HTTP/WS/nats already use threads;
  realistic gain ~0.6-0.75·C). Config `SFU_FANOUT_WORKER_THREADS`.
- Send/Sync GATE: the captured dispatcher state (`receivers`, `room_state`,
  `forwarder`, `scorer`) is already Arc/lock-guarded and delivery already crosses
  task boundaries via `Recipient::try_send` (`chat_server.rs:3397`) — audit + prove
  Send/Sync before landing. backend-rust-streaming sign-off required.
- Forwarder stays a BYTE RELAY — no media decryption, E2EE fully preserved.

## Acceptance
- Fan-out runs on the multi-thread pool; a 20p×400 soak shows SFU CPU SCALING across
  cores (not flat at 1) and late joiners receive+decode audio+video (crc=0).
- vc-m7k6 ingest-saturation metric shows NO silent drops at 20p×400.
- No regression at small scale; WT+WS; code-reviewer + backend-rust-streaming.
## Branch: implement on `scratch/track1-fanout`; bot-harness validation gate before merge to experimental-sfu.
## Priority: P0 (v1-blocking). Lint: fmt + clippy -D warnings.
