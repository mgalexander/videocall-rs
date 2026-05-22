# Track1/B2: shard egress fan-out loop + fix recent_t0 lock + drop per-receiver format!

Source: ADR-0009 Part A + performance review. Depends on B1.

## Scope
- Shard the serial per-receiver fan-out loop (`chat_server.rs:~3386-3408`) by
  `hash(SessionId) % W` across the B1 pool workers; parse-once preserved
  (`parse_and_inspect` runs once, Arc shared). Barrier model (wait all shards per
  packet) first; pipelined deferred.
- **REQUIRED (the real lock hazard, perf review): shard `recent_t0`** — currently a
  single per-room exclusive `recent_t0.write()` inside the per-receiver loop
  (`forwarder.rs:608`); W workers serialize on it. Make it per-shard or DashMap keyed
  by `(receiver,sender)`. NOTE: video-path only (`forwarder.rs:534`); audio (dominant)
  doesn't touch it.
- **REQUIRED: remove the per-receiver `format!`+`.replace()`** (`chat_server.rs:~3532`,
  ~2 heap allocs/receiver/packet ≈ 800k allocs/s at target) — use SessionId-equality
  self-skip (`forwarder.rs:368`).
- room.read()/subscriptions.read() are READ locks → leave as-is (non-issue per review).

## Acceptance
- Egress parallelizes across W workers with no recent_t0 serialization; alloc rate
  drops; HEALTH_BEACON drop path (`chat_server.rs:3527`) preserved; correctness
  (every allowed receiver still gets every allowed packet, ordering intact); crc=0.
## Branch: scratch/track1-fanout. Priority: P0. Lint: fmt + clippy -D. code+perf review.
