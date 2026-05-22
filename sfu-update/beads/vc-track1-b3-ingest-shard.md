# Track1/B3: parallel/subject-sharded ingest + batch the scorer feed

Source: ADR-0009 Part A + performance review. Depends on B1.

## Scope
- Subject-shard the per-room `room.{room}.*` subscription (`chat_server.rs:2916`)
  across K consumers (`room.{room}.{shard}.{session}`) to remove the single
  `sub.next()` choke; coordinated client+server subject change
  (`models/mod.rs:51-53`, publish `session_logic.rs:615`); dual-subscribe migration
  so K=1 == today. K=1 default; cap rooms×K per pod (K×16384 buffers, `nats_connect.rs:196`).
- **REQUIRED (perf review): batch the scorer feed** — `scorer.write().await`
  (`chat_server.rs:3218`) is a genuine new serialization point under K parallel
  consumers; batch per-consumer and flush once per 200ms scorer tick.
- NOTE: ingest sharding buys drop-resistance + parse parallelism, NOT a higher egress
  ceiling (that's bandwidth). K=1 is correct default until needed.

## Acceptance
- Ingest parallelizes; no single sub.next() choke; scorer no longer serializes per
  packet; 20p×400 soak: no inbound drops (vc-m7k6), late joiners served, crc=0.
## Branch: scratch/track1-fanout. Priority: P1 (after B1/B2). Lint: fmt + clippy -D. code+perf review.
