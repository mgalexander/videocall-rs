# SFU: production-capable, low-overhead tracing for the join/subscribe/forward path

Source: `sfu-update/audits/200bot-monitor/MULTIPOD-ROOTCAUSE.md` Part 4
(instrumentation design). User requirement: **off by default, opt-in, NOT verbose
— performance matters (200+ participant real-time SFU).** The multi-pod 0-decode
bug took two load tests + two investigations to localize because the
join→admit/redirect→subscribe→forward decision path is unobservable in the wild.
This adds the observability so the next such bug is diagnosable from metrics/logs.

Scope is additive instrumentation. Coordinate with vc-redirect-teardown (Bead A):
A adds `sfu_join_decision_total` + `sfu_session_teardown_total`; THIS bead adds the
rest. Avoid editing the teardown lifecycle (that's A).

## Layer 1 — always-on aggregate counters/gauges (cheap, O(1), no per-packet strings)
In `actix-api/src/metrics.rs` (reuse existing Prometheus registry; if none, follow
the existing metrics pattern):
- `sfu_spillover_owner_count{room}` gauge — the owner member_count the spillover
  decision reads (so we can see WHY is_spilled_over is/ isn't true).
- `sfu_spillover_state{room}` gauge (0/1) — current SpilledOver verdict.
- `sfu_allowset_size` histogram — AllowSet size at resolve time (catch empty-AllowSet
  regressions).
- `sfu_forward_total` / `sfu_dropped_total{reason=unsubscribed|layer_budget|reference_miss|self_skip}`
  counters — increment-only on the forward path; NO string formatting per packet
  (pre-resolved static labels).
These are safe to leave on in production.

## Layer 2 — opt-in targeted trace (debug a SPECIFIC room/session on demand)
- Gate: env var `SFU_TRACE_ROOM=<room_id>` (and optional `SFU_TRACE_SESSION`),
  read once into an `ArcSwap`/atomic at startup + on SIGHUP (or a small admin
  endpoint). The hot path does a **single cheap atomic load + equality check**
  before emitting anything; when unset, zero formatting cost.
- When the room matches, emit structured `tracing` events on a dedicated
  `target = "sfu_trace"` at the decision points (below). Operators enable via
  `RUST_LOG=sfu_trace=debug` + `SFU_TRACE_ROOM=...`. For the per-packet forward
  decision, additionally SAMPLE (e.g. 1/N or first-N-per-second) so a traced room
  doesn't flood logs.
- Decision points to instrument (each records decision + REASON):
  - JoinRoom: admit_local vs redirect vs reject, with reason (spilled_over=?,
    owner=podN, owner_count=?) — `chat_server.rs` JoinRoom (~1524-1564).
  - Session teardown: reason (redirect/normal/error) — coordinate with Bead A's
    `wt_chat_session.rs` teardown (A owns the counter; this adds the trace event if
    not already covered).
  - Subscription/AllowSet resolve: resulting AllowSet membership + why a given
    publisher is/ isn't included — `actix-api/src/sfu/subscription.rs` resolve.
  - Forward/drop per packet (SAMPLED): forward vs drop + drop reason —
    `actix-api/src/sfu/forwarder.rs` decide.

## Acceptance
- With tracing OFF (default), the forward hot path adds at most one atomic load per
  packet (benchmark or reason about it); no per-packet allocation/format.
- `SFU_TRACE_ROOM=<room>` + `RUST_LOG=sfu_trace=debug` emits the full
  join→subscribe→forward decision trace for ONLY that room, sampled on the
  per-packet path; other rooms emit nothing on the trace target.
- Always-on counters/gauges appear on the metrics endpoint and move correctly under
  a multi-pod load test (e.g. `sfu_spillover_owner_count`, `sfu_dropped_total`).
- Documented in a short README/section: how to enable targeted tracing in prod.

## Priority: P1 (parallel with Bead A; counters help validate A).

## Lint
`cargo fmt` + `cargo clippy -- -D warnings` on `actix-api` clean.
