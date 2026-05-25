# SFU Observability & Targeted Tracing (vc-8wd)

Two complementary layers instrument the SFU join → subscribe → forward path.
Layer 1 is **always on** and cheap. Layer 2 is **off by default** and lets you
trace a single room/session on demand without flooding logs or slowing the
forward hot path.

> This is a 200+ participant real-time SFU. With tracing OFF (the default), the
> per-packet forward path adds **at most one relaxed atomic load** and does **no
> per-packet allocation or string formatting**. See "Hot-path guarantee" below.

## Layer 1 — always-on aggregate metrics

Exposed on the existing Prometheus `/metrics` endpoint (default registry — no
endpoint changes). All metric names are prefixed `sfu_`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `sfu_spillover_owner_count` | GaugeVec | `room` | Owner-pod participant count the spillover decision read at JoinRoom time (0 if no fresh beacon). Explains *why* `is_spilled_over` is/isn't true. |
| `sfu_spillover_state` | GaugeVec | `room` | Current SpilledOver verdict (1 = spilled over, 0 = not). |
| `sfu_allowset_size` | Histogram | — | Resolved AllowSet video size, observed on each (re)compute (not on cache hits). A pile-up at bucket 0 = receivers resolving to "see nobody" (empty-AllowSet regression). |
| `sfu_forward_total` | Counter | — | Increment-only count of forward decisions. |
| `sfu_dropped_total` | CounterVec | `reason` | Increment-only drops. Reasons: `unsubscribed`, `layer_budget`, `reference_miss`, `self_skip` (plus the pre-existing `kfr_unsubscribed`). |

`sfu_dropped_total` reason labels are static `&'static str` constants — there is
**no** per-packet string formatting. All reason labels are touched at zero on
startup so they appear in `/metrics` before the first drop.

These work correctly under multi-pod load: gauges are per-pod and labeled by
`room`; counters are monotonic per-pod. Aggregate across pods in Prometheus with
`sum by (room) (...)` / `sum(rate(sfu_forward_total[1m]))` as usual.

## Layer 2 — opt-in targeted trace

Emits structured `tracing` events on the dedicated target `sfu_trace` at four
decision points, **only** for the room you select:

- **JoinRoom** — `admit_local` (spilled_over) vs `redirect` (wrong_owner), with
  `spilled_over`, `self_ordinal`, `owner_count`.
- **AllowSet resolve** — resulting `audio_len` / `video_len` and the generations
  that drove the (re)compute, on the configured receiver session.
- **Forward/drop per packet** — `forward`/`drop` + drop reason, **sampled**
  (1-in-N) so a busy room emits a bounded trickle, not a flood.

(Session teardown tracing is intentionally **not** added here — that lifecycle
and its counter are owned by a separate bead, vc-n9o / "Bead A".)

### Enabling it in production

1. Set the env var(s) on the pod(s) you want to trace and restart them
   (a single canary pod is enough; the env is read **once** at startup):

   ```bash
   SFU_TRACE_ROOM=<room_id>          # required to arm tracing at all
   SFU_TRACE_SESSION=<session_id>    # optional: narrow to one session
   SFU_TRACE_FORWARD_SAMPLE=200      # optional: 1-in-N forward sampling (default 200)
   ```

2. Raise the log level for the `sfu_trace` target:

   ```bash
   RUST_LOG=sfu_trace=debug
   ```

3. Watch the pod logs. Only the configured room/session emits on `sfu_trace`;
   all other rooms emit nothing on that target.

> `SFU_TRACE_ROOM` uses the raw room id; the forwarder gate matches against the
> room's stored id. If your room id contains spaces, match the form the SFU
> stores (spaces are normalized to `_` in NATS subjects but the in-memory
> `RoomState.room_id` is the raw id — use the raw id here).

### Live refresh

Targeting is read **once at startup**; there is no SIGHUP refresh. To retarget,
update the env var and restart the pod (or a canary). This keeps the hot-path
gate a plain `AtomicBool` with no signal-handler machinery.

## Hot-path guarantee (tracing OFF, the default)

When `SFU_TRACE_ROOM` is unset:

- A single `AtomicBool` (`TRACE_ENABLED`) stays `false`.
- `Forwarder::decide` reads it once via `trace::tracing_enabled()` — one
  **relaxed** atomic load — and skips cloning the room id (no `String` alloc).
- Each decision point calls `trace_forward_decision(&None, …)`, which is a
  single `if let Some` branch that returns immediately. Rust does **not**
  evaluate the `tracing::debug!` macro arguments behind the `false`/`None`
  guard, so no formatting and no allocation occur.
- The 1-in-N sampler's atomic counter is only touched inside a traced room, so
  the global forward path never perturbs it.

Net per-packet cost with tracing OFF: **one relaxed atomic load**, zero
allocations, zero string formatting — the Layer 1 counters/gauges aside, which
are O(1) atomic increments using compile-time-constant labels.
