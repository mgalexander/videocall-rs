# SFU: ingest-saturation observability — the late-joiner delivery failure is currently INVISIBLE

Source: `PRESENTER-SCALING-ROOTCAUSE.md`. The single per-room dispatcher silently
drops inbound at the saturated NATS consumer (async-nats SlowConsumer), so
late-joiner delivery fails with NO signal: crc=0, 0 restarts, and the vc-9eh
watchdog only detects SILENCE (last_msg_at keeps advancing under saturation, so it
never trips — `chat_server.rs:2798`). We cannot verify ANY scaling fix without
making this visible. Foundational + non-controversial; do FIRST.

## Fix
- Count inbound drops / consumer lag on the per-room dispatcher's NATS subscription
  (`chat_server.rs:2916` sub, loop `:3341`): `sfu_dispatcher_inbound_dropped_total`,
  `sfu_dispatcher_lag` (queue depth / pending), `sfu_dispatcher_inbound_rate`.
  Detect async-nats SlowConsumer / Lagged and increment explicitly (don't let it be
  silent).
- Make health/watchdog SATURATION-aware, not just silence-aware: a dispatcher whose
  inbound is being dropped while receivers are non-empty is unhealthy — surface it
  in the forwarding-aware `/healthz` (vc-zf8k) and as a counter.
- Emit it in the join-milestone payload (vc-9eve) too, so a soak shows
  "inbound dropped @ N presenters / M receivers".

## Acceptance
- A 20-presenter / 400-listener soak shows non-zero `sfu_dispatcher_inbound_dropped`
  (the saturation that's currently silent), and /healthz/metrics make it visible.
- No false positives at low load.
## Priority: P0 (v1-blocking observability — gates verifying the scaling fixes).
## Lint: cargo fmt + clippy -D warnings clean.
