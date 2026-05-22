# SFU: redirected session never tears down under load — client hangs, never follows redirect (multi-pod 0-decode root cause)

Source: `sfu-update/audits/200bot-monitor/MULTIPOD-ROOTCAUSE.md` (decisive root
cause) + `spillover-decode/DECODE-VERIFY-FINDINGS.md`.

## The bug (THE blocker for all multi-pod / spillover capacity)
In any replicas≥2 config, a client that gets an ADMISSION_DECISION redirect on a
non-owner pod **never closes its session and never follows the redirect** when it
is actively sending media. Result: senders redirected toward the owner hang on the
non-owner pod, never reach `connection_state == Active`, so **never publish to
NATS** — no media exists in the room — and every listener on every pod decodes 0.
Confirmed: decode-verify run = 1,500 listeners, 0 video/0 audio; senders show
`redirects_followed: 0`, `joined_pod` = a non-owner, then ~7 min of silence.

## Mechanism (file:line)
1. JoinRoom-Err redirect path runs `ctx.notify(StopSession)` (vc-883,
   `actix-api/src/actors/transports/wt_chat_session.rs:422-463`) instead of
   `ctx.stop()`, to let the queued REDIRECT drain first.
2. `ctx.notify` enqueues `StopSession` on the actor *items* list, which actix
   processes only AFTER the mailbox is drained.
3. The bridge unistream reader (`actix-api/src/webtransport/bridge.rs:145-167`)
   keeps `accept_uni`-ing the still-open client's 30fps video + audio and
   `try_send`-ing `WtInbound` into the mailbox → the mailbox is NEVER empty →
   `StopSession` is starved → `ctx.stop()` never runs.
4. `outbound_tx` never drops → the bridge writer never sees `recv()==None`
   (`bridge.rs:205`) → `wait_for_disconnect`/`join_next` never returns
   (`bridge.rs:132`, `mod.rs:389`) → `bridge.shutdown()` never aborts the readers
   → quinn never closes the QUIC session.
5. The client never sees `accept_uni` error → never fires its session-end signal →
   never follows the redirect. It hangs for the whole duration.

Inherently multi-pod-only (needs a redirect) and load-dependent (needs the client
to keep sending, which starves the mailbox). This is why single-pod (no redirect)
works perfectly and every multi-pod run yields 0 decode.

## Fix spec (delicate — preserve prior invariants)
Force the QUIC teardown on a JoinRoom-Err redirect WITHOUT regressing:
- vc-883: the REDIRECT bytes must still drain to the wire before close.
- vc-xnp: the redirect goes via reliable uni-stream (keep).
- vc-s9e: writer Session-clone drain grace on teardown (keep).
Candidate approaches (pick the cleanest that holds all three):
- Stop reading from the client immediately on the redirect decision (abort/stop the
  bridge unistream reader) so the mailbox can drain and `StopSession` runs; OR
- Proactively `session.close()` after the REDIRECT send completes (flush-then-close
  on the redirect path specifically); OR
- Deadline-escalate: if `StopSession` hasn't run within a short grace (e.g. 200-500ms)
  after the redirect is enqueued, force `ctx.stop()` / `session.close()`.
The redirect must still be delivered first (don't reintroduce the vc-xnp/vc-883 bug).

## Acceptance
- replicas≥2 decode-verify: senders FOLLOW the redirect (`redirects_followed > 0`,
  `joined_pod` = owner), reach Active, publish to NATS; listeners decode real media
  with `crc_mismatches=0`.
- Redirect responsiveness preserved: reconnect ≤500ms, first media ≤1.5s.
- vc-883 / vc-xnp / vc-s9e regression tests still pass (REDIRECT drains before close).
- Add the two regression-detector counters (also serves the instrumentation bead):
  `sfu_join_decision_total{outcome=admit_local|redirect|reject}` and
  `sfu_session_teardown_total{reason=redirect|normal|error}`. A redirect-vs-teardown
  gap is the live regression signal.

## Priority: P0 — blocks all distributed/spillover capacity.

## Lint
`cargo fmt` + `cargo clippy -- -D warnings` on `actix-api` clean.
