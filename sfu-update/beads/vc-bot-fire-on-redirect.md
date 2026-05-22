# bot: follow ADMISSION_DECISION REDIRECT on receipt — fire the session-end signal instead of waiting for the session to close

Source: live diagnosis of the decode-verify retest (2026-05-20). THE bot-side
root cause of multi-pod 0-decode, traced to an exact line.

## The bug (2-line, bot-side, explains the whole redirect saga)
When the inbound consumer extracts an `ADMISSION_DECISION{REDIRECT}`, it stashes
the target but never WAKES the reconnect loop:

`bot/src/webtransport_client.rs:715-722` (`handle_inbound_stream_data`):
```rust
if let Some(signal) = session_end {
    if let Some(target) = try_extract_redirect_target(&data) {
        info!("... received ADMISSION_DECISION REDIRECT to {}", target);
        *signal.redirect_to.lock().unwrap() = Some(target);   // <-- stash only
    }
}
```
It sets `redirect_to` but does NOT call `SessionEndSignal::fire(...)`, so
`ended` stays false and `notify_waiters()` is never called.

The reconnect loops in `run_sender` (`orchestrate.rs:589-592`) and `run_listener`
park on `signal.notify` / `signal.ended` and only proceed to check `redirect_to`
AFTER the session ends. `fire(None)` is only called on the terminal disconnect arm
(`webtransport_client.rs:582-583`) — i.e. when `accept_uni` errors because the SFU
closed the session.

So the bot has the redirect target in hand but **waits for the SFU to close the
QUIC session before following it.** Confirmed in the decode-verify retest: senders
logged "received ADMISSION_DECISION REDIRECT to ...-0", then started producing and
ran the full duration on the NON-OWNER pod — `redirects_followed=0`. Their sessions
never reached Active (JoinRoom was redirected), so they never published to NATS →
no media in the room → every listener decoded 0.

This is why every prior fix (vc-883 drain, vc-xnp uni-stream delivery, vc-n9o
server teardown) failed to make multi-pod work: they all targeted the SFU closing
the session, but the bot never needed that — it should act on the redirect directive
proactively.

## Fix (decouple redirect-follow from server teardown)
In `handle_inbound_stream_data` (`webtransport_client.rs:716-722`), on redirect
extraction call `signal.fire(Some(target))` (which stashes the target AND sets
`ended=true` AND `notify_waiters()`) instead of only stashing. This wakes the
reconnect loop immediately; it drops the current client (Drop closes the client
side) and reconnects to the redirect target — no dependency on the SFU teardown.

Applies to BOTH senders and listeners (both attach a `session_end` and take the
inline-drain arm). Keep the existing `MAX_REDIRECT_HOPS` cap and the
`record_redirect_hop`/`redirect_chain` latency bookkeeping.

## Acceptance
- replicas≥2 decode-verify: senders FOLLOW the redirect (`redirects_followed > 0`,
  `joined_pod` resolves to the owner), reach Active, publish to NATS; listeners
  across all pods decode real media with `crc_mismatches=0`.
- `redirect_chain` populated with hop latency; reconnect ≤500ms, first media ≤1.5s.
- No regression for the direct-connect (no-redirect) case: a bot that never gets a
  redirect still runs normally and `fire(None)` on real disconnect still works.
- Unit/integration: a bot receiving a REDIRECT follows it WITHOUT requiring the
  server to close the session first.

## Priority: P0 — this is the actual blocker for multi-pod media delivery.

## Lint
`cargo fmt` + `cargo clippy -- -D warnings` on `bot` clean.
