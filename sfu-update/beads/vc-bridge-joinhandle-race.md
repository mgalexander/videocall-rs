# SFU: per-session writer JoinHandle re-awaited after completion → panic → forwarding-dead zombie

Source: `sfu-update/audits/200bot-monitor/DEFECT-JOINHANDLE-PANIC.md` + `soak-4cpu/sfu-panic-evidence.log`.
THE blocker for multi-presenter capacity: panics into a zombie at 500 listeners + 20 presenters.

## Root cause (decisive, code-proven)
`actix-api/src/webtransport/bridge.rs:107` stores the writer task as a bare,
non-fused `JoinHandle<()>`. Per session (`actix-api/src/webtransport/mod.rs:397-398`):
1. `wait_for_disconnect()` — `tokio::select!` polling `_ = &mut self.writer` (`bridge.rs:205`).
2. `shutdown()` — `self.writer.abort(); let _ = self.writer.await;` (`bridge.rs:212-213`).
When the client disconnects and the **writer arm wins the select**, the handle is
polled to `Ready` and consumed; `shutdown()` then `.await`s the SAME completed
handle → `JoinHandle polled after completion` panic (tokio core.rs:412) on the
runtime `main` thread. `abort()` is a no-op on a finished task. It's a select! race
that fires on the subset of teardowns where the writer completes first — so heavy
session churn (500 listeners + 20 presenters) produced 115 panics. NOT
resource-bound (pod was at ~967m/4000m CPU, 142Mi). Confirmed the dispatcher
handle (`chat_server.rs:660/1078`) is only aborted/dropped, never re-polled — ruled out.

## Fix
Guard the re-await in `shutdown()`: `if !self.writer.is_finished() { self.writer.abort(); let _ = (&mut self.writer).await; }`
(or fuse the handle in an `Option` and `take()` it once consumed). Ensure
`wait_for_disconnect` and `shutdown` cannot both poll a completed handle.

## Acceptance
- A `bridge::tests` regression test: a writer task that completes BEFORE shutdown
  does NOT panic on the subsequent shutdown (reproduce the race deterministically).
- replicas=1, 500 listeners + 20 presenters (own-pod senders) for several minutes:
  ZERO `JoinHandle polled after completion` panics; SFU keeps forwarding (CPU not
  collapsing to idle); 0 zombie.
- No regression to teardown (vc-883/vc-xnp/vc-s9e/vc-n9o tests pass).

## Priority: P0 — blocks any multi-presenter run.
## Lint: cargo fmt + clippy -D warnings on actix-api clean.
