# DEFECT: SFU "JoinHandle polled after completion" panic → forwarding-dead zombie

**Status:** Root-caused (read-only investigation). Fix spec below.
**Branch:** `experimental-sfu` @ `fcfb013`
**Evidence:** `sfu-update/audits/200bot-monitor/soak-4cpu/sfu-panic-evidence.log`
**Repro conditions:** 500 listeners + 20 presenters, replicas=1 (NO redirects), pod at ~967m/4000m CPU + 142Mi at panic (NOT resource-bound). Panic first fires ~100s in, ×115. Forwarding tasks die, SFU CPU 967m→34m, TX flatlines; process stays alive (NATS PONGs, `/healthz`=200, 0 restarts) ⇒ **zombie**.

---

## 1. Root cause

### Decisive site — the per-session bridge writer `JoinHandle`

`actix-api/src/webtransport/bridge.rs`

The bridge stores the writer task as a **bare, non-fused** `tokio::task::JoinHandle<()>`:

```
bridge.rs:107   writer: tokio::task::JoinHandle<()>,
```

Each WebTransport session runs exactly this sequence (`actix-api/src/webtransport/mod.rs:397-398`):

```
mod.rs:397   bridge.wait_for_disconnect().await;
mod.rs:398   bridge.shutdown().await;
```

`wait_for_disconnect` polls the writer handle inside a `select!`:

```
bridge.rs:202   pub async fn wait_for_disconnect(&mut self) {
bridge.rs:203       tokio::select! {
bridge.rs:204           _ = self.readers.join_next() => {}
bridge.rs:205           _ = &mut self.writer => {}          // <-- polls the writer JoinHandle
bridge.rs:206       }
bridge.rs:207   }
```

`shutdown` then **awaits the same handle again**:

```
bridge.rs:210   pub async fn shutdown(mut self) {
bridge.rs:211       self.readers.shutdown().await;
bridge.rs:212       self.writer.abort();
bridge.rs:213       let _ = self.writer.await;            // <-- re-polls the SAME handle
bridge.rs:214   }
```

**The panic:** when the `_ = &mut self.writer` arm (line 205) is the branch that completes the `select!`, the writer `JoinHandle` has been polled to `Ready` and the task's join output consumed. Line 213 (`self.writer.await`) then polls that already-completed handle again → tokio's `core.rs:412` `JoinHandle polled after completion` panic.

`abort()` on line 212 does NOT save it: abort on a finished task is a no-op, and `.await` on line 213 still polls the consumed handle.

### Why the writer branch wins under this exact load

The writer task (`spawn_writer`, `bridge.rs:356-399`) breaks out of its send loop and returns whenever a QUIC write/open fails:

```
bridge.rs:375   break;   // stream.write_all error
bridge.rs:386   break;   // session.open_uni error
```

After the loop it sleeps `WRITER_DRAIN_GRACE` (250ms, `bridge.rs:87`/:392) and returns — completing the handle.

On a **client-initiated disconnect / link drop** (the dominant teardown event in this run), the QUIC session faults. This makes BOTH select arms eligible:
- the readers exit on QUIC error and `join_next()` returns (`bridge.rs:259`),
- the writer's next `open_uni`/`write_all` fails, it breaks, drains, and returns.

`select!` is a race. Whenever the **writer** arm is the one polled to `Ready` first (entirely timing-dependent, and increasingly likely once `open_uni` starts failing fast on a dead link), the subsequent `shutdown()` re-polls the completed writer handle and panics.

500 listeners + 20 presenters means a continuous, high-volume stream of session establish/teardown events — every one of them runs `wait_for_disconnect()` then `shutdown()`. The bug fires on the subset of teardowns where the writer arm wins the select. ×115 panics over the run is exactly the shape of "a fraction of hundreds of session teardowns hit the racy branch." It is **session-churn-triggered, not load-exhaustion-triggered** — matching the evidence (not resource-bound).

### Why on the `main` thread

`wait_for_disconnect`/`shutdown` are awaited directly inside `handle_webtransport_session` (`mod.rs:339`), which is driven from the `actix_rt`/tokio runtime in `webtransport::start`, launched on the main runtime in `webtransport_server.rs` (`#[actix_rt::main] async fn main`, line 36-37, the `actix_rt::spawn` block at :164-177). The panic surfaces on the runtime driver thread reported as `main`.

### Candidate ranking

| Rank | Candidate | Verdict |
|---|---|---|
| **1 — DECISIVE** | `bridge.rs` writer `JoinHandle`: polled in `wait_for_disconnect` select (`:205`) then re-awaited in `shutdown` (`:213`) | **Confirmed.** Bare non-fused handle, deterministic double-poll whenever the writer arm wins the select. Runs on every session teardown ×hundreds. |
| 2 — RULED OUT | vc-9eh per-room dispatcher + respawn watchdog (`chat_server.rs:1066`, `:2372`) | The dispatcher `JoinHandle` (`RoomDispatch.task`) is ONLY ever `.abort()`-ed (`:660`) or dropped without awaiting (`:1081`, comment :1078-1080). `grep` confirms NO `.await` on `dispatch.task`/`existing.task` anywhere. The watchdog resubscribes IN PLACE on the same task (`:2550`) and never re-polls a completed handle. Not the cause. |
| 3 — RULED OUT | Bridge `readers` JoinSet `join_next` (`bridge.rs:204`) | `JoinSet::join_next` is the correct consume-once API; `shutdown()` uses `JoinSet::shutdown()` (`:211`). No double-poll. Sound. |
| 4 — RULED OUT | wt_chat_session teardown (vc-n9o/vc-s9e) | Redirect teardown not exercised at replicas=1 (no redirects). Normal teardown routes through the same `wait_for_disconnect`/`shutdown` pair — i.e. it is subsumed by candidate 1, not a separate handle bug. |

---

## 2. Fix spec (P0 — panic)

**Goal:** never poll the writer `JoinHandle` after it has completed.

**Function:** `WebTransportBridge::wait_for_disconnect` and `WebTransportBridge::shutdown` (`bridge.rs:202` and `:210`).

Recommended approach — **track writer completion and skip the re-await**. Two equivalent options:

**Option A (preferred): guard the re-await with `is_finished()`.**
In `shutdown` (`bridge.rs:210-214`), only await the writer if it has not already finished:

```rust
pub async fn shutdown(mut self) {
    self.readers.shutdown().await;
    if !self.writer.is_finished() {
        self.writer.abort();
        let _ = self.writer.await;
    }
}
```
`JoinHandle::is_finished()` is non-consuming and returns `true` once the task has completed (including when its output was already taken in the `select!`). When the writer arm of `wait_for_disconnect` won, `is_finished()` is `true` and we skip the offending `.await`. When the readers arm won, the writer is still live, so we abort+await as before. This is the minimal, surgical fix.

**Option B: fuse the handle behind an `Option`.**
Change the field to `writer: Option<tokio::task::JoinHandle<()>>`. In `wait_for_disconnect`, select on `self.writer.as_mut()` (via `OptionFuture` or an explicit `if let`), and on completion `self.writer = None`. `shutdown` then only acts when `Some`. More invasive (touches the struct + `new_with_callback`), but makes the "polled once" invariant type-enforced.

**Recommendation:** ship Option A (one-line guard, lowest risk, no struct/API change). Add a regression test that drives a bridge where the writer completes first and asserts `shutdown()` does not panic (extend the existing `bridge::tests` module — it already builds bridges and exercises `WRITER_DRAIN_GRACE`, e.g. around `bridge.rs:663-766`).

**Acceptance criteria (P0):**
- `wait_for_disconnect` followed by `shutdown` never panics with "JoinHandle polled after completion", in either select-branch-winner case.
- New unit/integration test: force the writer task to complete (drop `outbound_tx`), poll `wait_for_disconnect` to completion via the writer arm, then call `shutdown()` — must not panic.
- Existing vc-883/vc-s9e REDIRECT-flush guarantees and `WRITER_DRAIN_GRACE` behavior unchanged (the reader-parking + writer-drain semantics are not touched).
- Soak repro (500 listeners + 20 presenters, replicas=1) runs >300s with 0 occurrences of the panic and TX stays flat-line-free (forwarding CPU stays at expected level).

---

## 3. Fail-fast robustness fix (P1 — must not zombie)

The panic above is on a per-session task path, but the broader defect is that **a panic that kills forwarding does not take down the process or fail the health check** — so k8s never restarts and recovery never happens.

### Why it zombies today
- The health endpoint is a **static** `HttpResponse::Ok().body("Ok")` — `webtransport_server.rs:32-34` (`health_responder`) — served by an **independent** `actix_rt::spawn`ed HTTP server (`webtransport_server.rs:142-161`). It has no coupling to forwarding liveness, so it answers 200 forever after forwarding dies.
- There is **no `livenessProbe`/`readinessProbe`** in the SFU statefulset (`helm/rustlemania-webtransport/templates/statefulset.yaml` — `grep` for `Probe` returns nothing). The only health consumer is the DO load-balancer annotation `service.beta.kubernetes.io/do-loadbalancer-healthcheck-path: "/healthz"` (`helm/rustlemania-webtransport/values.yaml:99`), which only affects LB routing, not pod restarts.
- The panic occurs on a spawned task; tokio's default panic behavior unwinds only that task. The runtime and the health server task survive.

### Recommended mechanism (defense in depth — do both)

**(a) Abort-on-panic for the process / panic hook that exits.**
The cleanest "crash so k8s restarts" lever: set a process-global panic hook in `webtransport_server.rs::main` (before any task is spawned, near line 38-48) that logs and then `std::process::abort()`s — OR set `panic = "abort"` in the binary's release profile so any panic on any thread terminates the process. With abort, the ~100s-in writer panic would have crashed the pod immediately, k8s would restart it, and forwarding would recover (instead of 115 silent zombie panics). Prefer an explicit panic hook over `panic=abort` only if some panics must remain recoverable; for an SFU whose entire job is forwarding, `panic=abort` (or hook→abort) is the correct posture.

**(b) Forwarding-liveness-aware readiness probe + k8s liveness probe.**
Make `/healthz` (or a new `/ready`) reflect actual forwarding liveness instead of returning a constant:
- Maintain an atomic "last successful forward" timestamp / heartbeat updated by the per-room dispatcher hot path (`chat_server.rs` dispatcher loop, ~`:2599+`) and/or the bridge writer.
- `health_responder` (`webtransport_server.rs:32`) returns 503 when no forward has happened within a threshold while receivers+publishers are present (the same liveness signal the vc-9eh watchdog already computes — `watchdog_should_resubscribe`, `chat_server.rs:2330`).
- Add a `livenessProbe` (and `readinessProbe`) to `helm/rustlemania-webtransport/templates/statefulset.yaml` hitting that endpoint on `healthPort` (`statefulset.yaml:60`, `service.healthPort`), so k8s restarts the pod when forwarding stalls — closing the zombie gap even for future, unrelated forwarding failures.

**Recommendation:** (a) is the immediate, low-cost guarantee that THIS class of bug can never zombie again — ship it with the P0 fix. (b) is the durable, defect-class-independent safety net (catches any future forwarding stall, not just panics) and should follow.

**Acceptance criteria (P1):**
- Inject a panic in a per-session forwarding/writer task in a test/staging SFU ⇒ process exits non-zero (abort path) and k8s records a restart, OR `/healthz` returns 503 within the configured threshold and the `livenessProbe` restarts the pod.
- Steady-state forwarding keeps `/healthz` at 200 with no false-positive restarts under normal churn (probe `failureThreshold`/`periodSeconds` tuned for real networks per the Change Impact Policy: 200ms+ RTT, jitter — do not floor the window so low that a brief NATS stall trips a restart).
- The forwarding-liveness signal reuses the existing vc-9eh liveness computation rather than a parallel heuristic, to avoid divergence.

---

## 4. Bead breakdown

### Bead A (P0, SFU) — Fix writer `JoinHandle` double-poll panic
- **Scope:** `actix-api/src/webtransport/bridge.rs` — guard `shutdown()`'s writer re-await with `is_finished()` (Option A), or fuse the handle (Option B).
- **Why:** deterministic "JoinHandle polled after completion" panic whenever the writer arm wins the `wait_for_disconnect` select; kills forwarding tasks under 20-presenter/500-listener churn.
- **Acceptance:** see §2. Includes a regression test in `bridge::tests` and a clean >300s soak repro.
- **Owner agent:** `backend-rust-streaming`, then `code-reviewer`.

### Bead B (P1, SFU) — Fail-fast hardening so forwarding death cannot zombie
- **Scope:**
  - (a) panic hook → `std::process::abort()` (or `panic = "abort"`) wired in `actix-api/src/bin/webtransport_server.rs::main` (~:38-48).
  - (b) forwarding-liveness-aware `/healthz` (`webtransport_server.rs:32`) reusing the vc-9eh liveness signal, plus `livenessProbe`/`readinessProbe` added to `helm/rustlemania-webtransport/templates/statefulset.yaml`.
- **Why:** today a forwarding-task panic leaves the process alive with a static 200 `/healthz` and no k8s probe ⇒ zombie, 0 restarts, no recovery.
- **Acceptance:** see §3.
- **Owner agents:** `backend-rust-streaming` (a + health endpoint), `deploy-sync-expert` (probe in chart), then `code-reviewer`.

### Sequencing & impact
- **Bead A is the immediate stop-the-bleed** and unblocks the 20-presenter capacity goal — without it, the SFU goes forwarding-dead ~100s into any run at this concurrency.
- **Bead B is independent** and class-protective; it should land alongside or immediately after A. A alone removes this specific panic; B ensures that if forwarding ever dies again (any cause), the pod restarts and recovers instead of zombying.
- Both are SFU-only; no client/frontend changes. Per Change Impact Policy, validate probe thresholds against high-RTT/jittery networks before tuning them aggressively.

---

## Code citations (file:line)
- `actix-api/src/webtransport/bridge.rs:107` — `writer: JoinHandle<()>` field (bare, non-fused).
- `actix-api/src/webtransport/bridge.rs:202-207` — `wait_for_disconnect` select polling `&mut self.writer`.
- `actix-api/src/webtransport/bridge.rs:210-214` — `shutdown` re-awaiting `self.writer` ⇒ **panic site**.
- `actix-api/src/webtransport/bridge.rs:356-399` — `spawn_writer`; breaks at `:375`/`:386` on QUIC error, returns after drain.
- `actix-api/src/webtransport/mod.rs:397-398` — caller: `wait_for_disconnect().await; shutdown().await;`.
- `actix-api/src/actors/chat_server.rs:660` — dispatcher `task.abort()` (no await; rules out dispatcher).
- `actix-api/src/actors/chat_server.rs:1066-1151` — `RoomDispatcherExited` respawn (drops completed handle, never awaits).
- `actix-api/src/bin/webtransport_server.rs:32-34` — static `health_responder` (zombie enabler).
- `actix-api/src/bin/webtransport_server.rs:142-161` — independent health-server task.
- `actix-api/src/bin/webtransport_server.rs:36-37,164-177` — `#[actix_rt::main]`, runtime driver = reported `main` thread.
- `helm/rustlemania-webtransport/templates/statefulset.yaml` — NO liveness/readiness probe.
- `helm/rustlemania-webtransport/values.yaml:99` — `/healthz` only consumed by DO LB health-check.
