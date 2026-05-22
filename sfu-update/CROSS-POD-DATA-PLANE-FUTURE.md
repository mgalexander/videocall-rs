# Cross-Pod Data Plane (Spillover) — Findings, Current State & Future-Effort Requirements

Status: **DEFERRED to a future effort.** Single-pod + redirect-to-owner is the v1
multi-pod story (proven, exceeds the 200-participant target). This document
captures what was learned, what is already a clean/working implementation, and
what remains to be designed/built for true cross-pod spillover — so the next
effort starts from a known baseline instead of re-discovering it.

Last updated: 2026-05-21. Branch: `experimental-sfu` (tip `da74e81`).
Supporting audits in `sfu-update/audits/200bot-monitor/`:
`MULTIPOD-ROOTCAUSE.md`, `DEFECT1-REDIRECT-BOUNCE.md`,
`DEFECT2-VIDEO-KEYFRAME.md`, `DEFECT3-CROSSPOD-DATAPLANE.md`,
`spillover-decode/DECODE-VERIFY-FINDINGS.md`, `spillover-ramp/`.

---

## 1. What "cross-pod spillover" means here

PLAN Phase 6: a room is consistent-hashed (`jump_hash`) to an owner pod. Clients
that connect to a non-owner pod are redirected to the owner. When the owner
exceeds a soft cap (180 participants / 80% CPU), the room is marked `SpilledOver`
and new joiners are admitted to **spill pods** (adjacent pods) instead of being
redirected — those spill pods receive the senders' media via NATS federation
(`room.{room}.*`) and forward it to their locally-admitted listeners.

**This is genuinely a hard distributed-SFU problem** — distributing ONE room
across multiple SFU instances is what mature SFUs (mediasoup, Janus) treat as an
advanced "cascading/relay" capability. It is NOT required for the v1 target: a
single pod handled 2,500 listeners on starved 256Mi/500m limits (far more at prod
limits), so one room fits one pod for 200 participants with ~10–25× headroom.

---

## 2. Footprint & complexity (why this is a separate effort)

~3,400 LOC of SFU + bot code, across 3 codebases (Rust SFU, Rust bot, Helm) plus
NATS (federation substrate) and K8s DNS:

| Area | File | ~LOC |
|---|---|---|
| Affinity / jump_hash / redirect FQDN | `actix-api/src/sfu/affinity.rs` | 1,173 |
| Health beacon producer | `actix-api/src/sfu/health_beacon.rs` | 800 |
| Spillover store / thresholds / ingest | `actix-api/src/sfu/spillover.rs` | 720 |
| Redirect/spill logic in the room actor | `chat_server.rs` (475 of 6,636) | 475 |
| Redirect teardown | `wt_chat_session.rs` | ~81 |
| Redirect transport (datagram vs uni-stream) | `bridge.rs` | ~76 |
| Receive-all / cross-pod publisher / AllowSet | `forwarder.rs` + `subscription.rs` | ~50 |
| Bot redirect-follow | `bot/src/{orchestrate,webtransport_client}.rs` | ~300 |
| Helm per-pod DNS | `helm/rustlemania-{webtransport,websocket}/templates/{statefulset,headless-service}.yaml` | — |

**12 interdependent mechanisms** must all be correct simultaneously for one
spill-admitted listener to decode one frame: jump_hash ownership → redirect
emission → redirect transport → client redirect-follow → session teardown →
per-pod DNS → beacon producer → beacon ingest → spillover threshold →
admit-vs-redirect decision → cross-pod publisher registration → federation
forward/drop. The path crosses 5 layers (SFU, bot, Helm, NATS, K8s DNS); a bug was
found and fixed in **every** one (13 commits, §4).

---

## 3. Current state — what is CLEAN/WORKING vs what needs enhancement

### WORKING (keep — proven by load test)
- **jump_hash room→owner affinity** (`affinity.rs`). Deterministic ownership.
- **ADMISSION_DECISION redirect emission** on owner mismatch (`chat_server.rs`).
- **Redirect transport** — sent over a reliable uni-stream, not a lossy datagram
  (vc-xnp). Delivery confirmed.
- **Per-pod DNS** — redirect FQDN includes the namespace
  `<pod>.<headless-svc>.<namespace>.svc.cluster.local`; `POD_NAMESPACE` wired via
  downward API + `publishNotReadyAddresses` (vc-el0/vco-5qs). Resolves to the
  exact pod.
- **Client redirect-follow** — bot fires the session-end signal on REDIRECT
  receipt and reconnects to the owner (vc-w71). **redirects_followed went 0→885,
  bounce eliminated** (rejections dropped from 2,105 to ~1/listener).
- **Redirect-to-owner data plane** — listeners that reach the owner pod decode
  real media, crc_mismatches=0 (1,385 listeners decoded audio in one run).
- **Per-room dispatcher liveness watchdog** (vc-9eh) — fixes the silent NATS
  subscription that starved late joiners on a single pod.
- **Health beacon producer + ingest + SpilloverStore + thresholds**
  (`health_beacon.rs`, `spillover.rs`) — implemented and wired; spill-admit
  decision fires in JoinRoom (vc-85p). The control plane functions.
- **Observability** (vc-8wd) — opt-in `SFU_TRACE_ROOM` trace + always-on counters
  (`sfu_join_decision_total`, `sfu_session_teardown_total`,
  `sfu_spillover_owner_count`, `sfu_dropped_total{reason}`, `sfu_forwarded_total`,
  `sfu_allowset_size`). Off by default, low overhead. **Keep this — it is the
  primary tool for the future effort.**

### BROKEN / NEEDS ENHANCEMENT (the future effort)
**The cross-pod federation DATA PLANE: a spill-admitted listener on a non-owner
pod does not receive a remote (cross-pod) publisher's federated media.** Confirmed
by load test: spill-admitted listeners on adjacent pods decoded 0 while owner-pod
listeners decoded fine. Federation DELIVERS the bytes (NATS messages arrive on the
spill pod) — the drop is per-receiver at `Forwarder::decide`.

Known sub-defects (see DEFECT3):
1. **Receive-all VIDEO cap denominator** (`forwarder.rs:487`): admits a publisher
   only while `allow.video.len() < MAX_VISIBLE_VIDEO(6)`, but `allow.video` is
   bound to LOCAL members (~86 fellow listeners on a populated spill pod) → guard
   is always false → cross-pod publisher video dropped as `unsubscribed`. The cap
   must be sized against actual video PUBLISHERS, not local member count.
2. **Cross-pod publisher registration**: a sender on the owner pod is a REMOTE
   session on a spill pod, not a known local member, so the AllowSet/forwarder
   doesn't treat it as forwardable. vc-72a + vc-54j2 attempted this
   (`remote_publishers_snapshot`) but did NOT resolve it in load test — and may
   have **regressed the intra-pod path** (owner-pod decode dropped from ~21k to
   ~925 audio after vc-54j2). REQUIRES re-validation with `SFU_TRACE_ROOM`.
3. **Audio drop contradiction**: code says `recv_all_audio` admits audio
   unconditionally, yet spill-pod audio was 0. Unresolved — needs the trace to
   arbitrate dropped-at-decide vs starved-upstream.
4. **Non-deterministic admit/redirect timing**: the spill-vs-redirect decision
   depends on eventually-consistent beacons (15s freshness, 180 threshold), so
   behavior FLIPPED run-to-run (one run redirects-everyone-to-owner and works, the
   next spill-admits-locally and fails). This non-determinism is itself a
   correctness/testability problem.

---

## 4. The fix arc (13 commits) — what each layer taught us
| Commit | Bead | Layer fixed |
|---|---|---|
| 2e16590 | vc-883 | drain mailbox before stop on REDIRECT |
| 614216b | vc-s9e | hold writer Session clone through drain grace |
| 5c3e79d | vc-1re | bot media/control split + trailer-CRC integrity |
| e1471de | vc-72a | admit cross-pod/co-arrival publishers (attempt 1) |
| e30e637 | vc-k4w | bot extract REDIRECT on both inbound arms |
| aff4b42 | vc-9eh | per-room dispatcher liveness watchdog |
| 1dc5802 | vc-xnp | redirect via reliable uni-stream not datagram |
| 5c45ebb | vc-n9o | tear down redirected session under load |
| 8938518 | vc-w71 | **bot follow REDIRECT on receipt (unblocked 1st multi-pod media)** |
| 494656b | vc-el0 | **redirect FQDN includes namespace (killed the bounce)** |
| f52c35d | vco-5qs | helm POD_NAMESPACE + publishNotReadyAddresses |
| da74e81 | vc-54j2 | register cross-pod publishers (attempt 2 — did not land result) |

The defect-discovery curve did NOT flatten — each fix surfaced another layer. This
is the empirical signal that cross-pod is a substantial remaining effort, not "one
more fix."

---

## 5. Requirements for a correct cross-pod data plane (future effort)

1. **Cross-pod publisher model.** A spill pod must treat the senders' federated
   streams (remote sessions seen on the `room.{room}.*` NATS ingress) as
   first-class forwardable publishers: registered in RoomState with a bounded,
   TTL-reaped remote-publisher set, included in local receivers' AllowSets.
2. **Cap on publishers, not members.** `MAX_VISIBLE_VIDEO` must limit distinct
   video PUBLISHERS a receiver sees, never be diluted by the local listener count.
3. **Audio admission must hold cross-pod.** `recv_all_audio` must actually forward
   a remote publisher's audio to a spill-admitted listener (resolve the §3.3
   contradiction first via trace).
4. **No intra-pod regression.** Any cross-pod change must be proven NOT to degrade
   the owner-pod (intra-pod) forwarding path — gate with the single-pod
   decode-verify as a regression test.
5. **Deterministic / testable admit decision.** Reduce reliance on eventually-
   consistent timing, or make the test harness drive a deterministic spill
   (e.g. pre-warm the owner past threshold before listeners join) so results are
   reproducible. The current run-to-run flip must be eliminated.
6. **O(n) safety at 200+.** Remote-publisher set bounded by sender count
   (≤ ~10 for webinar), not receiver count; no per-receiver O(members) work on the
   hot forward path; no join-wave KFR/AllowSet storms.
7. **Keyframe for cross-pod late joiners.** A spill-admitted listener joining
   mid-stream needs a keyframe (interacts with DEFECT2 — see note).
8. **Observability-driven.** Build/validate against the vc-8wd counters
   (`sfu_dropped_total{reason}` ~0 for sender packets on the spill pod;
   `sfu_forwarded_total` rising) and the `SFU_TRACE_ROOM` trace.

## 6. Recommended approach for the future effort
- Start by **running the existing `SFU_TRACE_ROOM` instrumentation** to get the
  ground-truth forward/drop trace on a spill pod (the diagnostic we deferred) —
  this resolves the audio contradiction and confirms the video cap mechanism
  before writing any more forwarder code.
- Consider whether `da74e81` (vc-54j2) should be **reverted** first (it did not
  land the result and may have regressed intra-pod) so the effort starts from the
  clean redirect-to-owner baseline.
- Treat it as a dedicated design+build (not incremental patching): one design doc
  → trace-confirmed root cause → forwarder/RoomState rework → regression-gated by
  the single-pod decode-verify → multi-pod acceptance run.

## 7. Out of scope for v1 (explicitly)
True cross-pod spillover (one room across multiple pods). v1 ships single-pod +
redirect-to-owner, which exceeds the 200-participant target. The instrumentation
(vc-8wd) stays in to support the future effort.
