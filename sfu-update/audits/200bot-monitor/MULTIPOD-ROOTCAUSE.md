# Multi-pod 0-decode — root cause + instrumentation spec — 2026-05-20

Read-only investigation against `experimental-sfu` @ `1dc5802` (vc-xnp + vc-85p).
Source of truth for the run: `sfu-update/audits/200bot-monitor/spillover-decode/`
(`run.log`, `s.log`, `l1.log`, `decode-verify.sh`) and the ramp findings in
`spillover-ramp/SPILLOVER-RAMP-FINDINGS.md`.

---

## TL;DR (decisive)

The data plane is not broken in the SFU forwarder. **No sender media ever enters
NATS in a multi-pod run, because every sender is REDIRECTED off its non-owner pod
and then HANGS — the redirect is never followed and the connection is never
closed.** With zero media in the system, every listener (on every pod, including
the owner) decodes 0. This is a **redirect-teardown bug**, not a
subscription/AllowSet/federation bug.

Two distinct defects compound it, ranked by blast radius:

1. **(PRIMARY — the blocker) Server fails to tear down the QUIC session on a
   JoinRoom-Err (redirect) decline under load.** `WtChatSession::join_room`
   queues `ctx.notify(StopSession)` (vc-883), but the actor's mailbox is
   continuously refilled by inbound media from the still-open client
   (`Handler<Packet>` / the bridge unistream reader `try_send`ing `WtInbound`),
   so the queued `StopSession` *item* is starved and `ctx.stop()` never runs.
   `outbound_tx` never drops → the bridge **writer** never sees `recv()==None` →
   `wait_for_disconnect()` (`join_set.join_next`) never returns →
   `bridge.shutdown()` never aborts the readers → the QUIC session stays open for
   the entire run. The redirected client therefore never sees `accept_uni` error,
   never fires its session-end signal, never follows the redirect.
   - File anchors: `actix-api/src/actors/transports/wt_chat_session.rs:422-463`
     (`join_room` → `ctx.notify(StopSession)`), `:390-399`
     (`Handler<StopSession>` → `ctx.stop()`), `:403-415` (`Handler<Packet>` keeps
     forwarding inbound), `actix-api/src/webtransport/mod.rs:383-393`
     (`wait_for_disconnect` → `shutdown` → `StopSession` ordering),
     `actix-api/src/webtransport/bridge.rs:131-138` (`wait_for_disconnect` =
     `join_next`), `:198-241` (writer ends only on `recv()==None`), `:140-186`
     (readers exit only via `shutdown()`/`accept_uni` error).

2. **(SECONDARY — masks #1 and is the headline "1,210 redirects") Spillover
   never engages because the owner-pod beacon count stays below threshold.** The
   beacon publishes the owner pod's *local* `RoomState::member_count()`
   (`actix-api/src/sfu/health_beacon.rs:389-394`). But because of defect #1 the
   senders that get redirected to the owner never actually join it, and most
   listeners are redirected away, so the owner's local member count never climbs
   past 180. `SpilloverStore::is_spilled_over` therefore returns false on the
   non-owner pods (`actix-api/src/sfu/spillover.rs:147-151`,`188-194`) and they
   keep redirecting. Even when spillover *did* engage in the ramp, the load was
   connection-only — defect #1 still zeroed the media.

Single-pod (replicas=1) works because `compute_redirect_target` returns `None`
for the sole owner (`actix-api/src/sfu/affinity.rs:508-528`): nobody is ever
redirected, so the teardown path in #1 is never exercised and senders publish
normally.

---

## Part 1 — Root cause

### Evidence chain (from the run logs)

- `run.log`: `pod-0: 0`, `pod-1: 610`, `pod-2: 600` "owned by a different pod"
  → **pod-0 is the room owner** (owner never redirects). 1,210/1,500 listeners
  hit a non-owner and were redirected.
- `s.log` (10 senders, launched 20s *before* listeners): all 10
  `received ADMISSION_DECISION REDIRECT to rustlemania-webtransport-0`, but
  `"redirects_followed": 0`, **`joined_pod = 10.42.2.45`** (a *non-owner* pod),
  `tx_packets_enqueued: 2048`, `tx_drops_channel_full: 276315`. Per-sender
  timeline: session established + producers started @ 21:28:44.6, REDIRECT @
  21:28:44.7, then **silence until 21:35:40** (duration end, ~7 min later). No
  "Inbound consumer stopped", no "following ADMISSION_DECISION REDIRECT", no
  reconnect. The sender sat pinned to the non-owner pod, pumping media into a
  100-slot channel the stopped server actor never drained.
- `l1.log` (100 listeners): all 100 `received ADMISSION_DECISION REDIRECT to ...-0`,
  `joined_pod = 10.42.0.42`, `"redirects_followed": 0`, 0 video/0 audio decoded.
  Same hang shape.

### Why this produces 0 decode everywhere

`ChatServer::handle(ClientMessage)` only publishes a sender's media to NATS when
`connection_state == Active` (`actix-api/src/actors/chat_server.rs:1276-1289`).
A redirected/declined session is never activated, and (per defect #1) its actor
is stopped-but-not-dropped, so its inbound media is dropped on the floor at the
bot's egress channel. **Nothing is published to `room.{room}.{session}`**, so the
per-room dispatchers (`spawn_room_dispatcher`, subscribed to `room.{room}.*`,
`chat_server.rs:1125`,`2237`) have nothing to fan out. The forwarder, AllowSet
resolver, and the vc-72a receive-all cross-pod fallback are all correct and
irrelevant here — they never get a packet to decide on.

### Why senders end up on a non-owner pod at all

The senders are 10 clients in one bot pod hitting the headless Service
(`webtransport-headless...`), which load-balances them onto an arbitrary SFU pod
(here 10.42.2.45). The room jump-hashes to pod-0
(`affinity::jump_hash`/`is_owner`), so pod-0 is the only pod that admits without
redirect. Every sender on a non-owner pod is told to redirect to pod-0 — and then
defect #1 strands it.

### The decisive mechanism, precisely (file:line)

`WtChatSession::join_room` (`wt_chat_session.rs:422-463`): on JoinRoom-Err it does
`ctx.notify(StopSession)` (NOT `ctx.stop()`), deliberately, so the queued REDIRECT
`Message` flushes first (vc-883). `ctx.notify` enqueues `StopSession` on the
actor's *items* list, processed only after the *mailbox* is drained each poll. The
bridge unistream reader (`bridge.rs:145-167`) keeps `accept_uni`-ing the client's
media and `try_send`ing `WtInbound` → the actor mailbox is continuously non-empty
under the sender's 30fps + audio load, so the `StopSession` item is starved.
`ctx.stop()` is never reached → `outbound_tx` (`wt_chat_session.rs:86`) is never
dropped → writer `recv()` (`bridge.rs:205`) never returns `None` → writer task
never completes → `wait_for_disconnect`/`join_next` (`bridge.rs:132`,
`mod.rs:389`) never returns → `bridge.shutdown()` (`mod.rs:390`) never runs → the
reader `Session` clones are never aborted → quinn never closes the connection.

This is inherently load-dependent and multi-pod-only: it requires (a) a redirect
to fire (replicas≥2) and (b) the client to keep sending after the decline (senders
do; the producers were already started). Single-pod never redirects, so it never
trips.

---

## Part 2 — Why spill-admit isn't engaging (the 1,210 redirects)

`is_spilled_over` requires a **fresh** owner beacon with **owner_count > 180**
(`spillover.rs:147-151`). The beacon carries the owner pod's *local*
`member_count` (`health_beacon.rs:389-394`), normalized-subject ingested by every
pod's `spawn_spillover_ingest` on `room.*.system` (`spillover.rs:315-385`). The
plumbing is correct (subjects, normalization, freshness, ingest task all check
out), but the *input value never crosses 180*:

- The owner pod (pod-0) only counts members that successfully joined it. The 10
  senders were redirected to pod-0 but, per defect #1, hung on the non-owner pod
  and never re-joined pod-0. Most listeners were redirected away from non-owners
  and likewise hung instead of landing on pod-0. So pod-0's local count stays
  well under 180.
- First beacon fires at t=5s (`health_beacon.rs:341-358`, first tick consumed),
  and the 15s freshness window is fine — but with a sub-180 count it never trips.

So even the spillover gate is starved by defect #1. There is **also** a genuine
ramp-window weakness independent of #1: with a fast join wave, joiners that arrive
before the owner's count beacon exceeds 180 *and* propagates (≤ one 5s beacon
interval) are redirected; that is the expected early-arrival cohort and is not by
itself a bug. The headline 1,210 number is dominated by defect #1, not by the
window.

---

## Part 3 — Why admitted listeners have empty AllowSets / get no media

They do **not** have an AllowSet problem. The local-admit path (and the spill
fall-through, which uses the *same* machinery) materializes `RoomState`,
`insert_member`, the per-room dispatcher subscribed to `room.{room}.*`, and
registers the receiver synchronously before any await
(`chat_server.rs:1843-1976`). A listener that never sends a `SubscriptionUpdate`
resolves to the legacy-default "everyone" AllowSet
(`subscription.rs:301-319`), and the vc-72a receive-all fallback admits cross-pod
publishers that physically arrive over NATS but are not local members
(`forwarder.rs:381-473`, `subscription.rs:449-455`). All of this is correct.

The reason admitted/co-located listeners decode 0 is upstream: **there is no media
on NATS to fan out**, because the senders never published (Part 1). The
`SPILLOVER-RAMP-FINDINGS.md` "suspiciously low SFU CPU (~60m/pod)" is the same
fact observed from the other side — the pods were forwarding almost nothing
because almost nothing was being published.

(One pre-existing, *latent* correctness note for after the fix: when a sender is
on a different pod than the listener and the listener has an *explicit*
restrictive subscription with both receive-all flags false, the cross-pod sender
is not a local member and is not in the membership-bound AllowSet, so it is
dropped. Bots use the legacy/empty-update path → `(true,true)` → unaffected. Real
clients send `receive_all_*: true` on their opening flush → unaffected. Flag for a
follow-up only; it is not this bug.)

---

## Part 4 — Instrumentation design

Constraints honored: **off by default, opt-in, low overhead** on a 200+
participant real-time SFU. Reuse `actix-api/src/metrics.rs` (Prometheus) and the
existing `tracing` infrastructure. No new deps.

### 4.1 Decision points to instrument (the join→forward path)

| # | Decision point | File:line anchor | Record (decision + reason) |
|---|---|---|---|
| D1 | JoinRoom admit-local vs redirect vs spill | `chat_server.rs:1581-1647` | outcome ∈ {admit_local, spill_admit, redirect, reject}; reason: `spilled_over`, `owner_ordinal`, `self_ordinal`, `owner_count`/`owner_cpu` from the beacon snapshot |
| D2 | Admission cap (soft/hard) | `chat_server.rs:1718-1760` | outcome ∈ {admit, queued, rejected}; reason: `current`, `soft_cap`, `hard_cap` |
| D3 | Spillover predicate inputs | `spillover.rs:147-151` | on the *miss/false* branch only: `owner_count`, `owner_cpu`, `age_ms`, why-false (`stale`/`under_threshold`/`unknown_room`) |
| D4 | Session activation (Testing→Active) | `chat_server.rs:866-886` | so an operator can see whether a redirected session ever activated (it must not) |
| D5 | **Session teardown reached** (the bug-relevant one) | `wt_chat_session.rs:393-399` (StopSession handler) and the writer-end / `wait_for_disconnect`-return path `bridge.rs:132`,`240` | emit a counter when `ctx.stop()` actually runs and when the bridge writer ends, so a "redirect issued but teardown never reached" gap is directly observable |
| D6 | AllowSet resolve outcome | `forwarder.rs:381-473` | AllowSet size + whether a sender was admitted via membership vs receive-all fallback vs dropped (`unsubscribed`) |
| D7 | Forward vs drop (already partly wired) | `forwarder.rs:474-588` | already increments `sfu_dropped_total{reason}` / `sfu_forwarded_total{packet_type}` — extend reasons, see 4.2 |

### 4.2 Cheap always-on layer (O(1) Prometheus counters/gauges)

All additive, no per-packet string formatting, safe to leave on. Register in
`metrics.rs` next to the existing `SFU_*` block (`metrics.rs:300-424`):

- `sfu_join_decision_total{outcome}` — counter, outcome ∈
  `admit_local|spill_admit|redirect|reject|queued`. Inc once per JoinRoom at D1/D2.
- `sfu_spillover_state{room}` — gauge 0/1, set by the JoinRoom path from the
  beacon snapshot (or a periodic sweep) so operators see which rooms are spilled.
- `sfu_spillover_owner_count{room}` / `sfu_spillover_owner_cpu{room}` — gauges, the
  last beacon values the spill store holds (lets you see *why* spill did/didn't
  trip without verbose logs). Cardinality-bounded to active rooms.
- `sfu_session_teardown_total{reason}` — counter, reason ∈
  `redirect|client_close|region_redirect|stop_starved_watchdog`. Inc at D5. The
  `redirect` count MUST track the `sfu_join_decision_total{outcome=redirect}`
  count; a persistent gap *is* the defect-#1 signature and is now a
  one-PromQL-query alert.
- Extend `SFU_DROPPED_TOTAL` reasons (already `self_skip|unsubscribed|layer_budget|
  reference_miss|kfr_unsubscribed`, `metrics.rs:325-339`,`forwarder.rs`) with a
  pre-registered `non_member_no_receive_all` so the latent Part-3 case is visible.
- `sfu_allowset_size` — histogram, observed once per AllowSet *recompute* (cache
  miss only — `subscription.rs:285-296`), NOT per packet. Cache hits do nothing.

These are all single atomic incs/sets; the per-packet ones (`SFU_FORWARDED_TOTAL`,
`SFU_DROPPED_TOTAL`) already exist and are already on the hot path.

### 4.3 Opt-in targeted detailed layer (low overhead, room/session-scoped)

Gate verbose structured trace on a **cheap pre-check** so the hot path pays at
most one comparison when tracing is off.

**Gating mechanism (recommended): `SFU_TRACE_ROOM` env + a process-wide
`OnceLock<Option<String>>`,** mirroring the existing cached-env pattern in
`affinity.rs:124-194` (read once, no per-call `env::var`). Optionally also
`SFU_TRACE_SESSION` for a single session id.

- Pre-check helper (new, e.g. `sfu::trace::room_traced(room) -> bool`): compares
  the normalized room id against the cached target. One `&str` compare per
  decision point; returns instantly when unset.
- Only when `room_traced(room)` is true do we build and emit a structured
  `tracing::debug!(target: "sfu_trace", ...)` event at D1–D7 carrying the full
  reason payload (session, user, ordinal, owner_count, AllowSet members, drop
  reason, etc.). When unset, the closure is never entered → no formatting, no
  allocation.
- **Never** put an ungated `tracing::debug!` on the per-packet forward path. The
  per-packet D6/D7 verbose events must be inside `if room_traced(room)` AND
  further sampled (e.g. 1-in-N or first-K-per-(receiver,sender)) so even a traced
  room cannot emit at 1000pps × N receivers. Aggregate counters (4.2) carry the
  steady-state signal; the verbose layer is for a human debugging one room.
- Operator toggle without redeploy: pair the env default with a small admin
  endpoint (the metrics/diagnostics server already exists,
  `actix-api/src/bin/metrics_server.rs`) that writes the `OnceLock`'s
  `ArcSwap`/`RwLock`-backed target at runtime — `POST /debug/sfu-trace?room=<id>`.
  Justification for low overhead: the gate is a single relaxed atomic load + a
  short string compare; off-state cost is ~1ns and there is zero per-packet
  formatting unless an operator has explicitly armed a specific room.

Why a tracing *target* (`sfu_trace`) rather than only a level: lets ops enable it
via `RUST_LOG=sfu_trace=debug` cluster-wide *or* scope to one room via the env —
both reuse the existing subscriber with no new sink.

---

## Part 5 — Bead breakdown

### Bead A (PRIMARY fix, SFU) — "Force QUIC teardown on JoinRoom-Err redirect"
- **Problem:** `ctx.notify(StopSession)` is starved by inbound media; the session
  is never closed (`wt_chat_session.rs:422-463`).
- **Fix spec (one of, pick the minimal):**
  1. After flushing the REDIRECT, stop *reading* from the client immediately
     (set the `quit` flag the bridge readers observe, `bridge.rs:481`) so the
     reader loop exits, completing a `join_set` task → `wait_for_disconnect`
     returns → `shutdown` runs. The redirect bytes are already queued ahead of
     this on `outbound_tx`, so the writer-drain-grace (vc-s9e) still flushes them.
  2. Or proactively close the session server-side after a bounded grace
     (`session.close(...)`, available per `webtransport/mod.rs`) once the redirect
     is enqueued, instead of relying on actor-drop → writer-recv-None.
  3. Or escalate `StopSession` so it cannot be mailbox-starved (e.g. a deadline:
     if not stopped within the writer-drain-grace, force `ctx.stop()` from a
     timer), and stop forwarding inbound `WtInbound` once the decline is decided.
- **Acceptance:** in a replicas≥2 run, a redirected sender logs
  "following ADMISSION_DECISION REDIRECT" and `redirects_followed > 0`; senders
  land on the owner pod (`joined_pod` == owner) and publish; `tx_drops_channel_full`
  ≈ 0. Decode-verify run: spill/owner listeners decode video+audio, crc=0.
- **Responsiveness budget:** teardown must complete within the existing
  `WRITER_DRAIN_GRACE` (vc-s9e, `bridge.rs:259-261`) + one RTT; the redirect must
  still reliably reach the client (do not regress vc-883/vc-xnp).
- **Deps:** none. **Owner:** SFU (backend-rust-streaming). **Priority:** P0 — this
  is THE multi-pod blocker.
- **Validate against:** both WebTransport and WebSocket transports (the WS path
  has its own `ws_chat_session.rs` teardown — confirm it doesn't share the same
  starvation), reconnection, and graceful client-initiated disconnect (must not
  regress the vc-883 redirect-delivery ordering).

### Bead B (SECONDARY, SFU) — "Spillover beacon should reflect intended room size"
- Only meaningful AFTER Bead A (once senders/listeners actually land on the owner,
  the count climbs naturally). Re-run decode-verify after A; if the early-join
  wave still over-redirects, consider: count *redirected-to-owner-in-flight*
  joiners toward the beacon, lower the ramp's arrival rate, or seed spillover from
  a faster signal. **Do not build B before re-measuring with A in place** — A may
  fully resolve the redirect volume.
- **Acceptance:** past 180 admitted, non-owner pods admit locally (spill); redirect
  volume drops to the early-window cohort only.
- **Deps:** Bead A. **Owner:** SFU. **Priority:** P2 (re-measure first).

### Bead C (instrumentation, SFU) — "Opt-in SFU join/forward tracing + always-on decision counters"
- Implement Part 4: the `sfu_join_decision_total`, `sfu_session_teardown_total`,
  `sfu_spillover_*` gauges, the extended drop reason, and the `SFU_TRACE_ROOM`
  gated `sfu_trace` target with the cheap pre-check + sampling.
- **Acceptance:** with no env set, `/metrics` shows the new counters at 0 and
  there is no measurable change to `sfu_decide_latency_us`
  (`metrics.rs:358-363`); with `SFU_TRACE_ROOM=<room>` set, structured
  `sfu_trace` events appear ONLY for that room and per-packet events are sampled;
  a redirect-without-teardown gap is visible as
  `sfu_join_decision_total{outcome=redirect}` >> `sfu_session_teardown_total{reason=redirect}`.
- **Deps:** independent of A/B but lands the regression detector for A — schedule
  alongside A. **Owner:** SFU. **Priority:** P1.

### Bead D (load harness, bot) — "Decode-verify asserts senders published + redirects followed"
- The harness already collects `redirects_followed`; make the decode-verify
  pass/fail gate assert `sender redirects_followed > 0` (or `joined_pod == owner`)
  and `tx_drops_channel_full` below a threshold, so a future regression of Bead A
  fails the run instead of silently producing 0 decode.
- **Acceptance:** the script (`spillover-decode/decode-verify.sh`) fails fast if
  senders hang post-redirect. **Deps:** none (test-only). **Owner:** bot
  (load-test). **Priority:** P2.

---

## Code map (for the fix beads)

- Redirect decision: `actix-api/src/actors/chat_server.rs:1581-1647`
- Local/spill admit machinery: `chat_server.rs:1843-1976`
- NATS publish gate (Active-only): `chat_server.rs:1276-1289`
- Dispatcher subscribe `room.{room}.*` + fan-out: `chat_server.rs:1125`,`2221-2600`
- Forwarder + vc-72a receive-all fallback: `actix-api/src/sfu/forwarder.rs:295-588`
- AllowSet resolve / receive_mode: `actix-api/src/sfu/subscription.rs:243-455`
- Spillover store/ingest/threshold: `actix-api/src/sfu/spillover.rs:147-194`,`315-416`
- Beacon publish (local member_count): `actix-api/src/sfu/health_beacon.rs:341-400`
- Affinity / redirect target: `actix-api/src/sfu/affinity.rs:461-528`
- **Teardown (the bug):** `actix-api/src/actors/transports/wt_chat_session.rs:390-463`,
  `actix-api/src/webtransport/mod.rs:383-393`,
  `actix-api/src/webtransport/bridge.rs:131-241`
- JoinRoom-Err → stop decision: `actix-api/src/actors/session_logic.rs:521-543`
- Metrics module: `actix-api/src/metrics.rs:300-424`
