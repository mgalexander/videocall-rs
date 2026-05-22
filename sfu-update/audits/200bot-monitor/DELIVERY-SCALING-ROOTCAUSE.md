# Delivery / subscription scaling root-cause — 10k egress soak

Date: 2026-05-21
Branch: `experimental-sfu` (tip ~`9e7bbfb`)
Scope: read-only code investigation. No code changed, no cluster commands run.

> **READ THE REFINEMENT FIRST.** A confirmation soak with the landed milestone
> instrumentation (vc-xow8) **relocated the primary root cause** from the media
> fan-out dispatcher (Rank 1 below) to an UPSTREAM **join-registration**
> bottleneck: the single `ChatServer` actix actor with a **default 16-deep
> mailbox**. Most listeners never register at all. See
> **"## REFINEMENT (2026-05-21) — the real bottleneck is join-registration, not fan-out"**
> at the end of this document. The original analysis below remains accurate for
> the media path but is now the SECOND bottleneck, not the first.

Inputs corroborated against `sfu-update/audits/200bot-monitor/soak-10k/FINDINGS.md`
and `metrics.csv` (CPU peaks ~960m at 2k–3k, then FLAT/declining through 10k;
mem ~796Mi; 0 restarts/panics; mid-stream probes decode 0 video AND 0 audio at
≥2k while the T=0 co-arrival probe decoded media).

---

## Topology (the load-bearing fact)

- 20 presenters each on their **own pods**. From the SFU-under-test pod they are
  **remote publishers**: their media arrives over NATS federation on
  `room.{room}.{sender_sid}` and is matched by the pod's per-room wildcard
  subscription `room.{room}.*` (`actix-api/src/models/mod.rs:44`).
- 10,000 listeners are **local members** on the single SFU pod. The bot sends
  **no `SubscriptionUpdate`** (confirmed: `grep` for subscription/`receive_all`/
  `pinned` in `bot/src/*.rs` returns nothing), so every listener is a
  **legacy-default receiver** → `receive_mode` returns `(true, true)`
  (`actix-api/src/sfu/subscription.rs:554-560`) and `resolve_inner` takes the
  legacy-default branch that admits all members + all remote publishers
  (`subscription.rs:345-388`).
- Listeners are NOT observers. `insert_member` hard-codes `is_observer: false`
  (`actix-api/src/sfu/room_state.rs:447-454`); the JoinRoom path never sets it
  true (the comment at `chat_server.rs:2261` confirms "`is_observer` is never set
  true"). So to reach 10k local members the soak must run
  `MAX_PARTICIPANTS_PER_ROOM` raised well above its 200 default
  (`actix-api/src/constants.rs:91`), otherwise listeners 201+ would be rejected
  at `chat_server.rs:1875-1920`.

This rules OUT the AllowSet / subscription-cap family as the cause: a bot
listener's AllowSet is never empty (legacy default admits everyone), so
`forwarder.decide` returns `Forward` for both audio and video
(`forwarder.rs:455-510`, `subscription.rs:365-388`). The 0-delivery is therefore
**upstream of, or independent of, the per-receiver forward decision** — it is in
the fan-out engine itself.

---

## 1. Root cause (decisive mechanism, ranked)

### RANK 1 — Single per-room dispatcher task is a single-core serial fan-out bottleneck; at ≳1–2k receivers it cannot drain its NATS subscription, async-nats silently drops inbound, and the room becomes a quiet black hole for everyone (DECISIVE)

There is exactly **one** dispatcher tokio task per room
(`spawn_room_dispatcher`, `actix-api/src/actors/chat_server.rs:2372`), spawned by
the first joiner (`chat_server.rs:2078-2098`). There is **no** per-receiver task
and no work-stealing — fan-out is a single `loop` on one task:

1. `sub.next()` pulls ONE message from the room's wildcard subscription
   (`chat_server.rs:2478`). The subscription's bounded channel is
   `subscription_capacity(16 * 1024)` (`actix-api/src/nats_connect.rs:170`).
2. Parse once (`chat_server.rs:2616`).
3. Snapshot the **entire** `receivers` map and iterate it serially, calling
   `egress_decide_from_parsed` + `recipient.try_send` for **every** receiver
   (`chat_server.rs:2800-2835`). Each `decide` takes the room read lock, refreshes
   a gauge, clones snapshots, and resolves/looks-up the AllowSet
   (`forwarder.rs:333-365`, `:417-432`).

This whole loop runs on a **single task = at most one core**. Per inbound media
packet the cost is O(N_receivers). At 10k receivers and ~20 senders trickling
even ~50 audio pps each (~1000 pps inbound), the required work is
~1000 × 10,000 = **1e7 decide+try_send ops/sec on one core** — physically
impossible. The loop falls permanently behind; the 16K subscription channel fills;
**async-nats does not block and does not close — it silently drops the message and
fires a connection-global `Event::SlowConsumer`** (documented verbatim in the
vc-9eh comment at `chat_server.rs:2411-2420`). `sub.next()` never returns `None`,
so the normal `None`-exit/respawn path never fires.

Why this produces **flat CPU** (the corroborating signature): one task saturates
at ≈1 core. `metrics.csv` shows CPU climbing to ~960m (≈1 core of 4) by 2k–3k
listeners and then staying flat / declining (903→965→959→…→672m) as listeners
grow 2k→10k. True N-fan-out would scale CPU with N; a single-task serial loop
cannot exceed one core, so CPU plateaus exactly where the dispatcher saturates —
**~1–2k receivers**. That is the "bounded work, not delivering to all" signature
FINDINGS.md describes.

Why a mid-stream joiner gets **EXACTLY 0** (not low): once the dispatcher is
permanently behind, the subscription is a black hole — inbound media is dropped at
the async-nats channel **before** the fan-out loop ever sees it. The vc-9eh
watchdog (`chat_server.rs:2491-2608`) detects the silence and resubscribes in
place, but **resubscribing does not help**: the consumer (the O(N) loop) is still
the bottleneck, so the fresh subscription's channel re-fills and re-drops. The
room oscillates between brief post-resubscribe bursts and black-hole silence. A
joiner that arrives at T=0 (co-arrival, room ~1k) lands while the dispatcher is
still under its saturation knee, so it decodes media (probe@1000: video=1297,
audio=6059). A joiner arriving at ≥2k lands after the knee — the dispatcher is
black-holing, so it (and in fact every receiver in the room) gets ~nothing; the
mid-stream probe simply observes the steady-state black hole as a clean 0.

Why audio is 0 too (the keyframe red herring is correctly excluded): audio rides
the exact same single dispatcher loop and the same dropped subscription. There is
no audio-specific path. 0 audio confirms the failure is the shared transport/
fan-out engine, not a video keyframe/reference issue.

Why it WORKED at 300 (single-pod-verify, 454,930 frames): 300 receivers × ~1000
pps = ~3e5 decide ops/sec — comfortably inside one core. The dispatcher keeps up,
the subscription channel never saturates, mid-stream joiners are served. The break
is between ~300 and ~2000 receivers, exactly where O(N)×pps crosses one core's
budget. This matches FINDINGS.md.

Decisive code path:
- `chat_server.rs:2372` `spawn_room_dispatcher` — one task per room.
- `chat_server.rs:2475-2493` single `loop`, single `sub.next()`.
- `chat_server.rs:2800-2835` serial O(N_receivers) fan-out (`try_send` per receiver).
- `chat_server.rs:2411-2420` documented async-nats silent-drop / SlowConsumer.
- `nats_connect.rs:170` 16K subscription channel that fills.

### RANK 2 — Receiver-actor mailbox saturation compounds the loss (CONTRIBUTING, not primary)

Even when the dispatcher does forward, delivery is `recipient.try_send`
(`chat_server.rs:2824`) into the per-session actor mailbox. actix's default
mailbox is bounded (16). Under the burst that follows each watchdog resubscribe,
the dispatcher dumps a backlog at every receiver at once; mailboxes overflow and
`try_send` drops with the "mailbox full" warning (`chat_server.rs:2828-2832`).
This degrades quality but, on its own, produces LOW not 0 — so it is a
contributor, not the primary mechanism. (The primary 0 comes from inbound being
dropped before fan-out, per Rank 1.)

### RANK 3 — Per-`decide` room read-lock contention amplifies the serial cost (CONTRIBUTING)

Every `decide` acquires the room `RwLock` in read mode and refreshes
`SFU_ROOM_SIZE` (`forwarder.rs:344-346`). With 10k serial `decide` calls per
packet plus the dispatcher's own per-message remote-publisher read/write
(`chat_server.rs:2692-2705`) and the 250ms watchdog read (`chat_server.rs:2510-
2523`), lock traffic on the single room lock further slows the one loop, lowering
the saturation knee. Secondary to Rank 1 but worth fixing alongside it.

### Confounder explicitly ruled out

"Senders are CPU-limited and produce too little, so the SFU only serves an early
subset." — REJECTED as the primary cause. If it were merely low produced rate, a
mid-stream probe would decode a *small* count, not a clean **0**, and the
co-arrival probe would not decode meaningfully more. The data shows a hard
co-arrival/mid-stream split (media vs. exactly 0) and a CPU plateau at ~1 core —
both signatures of single-task saturation + silent inbound drop, not low ingest.
Low produced rate only sets *how fast* the knee is reached, not *whether* there is
a knee.

---

## Fix spec

Goal: make per-room fan-out scale past one core and stop silently black-holing
inbound when a room is large.

### FIX A (primary) — shard the per-room fan-out across multiple tasks/cores

Split the single dispatcher's receiver iteration into K parallel fan-out workers
so the O(N) `decide`+`try_send` work is spread across cores while inbound is still
parsed once per message.

- Keep ONE subscription + ONE parse per message (preserve vc-q0v).
- After parse, partition the `receivers` snapshot into K shards (e.g. by
  `SessionId % K`, K ≈ available cores) and `tokio::spawn`/`join_all` the
  per-shard `decide`+`try_send` loops, OR hand each parsed message to K long-lived
  worker tasks via per-worker bounded channels. The parse task must never block on
  fan-out — it must return to `sub.next()` immediately so the subscription channel
  drains.
- Hook point: replace the serial loop at `chat_server.rs:2800-2835` with the
  sharded dispatch; receivers map sharding can live behind the existing
  `Arc<RwLock<HashMap<...>>>` (`chat_server.rs:2081`).
- Acceptance: at 10k receivers a mid-stream joiner decodes audio AND video; SFU
  CPU scales with receiver count (no longer pinned at ~1 core); per-room
  `sfu_room_receiver_set` size equals `sfu_room_members` (Deliverable 2 confirms).

### FIX B (decouple inbound from fan-out — required even with A)

Insert a bounded intake stage so the subscription is always drained promptly and
backpressure is explicit instead of silent:

- `sub.next()` → push parsed message onto a bounded `tokio::sync::mpsc` consumed by
  the fan-out worker(s). When the fan-out channel is full, drop **lowest-priority
  classes first** (reuse `priority_queue.rs` Class ordering) and increment a
  visible drop counter — never let the async-nats subscription channel be the thing
  that silently overflows.
- Hook point: between `chat_server.rs:2478` (`sub.next()`) and the fan-out at
  `:2800`.
- Acceptance: under sustained overload, `sfu_room_outbound_backlog` and a
  `sfu_fanout_dropped_total{class}` counter rise, but inbound is never
  black-holed (audio keeps flowing); the watchdog stops oscillating.

### FIX C (mailbox headroom) — raise per-session mailbox capacity and/or convert the egress to the existing `PrioritySender`

Route fan-out through the bounded `priority_queue.rs` `PrioritySender` per receiver
so audio (P1) is never starved by video (P3/P4) on a full mailbox, and raise the
session actor mailbox via `set_mailbox_capacity` in `ws_chat_session` /
`wt_chat_session` `started()`.
- Acceptance: under the post-resubscribe burst, P1 audio loss ≈ 0; only P4
  enhancement is shed.

### FIX D (lock relief) — drop the per-`decide` `SFU_ROOM_SIZE` gauge write

Move `SFU_ROOM_SIZE.set(...)` (`forwarder.rs:344-346`) out of the per-receiver
`decide` and onto the once-per-message dispatcher path (or the milestone hook from
Deliverable 2). Removes N redundant gauge writes + lock holds per packet.
- Acceptance: `decide` latency histogram (`SFU_DECIDE_LATENCY_US`) drops at high
  receiver counts; no semantic change.

Priority: A and B are the real fix and must ship together. C and D are
load-relief that raise the knee and are cheap.

---

## 2. Tunable join-milestone instrumentation (Deliverable 2)

Built specifically to confirm Rank 1 in the wild: it surfaces, at each milestone,
the dispatcher's actual **receiver-set size** next to the room **member count**.
When delivery breaks, these diverge (member_count climbs, but the count the
dispatcher actually fans out to / the rate it forwards stalls), and the
forwarded-vs-dropped counters at the crossing show whether inbound is being
black-holed.

### Config parameter

- New env var `SFU_JOIN_MILESTONES`, comma-separated ascending counts.
  Example: `SFU_JOIN_MILESTONES=10,50,100,250,500,1000,2000,4000,8000`.
- Default: OFF (empty / unset → no milestone work, zero hot-path cost). Documented
  default-on suggestion for soak runs:
  `10,50,100,250,500,1000,2000,4000,8000,10000`.
- Parsed ONCE at startup. Add a `milestones: Vec<usize>` (sorted, deduped) field to
  `SfuConfig` and parse it in `SfuConfig::from_env` alongside `SFU_MODE`
  (`actix-api/src/sfu/config.rs:43-57`). Invalid tokens are skipped with a single
  `warn!`, mirroring the existing `SFU_MODE` tolerance. Empty vec ⇒ feature off.
- Plumb `milestones` into `ChatServer` (it already snapshots `sfu_config`,
  `chat_server.rs:231`).

### Hook point (the milestone check)

In the JoinRoom handler, immediately AFTER the new member is registered in both the
member table and the dispatcher receiver set — i.e. right after
`self.joined_sessions.insert(session)` at **`chat_server.rs:2122`** (the receiver
insert is at `:2120`, member insert at `:1979`/`:2054`). At that point all the
"state that explains the plateau" is in scope under the handler.

```text
// after chat_server.rs:2122
let member_count = self.room_members.get(&room).map(|m| m.len()).unwrap_or(0);
let receiver_set = {                       // what the dispatcher fans out to
    let g = receivers_for_room.read().unwrap_or_else(|p| p.into_inner());
    g.len()
};
if crossed_milestone(prev_member_count, member_count, &self.sfu_config.milestones) {
    // O(1): only fires at a crossing, NOT per join.
    let allow_video = /* new joiner's AllowSet video.len() — resolve once here */;
    let allow_audio = /* … audio.len() */;
    let (fwd, dropped) = (SFU_FORWARD_TOTAL.get(), <sum of SFU_DROPPED_TOTAL>);
    crate::metrics::SFU_ROOM_MEMBERS.with_label_values(&[&room]).set(member_count as f64);
    crate::metrics::SFU_ROOM_RECEIVER_SET.with_label_values(&[&room]).set(receiver_set as f64);
    tracing::info!(
        target: "sfu_trace",
        event = "sfu_join_milestone",
        room = %room,
        milestone = member_count_milestone,
        member_count,
        receiver_set,                       // <-- diverges from member_count when broken
        new_joiner_allow_audio = allow_audio,
        new_joiner_allow_video = allow_video,
        sfu_forward_total = fwd,
        sfu_dropped_total = dropped,
        // optional, if cheap: per-room outbound backlog (Fix B's channel depth)
        "join milestone crossed"
    );
}
```

`prev_member_count` is `member_count - 1` for an admit (cheap to derive at the
hook). `crossed_milestone(prev, now, &ms)` returns the milestone value iff some
`m` in `ms` satisfies `prev < m <= now`. This is O(milestones) at a crossing only —
never per join.

### Marker fields (the diagnostic payload)

One structured `tracing` event `sfu_join_milestone` on the existing `sfu_trace`
target (reuses vc-8wd plumbing; `actix-api/src/sfu/trace.rs`). Fields:

- `room` — room id.
- `milestone` — the threshold crossed.
- `member_count` — `room_members.len()` (authoritative membership).
- `receiver_set` — `room_dispatch[room].receivers.len()` (what the dispatcher
  actually fans out to). **Divergence from `member_count` is the smoking gun.**
- `new_joiner_allow_audio`, `new_joiner_allow_video` — the new joiner's resolved
  AllowSet sizes (empty ⇒ subscription bug; full ⇒ confirms the bug is in the
  fan-out engine, not the AllowSet).
- `sfu_forward_total`, `sfu_dropped_total` — forwarded-vs-dropped at the crossing
  (forward rate flatlining while members climb ⇒ confirms inbound black-hole).
- `outbound_backlog` (optional, only after Fix B exists) — per-room intake channel
  depth.

### Counter / gauge names (align with vc-8wd)

- `sfu_room_members{room}` — `GaugeVec`, set at each milestone crossing.
- `sfu_room_receiver_set{room}` — `GaugeVec`, set at each milestone crossing. New.
- `sfu_join_milestone` — `tracing` event on `sfu_trace` (no new metric needed; it
  is the structured marker).
- Reuse existing `sfu_forward_total` (`metrics.rs:556`) and `sfu_dropped_total`
  (`metrics.rs:325`) for the forwarded/dropped fields.
- (Fix B introduces `sfu_fanout_dropped_total{class}` and
  `sfu_room_outbound_backlog{room}` — referenced here so the milestone marker can
  print them once they exist.)

Register the two new `GaugeVec`s in `actix-api/src/metrics.rs` next to
`SFU_ROOM_SIZE` (`metrics.rs:347`).

Cost: parse once at startup; at runtime a single relaxed branch per join
(`milestones.is_empty()` short-circuit) plus O(milestones) only on an actual
crossing. No per-packet cost.

---

## 3. Bead breakdown

### Bead 1 — Shard per-room fan-out across cores (Fix A) — SFU
- Priority: P0 (this is THE delivery defect).
- Scope: `chat_server.rs:2372` `spawn_room_dispatcher`; replace the serial
  `:2800-2835` loop with K-way sharded `decide`+`try_send`; preserve single
  parse (vc-q0v).
- Acceptance: a 10k-listener single-room soak; a mid-stream probe joining at 2k,
  5k, 10k decodes audio AND video (>0); SFU CPU rises with receiver count (no
  flat-at-~1-core plateau); `sfu_room_receiver_set == sfu_room_members` at every
  milestone.

### Bead 2 — Bounded intake stage; never silently black-hole inbound (Fix B) — SFU
- Priority: P0 (ships WITH Bead 1).
- Scope: bounded mpsc between `sub.next()` (`chat_server.rs:2478`) and fan-out
  (`:2800`); priority-class shedding on overflow; `sfu_fanout_dropped_total{class}`
  + `sfu_room_outbound_backlog{room}`.
- Acceptance: under sustained overload audio (P1) keeps flowing to all receivers
  (probe audio > 0 at 10k); the vc-9eh watchdog no longer oscillates
  (resubscribe-in-place WARN rate ≈ 0 in steady overload); drop counters rise
  visibly instead of silent loss.

### Bead 3 — Egress via PrioritySender + larger session mailbox (Fix C) — SFU
- Priority: P1.
- Scope: route per-receiver egress through `priority_queue.rs` `PrioritySender`;
  `set_mailbox_capacity` in `ws_chat_session`/`wt_chat_session` `started()`.
- Acceptance: post-burst P1 audio loss ≈ 0; only P4 enhancement shed under
  pressure; no "mailbox full" warnings for audio.

### Bead 4 — Remove per-decide gauge write / lock relief (Fix D) — SFU
- Priority: P2.
- Scope: move `SFU_ROOM_SIZE.set` out of `decide` (`forwarder.rs:344-346`) to the
  once-per-message dispatcher / milestone hook.
- Acceptance: `SFU_DECIDE_LATENCY_US` p99 drops at ≥2k receivers; forwarding
  behavior unchanged (parity tests green).

### Bead 5 — Tunable join-milestone instrumentation (Deliverable 2) — SFU
- Priority: P1 (do EARLY — it confirms Beads 1–2 fixed the plateau, and confirms
  the root cause in the wild before/independent of the fix).
- Scope: `SFU_JOIN_MILESTONES` parsed in `SfuConfig::from_env`
  (`config.rs:43-57`); milestone hook after `chat_server.rs:2122`;
  `sfu_room_members` + `sfu_room_receiver_set` GaugeVecs (`metrics.rs:347`);
  `sfu_join_milestone` `sfu_trace` event with the fields above.
- Acceptance: with the env set, crossing each milestone emits exactly ONE
  `sfu_join_milestone` event carrying `member_count`, `receiver_set`,
  new-joiner AllowSet sizes, and forward/dropped counters; default-off ⇒ no events
  and no per-join cost; in a pre-fix soak the event SHOWS `receiver_set` tracking
  `member_count` upward while `sfu_forward_total` rate flatlines past ~1–2k
  (i.e. it reproduces and pinpoints the Rank-1 plateau).

### Bead 6 — Soak harness: collect base-listener receive/CRC summaries — Bot
- Priority: P2 (closes the FINDINGS.md "base receive data not collected" gap).
- Scope: `soak-10k/soak10k.sh` — collect post-completion base-listener summaries,
  not just probes + metrics, so the base cohort's delivery is measured at each
  step (confirms the plateau hits ALL receivers, not just the decode probes).
- Acceptance: a soak run emits per-step base-listener received-frame/CRC summaries
  alongside the existing probe logs.

---

## Code citations (index)

- One dispatcher per room: `actix-api/src/actors/chat_server.rs:2372`,
  spawned at `:2078-2098`.
- Serial O(N) fan-out loop: `chat_server.rs:2800-2835`.
- Single `sub.next()` / parse-once: `chat_server.rs:2478`, `:2616`.
- async-nats silent-drop / SlowConsumer (documented): `chat_server.rs:2411-2420`.
- Watchdog resubscribe-in-place: `chat_server.rs:2491-2608`.
- NATS subscription channel cap (16K): `actix-api/src/nats_connect.rs:170`.
- `try_send` per receiver + mailbox-full drop: `chat_server.rs:2824-2832`.
- Receiver insert on join (all joiners): `chat_server.rs:2120-2122`.
- Hard cap default 200 / env override: `actix-api/src/constants.rs:91-94`,
  applied at `chat_server.rs:1862-1920`.
- `is_observer` always false: `room_state.rs:447-454`; never set true
  (`chat_server.rs:2261`).
- Legacy-default AllowSet (admits all members + remote pubs):
  `subscription.rs:345-388`; `receive_mode` `(true,true)` for no-update:
  `subscription.rs:554-560`.
- forwarder per-receiver decide (audio/video allow gate): `forwarder.rs:297-651`,
  audio gate `:456`, video gate `:460-510`.
- Remote-publisher registry (cap 32 / TTL 10s — not the cap):
  `room_state.rs:53-82`, ingest `chat_server.rs:2651-2710`.
- Per-decide gauge write (lock relief target): `forwarder.rs:344-346`.
- Trace plumbing to reuse: `actix-api/src/sfu/trace.rs`; existing metrics
  `metrics.rs:347` (`SFU_ROOM_SIZE`), `:556` (`SFU_FORWARD_TOTAL`),
  `:325` (`SFU_DROPPED_TOTAL`).
- SFU config parse point for the new env var: `actix-api/src/sfu/config.rs:43-57`.
- Corroboration: `soak-10k/FINDINGS.md`, `soak-10k/metrics.csv` (CPU plateau ~960m).

---

## REFINEMENT (2026-05-21) — the real bottleneck is join-registration, not fan-out

New evidence: confirmation soak with the landed milestone instrumentation
(vc-xow8). 20 presenters + lightweight (`--listener-no-decode`) base growing to
3,000 listeners, single SFU pod 4CPU, `replicas=1`,
`SFU_JOIN_MILESTONES` default-on (10,50,100,250,500,1000,2000,...).

Observed:
- **ZERO `sfu_join_milestone` events fired** — not even the `1000` clip.
- Only **~360 distinct session/user-ids** appear in the SFU logs (of 3,000+ bots).
- Probe decode plateau reproduced exactly: probe@1000 (co-arrival) decoded;
  probe@2000 / @3000 (mid-stream) = 0.
- SFU stable: 0 restarts, 0 panics, CPU ~870–921m (still ~1 core).

### What the marker proves by NOT firing

The milestone marker fires when `room_members[room].len()` crosses a clip; the
call site reads `self.room_members.get(&room).map(|m| m.len())`
(milestone hook landed near `chat_server.rs:2304`; the same map is written on
admit at `chat_server.rs:1979`). Zero firings — including the `1000` clip —
proves **`room_members` never reached 1000 even though 3,000 listeners
established QUIC sessions.** ~360 distinct ids in the logs corroborates: the SFU
registered only a few hundred sessions. The bottleneck is therefore **upstream of
media fan-out** — at JOIN/registration. This RELOCATES the primary cause.

### Answers to the four questions

#### 1. Why does `room_members` stay < 1000 when 3,000 listeners connect+JoinRoom? — YES, the single `ChatServer` actor mailbox is the serialization point.

There is exactly ONE `ChatServer` actor instance for the whole pod, started with
the **default actix `Context`**:
- `ChatServer::new(nats_client).await.start()` —
  `actix-api/src/bin/webtransport_server.rs:169` and
  `actix-api/src/bin/websocket_server.rs:334`.
- `impl Actor for ChatServer { type Context = Context<Self>; }` —
  `chat_server.rs:827-829`. **`started()` is NOT overridden**, so there is **no
  `set_mailbox_capacity` call**. (The only `set_mailbox_capacity(4096)` in the
  tree is in a UNIT TEST actor — `wt_chat_session.rs:838` — not production.)
- actix's default mailbox capacity is **16**. So the entire pod's control plane is
  a single-threaded actor draining a **16-deep mailbox**.

Every per-session control + data message funnels through that ONE mailbox and is
processed serially on one actor task:
- `Connect` — `chat_server.rs:831`, sent via `.send()` from
  `wt_chat_session.rs:265` (`.wait(ctx)`).
- `JoinRoom` — `chat_server.rs:1605`, sent via **`.send()`** (a request expecting
  a `MessageResult` reply) from `wt_chat_session.rs:456` (`join_room`),
  `.wait(ctx)` at `:540`. This is a **bounded, awaited** send: it cannot complete
  until the actor dequeues and handles it.
- `ClientMessage` — `chat_server.rs:1479` — sent via **`do_send`** from
  `wt_chat_session.rs:446` / `ws_chat_session.rs:342` on **every inbound client
  packet** (RTT probes, heartbeats, diagnostics, and sender media). `do_send`
  bypasses the capacity *backpressure* but still **enqueues into the same single
  mailbox**, so it competes for the same single actor thread.
- Plus `ActivateConnection` (`:1026`), `Disconnect` (`:841`),
  `RoomDispatcherExited` (`:1237`), `HomeRegionResolved` (`:1347`), etc.

Under a burst of ~1000 joins/step, ~1000 sessions each issue a `Connect` then a
`JoinRoom` `.send()`, while every already-connected session floods the same
mailbox with `do_send(ClientMessage)` (RTT/heartbeat are periodic; a 200ms RTT
cadence × hundreds of sessions alone is hundreds of msgs/sec). The single actor
thread processes them serially. The 16-slot capacity means new `JoinRoom`
`.send()` futures back up (a bounded `send` to a full mailbox does not resolve
until a slot frees), and the actor spends its single thread interleaving a flood
of `ClientMessage`/RTT/heartbeat work with the slow JoinRoom handler — which is
itself heavy (cap accounting, NATS subject build, `room_states`/`subscriptions`/
`speaker_ticks`/`forwarders` materialisation, dispatcher spawn, receiver insert,
a spawned post-join task; `chat_server.rs:1860-2232`). The result is that only a
few hundred JoinRooms ever drain before the session-side deadlines/teardown fire.
`room_members` therefore stalls at a few hundred — exactly the ~360 observed —
and the milestone marker (gated on `room_members` ≥ 1000) never fires.

The "flat ~1 core CPU" signature is now over-determined: BOTH the single
ChatServer actor (join-registration) AND the single per-room dispatcher (fan-out)
are single-task/single-core. The join-registration limiter is hit first
(few-hundred sessions register), so the dispatcher never even gets a large
`receivers` set to choke on — the original Rank-1 fan-out limit is real but is
**masked** because we never reach the receiver counts that trigger it.

#### 2. Are sessions dropped/timed-out before registration? — YES, and they are NOT registered in any other structure that the dispatcher would see.

- `Connect` (`chat_server.rs:834-838`) inserts the session into `self.sessions`
  and `self.connection_states` (Testing). That is the ONLY thing that happens
  pre-JoinRoom. It does NOT make the session a room member and does NOT add it to
  the per-room dispatcher `receivers` map. So a session stuck in the mailbox queue
  after `Connect` but before its `JoinRoom` is handled is invisible to BOTH the
  marker (`room_members`) AND the dispatcher (`room_dispatch[room].receivers`,
  inserted only inside the JoinRoom handler at `chat_server.rs:2120`).
- Teardown-before-registration: the WT heartbeat watchdog uses
  `CLIENT_TIMEOUT = 30s` (`constants.rs:26`, `wt_chat_session.rs:53,204-206`) and
  is deliberately started AFTER Connect/JoinRoom (`wt_chat_session.rs:282`,
  "avoid premature timeout if Connect/JoinRoom are slow under load"). 30s is a
  large budget, so straightforward heartbeat timeout is unlikely to be the main
  reaper. The dominant effect is simpler: under the mailbox pile-up the
  `JoinRoom` `.send()` futures resolve very slowly (or the bot/connection gives up
  / the QUIC handshake backlog stalls), so most sessions **never get their
  JoinRoom handled at all** — they are not "dropped after registration", they are
  **never registered**. Either way the membership counter stays low.
- Net: there is no third structure holding the missing ~2,640 listeners. They are
  in flight (QUIC up, maybe `sessions`/`connection_states` populated) but never
  reach `room_members` / `room_dispatch.receivers`. Both the marker and the
  dispatcher correctly "miss" them because neither structure was ever written.

#### 3. Does Fix A (shard fan-out) address this? — NO. A SEPARATE fix is required for the join-registration path. vc-ypx3 must SPLIT.

Fix A (shard the per-room dispatcher fan-out) and Fix B (bounded intake) operate
ENTIRELY DOWNSTREAM of the `ChatServer` actor — they only run once a session is
already a member in `room_dispatch.receivers`. They do nothing for sessions
stuck in the actor mailbox before registration. With the join-registration
limiter in place, sharding fan-out has no receivers to shard.

Required new fix (call it **Fix E — de-serialize join-registration**), ranked
above A/B because it is hit first:

- **E1 (cheapest, do first): raise the `ChatServer` mailbox capacity.** Override
  `Actor::started` for `ChatServer` (`chat_server.rs:827`) to call
  `ctx.set_mailbox_capacity(N)` with a large N (e.g. 8192–32768). This removes the
  16-slot head-of-line backpressure so `JoinRoom` `.send()` futures stop stalling
  behind a full mailbox. NOTE: this alone does not add parallelism — the single
  actor thread is still serial — but it stops bounded-send stalls and lets the
  backlog drain. Likely lifts the few-hundred plateau substantially on its own.
- **E2 (the structural fix): move JoinRoom registration off the single actor's
  hot path.** The membership write (`room_members` insert, `room_states`
  `insert_member`, `room_dispatch.receivers` insert) is the load-bearing part and
  is cheap; the heavy/awaitable parts (NATS subject setup, beacon registration,
  the spawned post-join task) can be deferred. Options:
  - Make `JoinRoom` a `do_send` (fire-and-forget) instead of a bounded `.send()`
    request (`wt_chat_session.rs:456`), with the Ok/redirect/reject decision
    delivered via the existing recipient `Message` channel (the redirect/reject
    packets already travel that way — `chat_server.rs:1755,1897`). Removes the
    awaited round-trip that serializes joins against the session `started()`.
  - And/or shard the control plane: run multiple `ChatServer` actors keyed by room
    (jump-hash, mirroring the existing per-room ownership at
    `chat_server.rs:2030` `affinity::is_owner`) so join-registration scales across
    cores instead of one actor thread.
- **E3 (relieve the competing load): stop routing high-rate per-packet
  `ClientMessage` through the same actor mailbox.** Today every inbound client
  packet is `do_send(ClientMessage)` to the single actor
  (`wt_chat_session.rs:446` → `chat_server.rs:1479`), which then publishes to NATS.
  This per-packet traffic is what starves JoinRoom. Move the NATS publish for
  Active sessions OFF the actor — publish directly from the session/bridge task
  (the actor only needs the connection-state gate, which can be a shared atomic),
  so the actor mailbox carries only low-rate lifecycle messages.

Re-scope of the held bead **vc-ypx3** (currently "fan-out shard + bounded
intake"): it should be **SPLIT**, because it currently covers only the
downstream (Fix A/B) path and would NOT fix the observed plateau:
- Keep vc-ypx3 = Fix A + Fix B (media fan-out shard + bounded intake), but mark it
  **second in line** — it only matters once join-registration is fixed and rooms
  actually reach large receiver counts.
- File a NEW, HIGHER-priority bead (**vc-NEW: de-serialize join-registration** =
  Fix E1+E2+E3) and gate vc-ypx3 behind it. E1 is a one-line-ish change with high
  leverage and should ship first.

#### 4. Is `room_members` the right counter to drive the markers? — NO. Make the markers fire on a REGISTRATION/INTAKE counter so they fire even when join-registration is the bottleneck.

The markers' silence WAS diagnostic this time (it proved the registration
plateau), but a marker that can only fire after the very step that is broken is a
poor steady-state instrument. Refine the milestone instrumentation so it surfaces
the registration bottleneck directly:

- **Drive the milestone crossing off a CONNECT/intake counter, not membership.**
  Add a process-wide `sfu_sessions_connected_total` (incremented in the `Connect`
  handler, `chat_server.rs:834`) and/or a `sfu_join_attempts_total` (incremented
  at the TOP of the `JoinRoom` handler, `chat_server.rs:1619`, before any
  early-return). Cross milestones on the attempt/connect counter so the marker
  fires as load arrives — independent of whether registration succeeds.
- **Emit the divergence in the marker payload.** At each crossing log all three:
  `connected` (sessions), `join_attempts`, `members` (`room_members.len()`), and
  `receiver_set` (`room_dispatch[room].receivers.len()`). When join-registration
  is the bottleneck you SEE `connected` >> `members` ≈ `receiver_set`. When the
  fan-out is the bottleneck (post-Fix-E) you SEE `members` ≈ `receiver_set`
  climbing while `sfu_forward_total` rate flatlines. One marker now distinguishes
  the two failure modes.
- **Add a mailbox-depth gauge for the single actor.** Even approximate
  (e.g. a counter of in-flight JoinRooms: increment at handler entry, decrement at
  return) exposes the head-of-line stall directly. Name: `sfu_chatserver_inflight`
  or `sfu_join_inflight`.
- Keep the existing `sfu_room_members` / `sfu_room_receiver_set` gauges (they are
  still the right "did delivery scale" signal once registration is fixed); just
  ADD the connect/attempt counters as the marker TRIGGER.

Concrete hook points:
- `sfu_sessions_connected_total.inc()` in `Connect::handle`
  (`chat_server.rs:834-838`).
- `sfu_join_attempts_total.inc()` + `sfu_join_inflight` increment at
  `chat_server.rs:1619` (JoinRoom handler entry); decrement on each return path.
- Milestone crossing check: relocate from the post-registration site
  (`~chat_server.rs:2304`) to fire on `sfu_join_attempts_total` so it triggers
  under load regardless of registration success; include `members` /
  `receiver_set` / `connected` in the payload for the divergence read.

### Updated rank ordering (post-refinement)

1. **Fix E — de-serialize join-registration (single `ChatServer` actor, 16-deep
   mailbox).** PRIMARY. Hit at a few-hundred sessions; this is why `room_members`
   < 1000 at 3,000 connected. Citations: `chat_server.rs:827-829` (default
   Context, no mailbox override), `bin/webtransport_server.rs:169` /
   `bin/websocket_server.rs:334` (`.start()`), JoinRoom bounded `.send()`
   `wt_chat_session.rs:456,540`, per-packet `do_send(ClientMessage)`
   `wt_chat_session.rs:446` → `chat_server.rs:1479`.
2. **Fix A + B — shard fan-out + bounded intake (per-room dispatcher).** SECOND.
   Real, but currently MASKED — never reached because registration caps the room
   at a few hundred receivers. Becomes the active limiter only after Fix E lets
   rooms grow past ~1–2k.
3. **Fix C / D — mailbox headroom for session actors / per-decide lock relief.**
   Load-relief, unchanged.

### Bead re-scope (supersedes the Deliverable-3 list above for prioritisation)

- **vc-NEW (Fix E) — de-serialize join-registration — SFU — P0 (do FIRST).**
  - E1: `ChatServer::started` override calling `set_mailbox_capacity(>=8192)`
    (`chat_server.rs:827`).
  - E2: JoinRoom as `do_send` with result via recipient channel, and/or per-room
    sharded `ChatServer` actors (jump-hash, mirror `affinity::is_owner`
    `chat_server.rs:2030`).
  - E3: move per-packet NATS publish off the single actor (publish from the
    session/bridge task; actor keeps only the lifecycle messages).
  - Acceptance: at 3,000 connected listeners, `room_members` reaches 3,000 and the
    `sfu_join_milestone` markers fire through the 2000 clip; `sfu_join_inflight`
    stays bounded; no JoinRoom `.send()` stalls; ~3,000 distinct ids in logs.
- **vc-ypx3 (Fix A + B) — fan-out shard + bounded intake — SFU — P1, GATED behind
  vc-NEW.** Keep as filed but HOLD until Fix E lands; only then can a single room
  reach the receiver counts that trigger the dispatcher limit. SPLIT note: do NOT
  fold join-registration into vc-ypx3 — it is a distinct actor-mailbox defect.
- **vc-xow8 follow-up (milestone marker re-trigger) — SFU — P1.** Drive markers
  off `sfu_join_attempts_total` / `sfu_sessions_connected_total` (not
  `room_members`); add `sfu_join_inflight` gauge; include
  connected/attempts/members/receiver_set in the payload. Hook points as in answer
  4. Acceptance: with the registration bottleneck present, markers STILL fire on
  the connect/attempt counter and the payload shows `connected >> members`,
  pinpointing the registration plateau without relying on the broken step.

### Refinement code citations (index)

- Single `ChatServer` actor, default Context, NO mailbox override:
  `chat_server.rs:827-829`; started via `.start()` at
  `bin/webtransport_server.rs:169`, `bin/websocket_server.rs:334`.
- JoinRoom is a bounded awaited `.send()`: `wt_chat_session.rs:456` (`join_room`),
  `.wait(ctx)` `:540`; handler `chat_server.rs:1605`, attempt entry `:1619`.
- Connect handler (only pre-join state; not membership, not receivers):
  `chat_server.rs:831-838`; sent `wt_chat_session.rs:265`.
- Per-packet `do_send(ClientMessage)` floods the same mailbox:
  `wt_chat_session.rs:446`, `ws_chat_session.rs:342`; handler `chat_server.rs:1479`.
- Membership write is INSIDE the (serialized) JoinRoom handler: `room_members`
  `chat_server.rs:1979`; `room_states.insert_member` `:2054`;
  `room_dispatch.receivers` insert `:2120`.
- Milestone marker call site reading `room_members` (the counter that never
  crossed): `~chat_server.rs:2304` (vc-xow8 landed hook); `room_members` read
  pattern `self.room_members.get(&room).map(|m| m.len())`.
- Heartbeat/timeout: `CLIENT_TIMEOUT=30s` `constants.rs:26`;
  `wt_chat_session.rs:53,204-206,282`.
- Set-mailbox-capacity exists only in a TEST actor (not prod):
  `wt_chat_session.rs:838`.
- Corroboration: vc-xow8 confirmation soak — 0 `sfu_join_milestone` events at
  3,000 connected, ~360 distinct ids in SFU logs, probe plateau reproduced,
  CPU ~870–921m.
