# Late-Joiner Integration Root Cause — post-Track-1 (audio mid-stream join)

Read-only architectural audit. Branch `experimental-sfu`, tip `2e82095`.
Scope: webinar shape (one big room), v1-BLOCKING. All registration fixes
(vc-knqr / vc-ud6o / vc-8txq) AND all of Track 1 (vc-c609 / vc-9u8e / vc-kcpg)
are LANDED. SFU 4 CPU, `SFU_FANOUT_WORKER_THREADS=4`.

Observed: CO-ARRIVAL 20p×400 → ~366/400 audio (good); SLOW-JOIN (waves of 50 /
~25 s) 20p×400 → ~188/400, 10p×200 → ~98/200. Late waves get ~0 audio. crc=0,
SFU 0 restarts.

---

## TL;DR — the decisive mechanism

**It is NOT a snapshot-refresh-cadence problem, NOT a registration problem, and
NOT an AllowSet/subscription gating problem.** All three were ruled out against
the live code (see §2). A late joiner *is* inserted into the dispatcher's
`receivers` map synchronously and *is* in the very next per-packet snapshot, and
its egress decision *is* `Forward` (receive-all audio).

The decisive mechanism is a **membership-generation-driven AllowSet cache-miss
storm that runs INSIDE the per-message fan-out barrier**, which pushes the
per-room dispatcher's drain rate below the aggregate publisher rate, at which
point **async-nats silently drops inbound media at the 16 Ki subscription buffer
and the vc-9eh silence watchdog never trips** (the subscription is not silent —
it is *lossy*). The reason **late joiners specifically get ~0** rather than a
uniform haircut is timing: each wave of 50 joins fires a synchronized
generation bump → an O(R) burst of cache misses (each O(R) to recompute) → an
O(R²) CPU spike on the fan-out workers **precisely during the wave**, exactly
when the new cohort needs its first packets. The dispatcher spends the wave
window recomputing AllowSets for the *existing* set under the barrier and drops
the inbound backlog; the new cohort's window passes having received nothing, and
once the room is in sustained partial-drop it never recovers.

Decisive code path:

1. Every new join bumps the **global** members-generation
   (`room_state.rs:462-463` → `rebuild_members_snapshot` bumps
   `members_generation` at `room_state.rs:349`).
2. The per-receiver AllowSet cache is keyed on that **global** generation
   (`subscription.rs:289-296`), so **one** join invalidates **every** receiver's
   cache entry at once.
3. The next MEDIA packet decided for each receiver is therefore a cache MISS →
   `resolve_inner` rebuilds a fresh `AllowSet` by iterating **all** current
   members (`subscription.rs:365-388`) → O(members) per receiver.
4. That recompute runs inside `Forwarder::decide`
   (`forwarder.rs:435` `resolve_cached`), which is called from
   `egress_decide_from_parsed` (`chat_server.rs:4124`), which runs **inside the
   barrier-bound fan-out shard tasks** (`chat_server.rs:3846`). The dispatcher
   loop **awaits all W shard handles before pulling the next inbound message**
   (`chat_server.rs:3873-3892`) — so the recompute storm directly throttles
   inbound drain.
5. When drain < publish rate, async-nats drops at the 16 Ki bounded
   subscription mpsc **silently** (`nats_connect.rs:117-167`,
   `.subscription_capacity(16 * 1024)` at `nats_connect.rs:196`). The dispatcher
   keeps draining what fits, so `last_msg_at` keeps advancing and the silence
   watchdog (`chat_server.rs:3443-3454`) never fires. No recovery, no restart,
   crc=0 (the bytes that DO get through are still correct).

---

## 1. Execution model after Track 1 (the load-bearing facts)

Track 1 fixed the single-core ceiling that the prior `PRESENTER-SCALING-…` and
`DELIVERY-SCALING-…` audits identified. Verified against current code:

- The per-room demux/fan-out tasks now run on a **dedicated process-wide
  multi-thread tokio runtime** (`fanout_runtime`, built in
  `ChatServerPool::new`, `chat_server.rs:1108-1116`), NOT on the per-shard
  actix Arbiter thread. Dispatchers are spawned with `fanout_handle.spawn(..)`
  (`chat_server.rs:3168`). So fan-out CPU is no longer pinned to one core.
- Media **publish** is off-actor (vc-ud6o): `Handler<ClientMessage>` only sees
  rare CONTROL packets now (`chat_server.rs:1879-1893`); media goes
  client-task → NATS directly. So the actor thread is not saturated by media.
- Rooms shard to actors by `jump_hash(room, n_shards)`
  (`chat_server.rs:1173-1176`); a single room is one actor on one thread, but
  that thread only does join/registration now, not fan-out.
- Egress fan-out is sharded W ways by `hash(SessionId) % W` over the snapshot
  (`chat_server.rs:3808-3870`), `W = runtime.num_workers()`
  (`chat_server.rs:3169`) = `SFU_FANOUT_WORKER_THREADS` = 4. The shard tasks are
  joined at a **barrier** before the next inbound message
  (`chat_server.rs:3873-3892`).
- Ingest is sharded K ways (vc-kcpg) into K dispatcher tasks per room
  (`spawn_room_dispatchers`, `chat_server.rs:3055-3088`), all sharing the SAME
  `receivers` map.

The **per-packet receiver snapshot is re-read live** from the shared
`receivers` map on every inbound message (`chat_server.rs:3737-3743`), and the
join handler inserts the new receiver **synchronously** into that exact map
(`chat_server.rs:2756`, with the explicit "do not move below the spawn"
ordering invariant at `chat_server.rs:2744-2755`). **There is no snapshot
staleness and no separate "register with dispatcher" tick.**

---

## 2. The three "obvious" hypotheses, ruled out against code

**(a) Stale/periodic snapshot of the receiver set — NO.** The snapshot is
collected per inbound message under a fresh read lock (`chat_server.rs:3737`).
A receiver inserted at `chat_server.rs:2756` appears in the next packet's
snapshot. The doc at `chat_server.rs:144-147` and the ordering invariant at
`2744-2755` confirm "delivery-eligible the instant it is in the map."

**(b) JoinRoom→receivers handshake starved/tick-gated — NO.** The whole
admission path (region pinning, ownership/spill redirect, member insert,
receivers insert) is **synchronous** in `Handler<JoinRoom>`
(`chat_server.rs:2042`…`2757`); the only async work (home-region KV lookup) is
spawned off and the join proceeds locally on cache miss
(`chat_server.rs:2191-2209`). Nothing gates the `receivers` insert behind a tick
or another message. The 8192 mailbox (vc-knqr, `chat_server.rs:1196-1198`) +
room-sharded actors (vc-8txq) mean the join is accepted promptly.

**(c) AllowSet / subscription gating for the late joiner as a RECEIVER — NO.**
Bots send no `SubscriptionUpdate`. A receiver with no per-receiver subscription
defaults to **receive-all**: `receive_mode` returns `(true, true)`
(`subscription.rs:554-560`). In `Forwarder::decide` the AUDIO admit test is
`allow.audio.contains(&sender) || recv_all_audio` (`forwarder.rs:469`), and
`recv_all_audio == true`, so **audio is admitted unconditionally** for a fresh
joiner — regardless of the speaker tick, generation, or any pending set. There
is no per-joiner audio-forward enrollment step that a late joiner can miss.
(The keyframe/`recent_t0` machinery at `forwarder.rs:200,627-636` is VIDEO/SCREEN
only — irrelevant to the audio=0 symptom, which is exactly why audio being ~0
points at raw forwarding, not state.)

So the late joiner is in the snapshot AND its decision is Forward. The only way
it gets ~0 audio is that **inbound media is being dropped before the dispatcher
fans it out** — i.e. throughput collapse, not integration logic. That is (a)
below.

---

## 3. The actual mechanism — generation-bump cache-miss storm under the barrier

### 3.1 One join invalidates every receiver's AllowSet

`insert_member` rebuilds the members snapshot and bumps the **global**
`members_generation` whenever the keyset changes (`room_state.rs:447-464` →
`rebuild_members_snapshot` → `members_generation.wrapping_add(1)` at
`room_state.rs:349`). The forwarder reads `(members_snapshot, members_generation)`
under one lock (`forwarder.rs:360`) and passes the generation into
`resolve_cached`, whose cache validity check is:

```
entry.sub_version == sub_version
  && entry.members_generation == members_generation   // GLOBAL
  && entry.speakers_generation == speakers_generation   // (subscription.rs:289-296)
```

Because the generation is **global to the room**, a single join makes EVERY
receiver's cached entry stale at once. The next MEDIA packet decided for any
receiver misses and runs `resolve_inner` (`subscription.rs:298-300`), which for
a receive-all receiver allocates a fresh `HashSet`/`HashMap` and inserts **every
current member** (`subscription.rs:365-388`) — O(members) work, with allocation.

### 3.2 The recompute runs inside the fan-out barrier

`resolve_cached` is called from `Forwarder::decide` (`forwarder.rs:435`), called
from `egress_decide_from_parsed` (`chat_server.rs:4124`), called inside each
spawned fan-out shard task (`chat_server.rs:3846`). The dispatcher's main loop
**awaits all shard handles** before pulling the next inbound message
(`chat_server.rs:3873-3892`, "BARRIER … message k+1 fan-out never starts before
message k's completes"). So the AllowSet recomputes are directly on the
inbound-drain critical path: while the workers recompute, the loop does not call
`sub.next()`.

After a wave of W_join joins, the first packet to each of the R receivers is a
miss → an O(R) burst of misses, each O(R) → **O(R²) recompute work** concentrated
in the wave window, executed inside the barrier on only W=4 worker threads
(shared with the K ingest dispatchers and every other room). At R≈400 that is
~160k member-insert operations + 400 allocations triggered per generation bump,
and a wave is 50 bumps in quick succession.

### 3.3 Throughput collapse → silent NATS drops → no recovery

When per-message barrier latency rises (recompute storm) the dispatcher drains
its NATS subscription slower than the N presenters publish into it. async-nats's
per-subscription mpsc is bounded at 16 Ki (`nats_connect.rs:196`); on overflow it
**silently drops** the message and fires only a connection-global
`Event::SlowConsumer(sid)` — it does NOT close the subscription
(`nats_connect.rs:117-167`). The dispatcher keeps draining whatever fit, so:

- `last_msg_at` keeps advancing on the `Some(msg)` arm (`chat_server.rs:3364`),
- the silence watchdog gate `silence < window` stays true and **never trips**
  the resubscribe (`chat_server.rs:3443-3454`) — the code comments at
  `nats_connect.rs:134-138` call out this exact blind spot.

There is no restart (crc verifies the bytes that DO arrive — hence crc=0) and no
self-heal. The room sits in steady-state partial loss.

### 3.4 Why CO-ARRIVAL avoids it

At T=0 all members join in a tight burst BEFORE media reaches full rate. The
generation churns up front and **then goes stable** — `members_generation` stops
changing once the room is full. The AllowSet cache warms once and **stays warm
for the entire steady-state soak** (every subsequent `decide` is the lock-free
`Arc::clone` fast path, `subscription.rs:289-294`). The barrier cost is then just
the cheap cached `decide` + `try_send`, which 4 workers sustain for ~366
receivers. There is no recurring recompute storm racing the live media stream.

### 3.5 Why MORE PRESENTERS makes it worse (10p/200=49% vs pre-fix 4p/200=97%)

Two compounding effects, both rooted in the same barrier:

1. **Inbound rate scales with presenters.** N presenters each publish
   audio+video continuously into the one merged subscriber set. Doubling
   presenters ~doubles the inbound packet rate the barrier-bound loop must
   drain, halving the headroom before the 16 Ki buffer overflows.
2. **Recompute cost per wave scales with members = presenters + listeners.**
   `resolve_inner` iterates ALL current members (`subscription.rs:365-388`), and
   presenters are members too. More presenters → larger `current_members` → each
   of the R cache-miss recomputes is more expensive (O(members) grows), so the
   per-wave O(R·members) spike is larger AND it must be absorbed against a
   higher inbound rate. The two multiply, so the late-wave window collapses
   faster with more presenters even at the same or smaller R.

---

## 4. Confounders explicitly checked and excluded

- **K ingest-shard mismatch for the listener** — listeners don't publish, so
  their ingest shard is irrelevant; all K dispatchers share the one `receivers`
  map and each snapshots the full set (`chat_server.rs:3737`, `2724`). Not it.
- **Receiver land on a different ChatServer shard than the dispatcher** — single
  room → single `jump_hash` shard (`chat_server.rs:1173`); the `receivers` Arc
  is consistent. Not it.
- **Per-session (downstream) mailbox full** — would also hit co-arrival
  receivers; late joiners' session mailboxes start empty. A contributing factor
  only once the room is already saturated, not the trigger.
- **recent_t0 / keyframe gate** — VIDEO/SCREEN only (`forwarder.rs:200`); cannot
  explain audio=0. Confirms the gap is forwarding-throughput, not state.
- **`SFU_ROOM_SIZE` gauge write per decide** (`forwarder.rs:357-359`) — a real
  per-packet cost under the room read lock, contributing to barrier latency, but
  secondary to the recompute storm.

---

## 5. Fix spec

The root fix is to **stop a join from invalidating the whole room's AllowSet
cache during full-rate media**, and to **stop silently black-holing inbound when
the dispatcher falls behind**. Both are needed; the first removes the trigger,
the second removes the unrecoverable failure mode.

### FIX 1 (primary) — make AllowSet cache invalidation incremental, not global

**Problem:** the cache key is the global `members_generation`
(`subscription.rs:289-296`), so one join busts all R entries.

**Spec:** decouple cache validity from the global generation for the
receive-all default path, which is membership-monotonic (a join only ADDS a
sender; a receive-all AllowSet is just "all members minus self"). Options, in
order of preference:

- **(1a) Incremental update for receive-all receivers.** When a member is
  added, for every receive-all receiver mutate the cached `AllowSet` in place
  (`audio.insert(sid)`, `video.insert(sid, default)`), and for a removal,
  `remove`. This is O(R) work per join done ONCE on the (low-rate) join path
  instead of O(R²) lazily on the (high-rate) media path. The cached entry stays
  valid; no media-path recompute. Non-default (slot/pinned/speaker) receivers
  keep the generation-keyed recompute (they are rare and bounded by
  MAX_VISIBLE_VIDEO).
- **(1b) Split the generation.** Track `members_add_generation` separately and
  let the receive-all resolve treat additions as cache-compatible (the new
  member is admitted by `recv_all_*` regardless), invalidating only on
  REMOVALS. Audio for receive-all never needs the membership set at all
  (`forwarder.rs:469` short-circuits on `recv_all_audio`), so the audio AllowSet
  for a receive-all receiver can be skipped entirely.
- Acceptance: with a sustained slow-join soak, `SFU_ALLOWSET_SIZE.observe`
  events (`subscription.rs:305`, fired only on recompute) must NOT spike per
  wave; `SFU_DECIDE_LATENCY_US` stays flat across waves; late waves decode audio
  > 0.

### FIX 2 (primary) — never silently black-hole inbound; bound by load, not silence

**Problem:** the watchdog only recovers a *silent* subscription
(`chat_server.rs:3443-3454`); a *lossy* (partial-drop) subscription advances
`last_msg_at` and never trips (`nats_connect.rs:134-138`).

**Spec:** add a **saturation-driven** recovery/relief independent of silence.
We already count drops (`SFU_DISPATCHER_INBOUND_DROPPED_TOTAL`,
`nats_connect.rs:145`) and inbound rate (`SFU_DISPATCHER_INBOUND_RATE`,
`chat_server.rs:3431`). Use a rising drop slope (not silence) to trigger relief:
either (a) raise `subscription_capacity` materially and/or move ingest to a
JetStream pull/ack consumer with explicit flow control, or (b) pipeline the
fan-out — drop the per-message barrier (`chat_server.rs:3873`) so inbound drain
is decoupled from fan-out completion (bounded in-flight per room). Decoupling
the barrier is the higher-leverage change: it lets the loop keep draining NATS
even while a recompute spike is in flight. Per-room ordering can be preserved
per-receiver via the existing W-shard partition (each receiver is handled by one
worker in `SessionId`-order).

- Acceptance: under a deliberate publisher storm, `…INBOUND_DROPPED_TOTAL` slope
  stays ~0 and base-listener receive counts (not just probes) stay near parity
  between co-arrival and slow-join.

### FIX 3 (relief) — drop the per-decide room-size gauge write

Move `SFU_ROOM_SIZE.set(...)` (`forwarder.rs:357-359`) off the per-packet
per-receiver path (sample it on the watchdog tick instead). Removes one
write-under-read-lock from every `decide`, lowering barrier latency. Secondary
but cheap.

### FIX 4 (defense) — surface the lossy-but-not-silent state in the soak harness

Have the bot summaries assert `SFU_DISPATCHER_INBOUND_DROPPED_TOTAL == 0` and
plot it against `SFU_DISPATCHER_INBOUND_RATE` per wave, so the saturation
condition is visible in CI rather than only as a decode deficit.

---

## 6. Bead breakdown (v1-relevant)

| Bead | Title | Owner | Priority | Acceptance |
|------|-------|-------|----------|-----------|
| **lj-1** | Incremental / receive-all-aware AllowSet cache (FIX 1) — stop global generation busting all R entries on each join | SFU | **P0** | slow-join 20p×400 audio parity with co-arrival (≥360/400); no per-wave `SFU_ALLOWSET_SIZE` recompute spike; `SFU_DECIDE_LATENCY_US` flat across waves |
| **lj-2** | Saturation-driven inbound relief: pipeline the fan-out (drop per-message barrier, bounded in-flight) OR JetStream flow-controlled ingest (FIX 2) | SFU | **P0** | `…INBOUND_DROPPED_TOTAL` slope ≈0 under storm; base-listener receive parity co-arrival vs slow-join |
| **lj-3** | Drop-slope-triggered recovery distinct from silence watchdog (FIX 2 complement) — recover lossy-not-silent subscriptions | SFU | **P1** | injected partial-drop heals without restart; metric proves trip |
| **lj-4** | Move `SFU_ROOM_SIZE` gauge off the per-decide hot path (FIX 3) | SFU | **P2** | `decide` latency drops at high R; gauge still tracks within one tick |
| **lj-5** | Soak harness: assert/plot `…INBOUND_DROPPED_TOTAL` per wave; base-listener receive summaries (FIX 4) | Bot | **P1** | reproduces the slow-join deficit and proves lj-1/lj-2 close it; gates CI |

Sequencing: **lj-5 first** (instrument so the fix is provable), then **lj-1 and
lj-2 in parallel** (independent: one removes the trigger, one removes the
unrecoverable failure mode — either alone improves, both together close the
gap), then lj-3/lj-4 as hardening.

---

## 7. Code citation index

- Live per-packet receiver snapshot: `chat_server.rs:3737-3743`
- Synchronous late-joiner `receivers` insert + ordering invariant:
  `chat_server.rs:2735-2757`
- Fan-out W-shard partition + barrier: `chat_server.rs:3808-3892`
- Fan-out runtime (separate multi-thread): `chat_server.rs:1108-1116`, spawn `:3168`
- `egress_decide_from_parsed` → `forwarder.decide`: `chat_server.rs:4124`
- Audio receive-all admit (unconditional for default joiner):
  `forwarder.rs:469`; `receive_mode` default `(true,true)`:
  `subscription.rs:554-560`
- AllowSet cache key = global `members_generation`: `subscription.rs:289-296`
- `resolve_inner` O(members) rebuild: `subscription.rs:365-388`
- `insert_member` bumps generation: `room_state.rs:447-464`, `:349`
- Bounded 16 Ki subscription + silent slow-consumer drop:
  `nats_connect.rs:117-167`, `:196`
- Silence-only watchdog (blind to lossy-not-silent): `chat_server.rs:3443-3454`,
  `:3364`
- Drop / inbound-rate metrics already present: `nats_connect.rs:145`,
  `chat_server.rs:3431`
- recent_t0 is VIDEO/SCREEN only (not audio): `forwarder.rs:200,627-636`
