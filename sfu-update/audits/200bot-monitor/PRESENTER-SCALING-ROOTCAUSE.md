# Presenter-Scaling Root Cause — Late-Joiner Delivery Failure

Read-only architectural audit. Branch `experimental-sfu`. SFU single pod 4 CPU / 4 Gi.
Scope: webinar shape (many users in ONE room), v1-BLOCKING late-joiner correctness.

---

## TL;DR — Direct answers

### Q1: Is the PRESENTER side an architectural scaling limit? — **YES.**

For a **single room** (the webinar shape), the entire forwarding path — NATS ingest of all
presenters AND the O(receivers) egress fan-out — runs on **one tokio task pinned to one
current-thread runtime**, which is the **same single thread** that runs that room's
`ChatServer` actor mailbox (join/registration). Per-room sharding across arbiters
(`vc-8txq`) shards by *room*, so a single big room cannot use more than one core for its
hot path no matter how many CPUs the pod has. The work on that one thread grows as
**O(presenters × receivers)** while only one core is available to do it. This is a hard
single-core ceiling, not a tuning problem. It is *fixable within one pod* only by
parallelizing the fan-out and ingest (vc-ypx3 sharding + audio mixdown + publish-side
filtering) — see §4. Today, as written, it is an architectural limit.

### Q2: What is the bottleneck that gets WORSE with more presenters?

The **single per-room dispatcher task** in `spawn_room_dispatcher`
(`actix-api/src/actors/chat_server.rs:2900`), specifically its **serial, synchronous
per-receiver fan-out loop** at **`chat_server.rs:3341-3363`**, fed by a **single NATS
wildcard subscription** (`room.<room>.*`, subject built at
`actix-api/src/models/mod.rs:51-53`, subscribed at `chat_server.rs:2916`).

- Inbound packet **rate** into that one task scales with **presenters** (each presenter
  publishes audio+video continuously).
- For **each** inbound packet the task does **O(receivers)** work (the `for (rsid,
  recipient) in snapshot` loop, line 3341), each iteration calling `Forwarder::decide`
  (`sfu/forwarder.rs:297`) which takes a room read-lock + subscriptions read-lock +
  speaker borrow, plus a per-receiver `format!` String allocation
  (`chat_server.rs:3487`).

So total per-second work on that one core ≈ **packet_rate(presenters) × receivers**. At
20 presenters vs 10 presenters the inbound rate roughly doubles, the per-packet fan-out
cost is unchanged (still ×receivers), so the single core saturates ~2× sooner. Once it
saturates, the 16 384-slot NATS subscription buffer (`nats_connect.rs:170`) overflows and
**async-nats silently drops inbound messages** — and that is why 20 presenters decodes
worse than 10, and why late joiners get nothing (§3).

---

## 1. The execution model (why it is one core)

### 1.1 One subscription, one task, all presenters

- A room's dispatcher subscribes **once** to the wildcard `room.<room>.*`
  (`chat_server.rs:2916`, subject from `build_subject_and_queue`,
  `models/mod.rs:51-53`). Every presenter publishes to `room.<room>.<their_session>`
  (off-actor, `session_logic.rs:767 publish_media_off_actor` → `nc.publish`,
  `session_logic.rs:615`). So **all N presenters' streams converge into one
  subscriber stream consumed by one task**.
- That task is a single `tokio::spawn` (`chat_server.rs:2915`). Its main loop
  (`chat_server.rs:3003`) does, per inbound message: parse-once (3144), feed the speaker
  scorer (3157-3177, takes `scorer.write().await`), remote-publisher bookkeeping
  (3190-3238), then snapshot receivers (3328-3334) and **fan out serially** (3341-3363).

### 1.2 The dispatcher shares a core with the room's actor

- `ChatServerPool::new` (`chat_server.rs:1008`) starts each shard with
  `actix::Arbiter::new()` + `start_in_arbiter` (`chat_server.rs:1021-1022`). actix-rt
  2.11 (Cargo.lock) gives each `Arbiter` its **own OS thread running a current-thread
  (single-threaded) tokio runtime**.
- `spawn_room_dispatcher` is called from inside `Handler<JoinRoom>::handle`
  (`chat_server.rs:2584`), which executes **on the owning shard's arbiter thread**.
  `tokio::spawn` there schedules the dispatcher onto **that same current-thread
  runtime**. So the dispatcher and the actor mailbox are **two tasks on one thread**,
  cooperatively scheduled. They never run in parallel.
- Rooms are mapped to shards by `jump_hash(room, n_shards)` (per the pool doc comment,
  `chat_server.rs:941-947`). A single webinar room hashes to **exactly one shard** → one
  arbiter → one core. The 4 shards on a 4-CPU pod do **not** help a single big room; they
  only spread *different rooms* across cores.

### 1.3 The fan-out cannot yield mid-loop

`Handler<JoinRoom>` returns `MessageResult<JoinRoom>` (`chat_server.rs:1914`) — a
**synchronous** handler: its whole body runs to completion as one non-yielding unit. The
dispatcher's fan-out loop (`chat_server.rs:3341-3363`) is likewise fully synchronous
(`recipient.try_send`, no `.await` between snapshot and the end of the loop). On a
current-thread runtime these two are **mutually exclusive in time**: while the dispatcher
is grinding an O(receivers) fan-out for one packet, the actor mailbox (new-joiner
registration) is blocked, and vice-versa.

---

## 2. The O(...) cost model

Let **P** = active presenters (senders), **R** = receivers (≈ total users), **f** = media
frame rate per presenter (audio ~50 pps, video ~30 fps + layers).

### Inbound (ingest) into the single task
```
inbound_rate ≈ P × f         [messages/sec arriving on the one room subscription]
```
Grows **linearly with presenters**. There is exactly one consumer task, one core.

### Per-inbound-packet work (the fan-out loop, chat_server.rs:3341)
```
per_packet_cost ≈ R × decide_cost
```
where each `decide` (`forwarder.rs:297`) acquires:
- `room.read()` (3340) — membership snapshot + gauge set,
- `subscriptions.read()` (3418) — `resolve_cached` (DashMap-backed, mostly Arc-clone),
- `speakers.borrow()` (3404) — lock-free,
- on video non-keyframes a `recent_t0.write()` (3608) — a **global per-room RwLock write**
  taken inside the per-receiver loop,
plus a per-receiver `format!("room.{room}.{receiver_session}")` **heap allocation**
(`chat_server.rs:3487`) and a self-subject string compare.

### Total work per second on the one core
```
core_load ≈ inbound_rate × per_packet_cost
          ≈ (P × f) × (R × decide_cost)
          = O(P × R)            ← per unit time, on ONE core
```

**This O(P × R)-per-second load on a single core is the bottleneck.** With R = 400 and
P going 10 → 20, `core_load` roughly **doubles** while the available compute stays at one
core. That is the precise reason 400u/20p (67% video) decodes worse than 400u/10p (82%
video): twice the per-second fan-out work on the same single thread.

### Where the single-thread choke is, per stage
- **Ingest choke:** the single `sub.next()` consumer (`chat_server.rs:3006`) draining one
  16 384-slot buffer (`nats_connect.rs:170`). Saturates at high P.
- **Egress choke:** the serial `for … in snapshot` loop (`chat_server.rs:3341-3363`) — no
  parallelism across receivers; one slow/full receiver mailbox `try_send` is cheap but the
  *count* R is paid in full for every one of the P×f inbound packets.
- Both choke on the **same** core (§1.2), so they compete: ingest backpressure and egress
  fan-out cannot be traded off against each other.

### Audio is the dominant fan-out term
Audio has **no MAX_VISIBLE cap** — `receive-all-audio` admits every presenter to every
receiver (`forwarder.rs:456`: `allow.audio.contains(&sender) || recv_all_audio`). Video is
capped at `MAX_VISIBLE_VIDEO = 6` selected senders (`forwarder.rs:499`,
`subscription.rs`). So the *uncapped* fan-out term is **audio = P × R forwards per
audio-frame-time**. At P=20, R=400 that is 8 000 `decide`+`try_send` per audio frame,
~400 000/sec for audio alone on one core. This is why audio — which has no keyframe
dependency — still collapses for late joiners (§3): it is the largest single contributor
to the saturation, and once the consumer falls behind, audio packets are dropped at the
NATS buffer just like video.

---

## 3. Why LATE joiners specifically get NOTHING (including audio), and why P makes it worse

This is **not** registration starvation and **not** AllowSet/speaker-set gating. The new
joiner *does* get registered correctly. The mechanism is **inbound backpressure loss at the
single saturated consumer**:

1. **Co-arrival is fine because the consumer was never behind.** When everyone joins at
   T=0, the inbound rate ramps with the room and the single task keeps up "well enough"
   for the snapshot decode sample; nobody is downstream of an overflowed buffer.

2. **Under sustained P-presenter load the one consumer task falls behind.** `inbound_rate
   ≈ P×f` feeds one task whose per-packet cost is R×decide. When `core_load` exceeds one
   core, `sub.next()` (`chat_server.rs:3006`) drains slower than NATS delivers. The
   16 384-slot subscription buffer (`nats_connect.rs:170`) fills, and **async-nats then
   *silently drops* messages** and raises a connection-global `Event::SlowConsumer` — it
   does **not** close the subscription (this exact failure is documented in the vc-9eh
   watchdog comment, `chat_server.rs:2939-2948`). Drops are not targeted; they hit
   **whatever is in the buffer**, audio and video alike. That is why a late joiner's
   **audio ≈ 0** even though audio has no keyframe dependency: the audio packets are being
   dropped *before* fan-out, at ingest.

3. **Why LATE joiners get ~0 while co-arrival joiners are fine — the asymmetry.** A late
   joiner is inserted into `receivers` correctly and synchronously (the vc-9eh ORDERING
   INVARIANT, `chat_server.rs:2600-2622`), so it *is* in every subsequent fan-out
   snapshot. But it arrives **into an already-saturated steady state**: the consumer is
   already dropping inbound at the buffer, the room's video keyframes that the newcomer
   needs are among the dropped packets, and the newcomer never observes a clean keyframe
   to start decoding (video) — and its audio is dropped at the same overflowing buffer
   (audio). The co-arrival joiners "caught" the early, un-saturated window; the late joiner
   never gets one. The room as a whole degrades, but the late joiner — having no prior
   decoder state and no buffered history — manifests it as ~0 rather than as reduced
   quality.

4. **Why the vc-9eh watchdog does NOT save this.** The watchdog
   (`chat_server.rs:3019-3137`) only resubscribes when the subscription goes **silent**
   (`silence ≥ window`, base 750 ms — `WATCHDOG_SILENCE`, `chat_server.rs:2798`). Under a
   presenter storm the subscription is **not silent — it is saturated**: `last_msg_at` is
   refreshed on every delivered message (`chat_server.rs:3011`), so `silence` never grows,
   the watchdog never trips, and the silent-drop loss continues unchecked. The watchdog is
   the right tool for a *wedged/black-holed* subscription but the **wrong tool for
   backpressure loss under high traffic** — which is exactly the late-joiner failure mode.

5. **Why higher P makes it worse — directly.** `core_load ≈ P×R` per second. Doubling P
   doubles the per-second work on the one core, pushing it past saturation sooner and
   deeper, so the NATS buffer overflows more and drops more. Hence 20p < 10p at the same R,
   and slow/stepped ramps (where the room is already at full P when the newcomer arrives)
   collapse to 2-12% — the newcomer joins a room whose single consumer is already losing
   packets.

**Distinguishing the three candidate mechanisms the task asked about:**
- *Registration starvation* — **NOT the cause.** Registration is synchronous and the
  insert is correct (`chat_server.rs:2621`); per-room sharding (vc-8txq) already removed
  the join-serialization bottleneck for the *actor*. (It can be a *secondary* effect: a
  saturated dispatcher on the same core delays the actor mailbox, slowing joins — but it
  does not explain audio=0 for an already-joined late receiver.)
- *Registered-but-not-forwarded* — **this is the cause**, but the reason is **inbound drop
  before fan-out**, not the fan-out skipping the receiver. The receiver is in the snapshot;
  the packets just never arrive at the consumer to be fanned out.
- *AllowSet / speaker-set gating* — **NOT the primary cause** for audio (receive-all-audio
  bypasses speaker gating, `forwarder.rs:456`). It can contribute to *video* (a late
  joiner's visible-video set depends on speaker state), but audio=0 rules it out as the
  root: audio is ungated and still collapses, which only ingest-side drop explains.

---

## 4. Fix path — make late joiners reliably receive content at 20p × 400r

### Does vc-ypx3 (per-room dispatcher fan-out sharding) fix presenter scaling?
**Partially — it fixes the EGRESS term, not the INGEST term, and only if it shards across
threads.** Sharding the fan-out loop (`chat_server.rs:3341`) so the R receivers are split
across W worker tasks reduces egress wall-time per packet from `R×decide` to
`(R/W)×decide`. That directly relieves the dominant `P×R` egress cost and is **necessary**.
But two caveats:
1. It must place the workers on a **multi-thread runtime / dedicated thread pool**, not on
   the room's single current-thread arbiter — otherwise the shards still time-share one
   core and nothing changes (§1.2). This is the load-bearing detail: vc-ypx3 as "shard the
   fan-out" is insufficient unless it also escapes the single-core arbiter.
2. It does **not** fix the **ingest** choke: one `sub.next()` consumer draining one buffer
   still serializes all P presenters. Egress sharding lets the consumer drain faster (less
   time per packet handed to fan-out), which helps, but at very high P the single
   parse+scorer-write+consume loop remains a per-core ceiling.

### What else is required (these are the missing pieces)
- **Audio mixdown / SFU-side audio mixing** — **highest-leverage fix.** The uncapped
  audio fan-out `P×R` is the single largest term (§2). Mixing the top-K audio streams into
  one (or a small fixed number of) mixed stream(s) server-side collapses the audio egress
  from `P×R` to `K×R` (K≈3) and makes audio independent of P. This is what makes
  *audio* reliable for late joiners at 20p. Without it, audio fan-out alone saturates the
  core.
- **Publish-side / ingest-side presenter filtering** — cap the number of presenters whose
  media is actually ingested+forwarded to the active speaker set (the room already
  computes top-N speakers, `speaker.rs` `MAX_SPEAKERS=4`, and visible video
  `MAX_VISIBLE_VIDEO=6`). Today the dispatcher ingests **all** P presenters' packets and
  filters per-receiver at egress (`forwarder.rs:511` drops unsubscribed *after* paying the
  ingest+decide cost). Filtering earlier (ideally at publish, or at a single ingest gate
  before the per-receiver loop) makes the **ingest** term independent of total P and
  proportional only to *active* presenters.
- **Parallelize ingest per presenter** (or per subject-shard): split the single
  `room.<room>.*` subscription into K subject-shards each consumed by its own task on the
  worker pool, removing the single-consumer ingest choke.
- **Backpressure observability for the high-traffic case:** the vc-9eh watchdog cannot
  detect saturation-drop (§3.4). Add a SlowConsumer/drop counter wired to the room (or a
  consumer-lag gauge) so the saturated-but-not-silent state is visible and can drive
  resubscribe/shed decisions. Without this the failure is invisible in soaks (crc=0, 0
  restarts — exactly what was observed).
- **Cheap wins on the hot path:** remove the per-receiver `format!` self-subject
  allocation (`chat_server.rs:3487`) — precompute self-skip from `SessionId` equality like
  the forwarder already does (`forwarder.rs:368`); and reconsider the `recent_t0.write()`
  taken inside the per-receiver loop (`forwarder.rs:608`) which serializes video receivers
  on one per-room RwLock write. These don't change the O(P×R) class but they lower the
  constant and push the saturation point out.

### Re-scoped beads — now v1-BLOCKING (late-joiner correctness is a v1 failure)

| Priority | Bead | Scope | Why blocking |
|---|---|---|---|
| **P0** | **vc-ypx3 (fan-out sharding) — REVISED** | Shard the `chat_server.rs:3341` fan-out across a **multi-thread worker pool**, not the room's current-thread arbiter. | Removes the single-core egress ceiling; the held fix as-specified is insufficient unless it escapes the arbiter. |
| **P0 (NEW)** | **audio-mixdown** | Server-side mix top-K audio → 1–K streams; egress goes `P×R → K×R`. | The uncapped audio fan-out is the largest term and the reason late-joiner **audio**=0. Single biggest lever for late-joiner audio. |
| **P0 (NEW)** | **ingest backpressure signal** | Per-room consumer-lag / SlowConsumer-drop metric; gate or shed on saturation. | The current watchdog is blind to saturation-drop (§3.4); the failure is invisible in soaks. Required to *verify* any fix. |
| **P1 (NEW)** | **publish/ingest presenter filter** | Only ingest+forward the active-speaker / visible-video presenter set; filter before the per-receiver loop (ideally at publish). | Makes the **ingest** term independent of total P. Pairs with vc-ypx3 to cover both ingest and egress. |
| **P1 (NEW)** | **parallel ingest (subject-shard the `room.<room>.*` subscription)** | K consumer tasks on the worker pool. | Removes the single-`sub.next()` ingest choke that vc-ypx3 alone leaves in place. |
| **P2** | **hot-path constants** | Drop per-receiver `format!` (`chat_server.rs:3487`); revisit `recent_t0.write()` in the per-receiver loop (`forwarder.rs:608`). | Lowers the constant factor; pushes saturation out but does not change the O(P×R) class. |

**Definitive verdict.** The presenter side IS an architectural scaling limit *as currently
built*: single-room ingest+egress is pinned to one core with O(P×R)-per-second load. It is
recoverable **within one pod** only by doing all of: (a) sharding the fan-out across real
threads (revised vc-ypx3), (b) audio mixdown to kill the uncapped `P×R` audio term, and
(c) ingest-side presenter filtering + parallel ingest to kill the single-consumer choke.
vc-ypx3 alone does **not** fix it — it addresses egress but not the ingest choke and not
the dominant audio term, and is itself a no-op unless it leaves the single-thread arbiter.

---

## Key citations
- Single per-room dispatcher task: `actix-api/src/actors/chat_server.rs:2900` (`spawn_room_dispatcher`), `tokio::spawn` at `:2915`, loop at `:3003`.
- **Serial O(receivers) fan-out loop (the egress bottleneck):** `chat_server.rs:3341-3363`.
- Per-receiver `format!` allocation: `chat_server.rs:3487`.
- Single wildcard subscription `room.<room>.*`: `chat_server.rs:2916` + `models/mod.rs:51-53`.
- Per-receiver `decide` cost (locks): `sfu/forwarder.rs:297` (room read `:3340`, subs read `:3418`, speaker borrow `:3404`, `recent_t0.write()` `:3608`).
- Uncapped audio admit (no MAX_VISIBLE): `forwarder.rs:456`; video cap `MAX_VISIBLE_VIDEO`: `forwarder.rs:499`.
- One arbiter (current-thread runtime) per shard, room→one shard: `chat_server.rs:1008-1024`, `:941-947`.
- Synchronous JoinRoom handler (shares the core, can't yield): `chat_server.rs:1913-1914`, dispatcher spawned from it at `:2584`.
- Synchronous late-joiner receiver insert (registration is correct): `chat_server.rs:2600-2622`.
- NATS subscription buffer 16 384 (overflow → silent drop): `nats_connect.rs:170`.
- Silent-drop / SlowConsumer behavior documented: `chat_server.rs:2939-2948`; watchdog (silence-only, blind to saturation): `chat_server.rs:3019-3137`, `WATCHDOG_SILENCE` `:2798`.
- Speaker scorer per-tick over all senders + publish: `sfu/speaker.rs:418` (`tick_once`), `top_n` `:133`, publish `:560-565`; per-inbound-audio `scorer.write().await` on the dispatcher: `chat_server.rs:3173`.
- Off-actor media publish (inbound rate scales with presenters): `actors/session_logic.rs:767`, `:615`.
