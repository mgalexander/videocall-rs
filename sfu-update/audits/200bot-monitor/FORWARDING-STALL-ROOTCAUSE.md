# Forwarding-Death Root Cause — slow-join at ~250 receivers

Read-only investigation. Branch `experimental-sfu`, tip `95513d5`. All citations
are `file:line` against that tip.

/ Investigator note: the decisive mechanism is NOT a deadlock, NOT a panic, NOT a
worker wedge, and NOT a blocking try_send. It is a **single-task inbound-drain
saturation that crosses an irreversible cliff** — once async-nats tail-drops on
the 16 KiB subscription buffer the dispatcher is *fed a reduced stream forever*,
and the only recovery net (the vc-9eh silence watchdog) is structurally blind to
a subscription that is still delivering *something*. lj-2 moved the drop boundary
but did not add a recovery trigger for the partial-delivery state. /

---

## TL;DR — the decisive mechanism

There is exactly **ONE** inbound dispatcher task per room (K = `ingest_shards`,
default **1** — `actix-api/src/sfu/config.rs:108` `DEFAULT_INGEST_SHARDS = 1`).
That single task is spawned at `chat_server.rs:3453` and runs a serial
`loop { … }` (`chat_server.rs:3712`) that, **for every inbound NATS message,
before it pulls the next one**, performs ALL of:

1. parse (`chat_server.rs:3951`),
2. scorer-batch push (`:3986`),
3. **remote-publisher registry maintenance with a `room_state.write()` taken
   INLINE** (`:4033`–`4087`) — load-bearing here because the 20 presenters are on
   *other* pods, so every media packet is from a *remote* publisher,
4. diagnostics ingest (`:4109`),
5. receiver snapshot under read lock (`:4182`),
6. the **sharded fan-out BARRIER** (`:4225`–`4338`): spawn ≤W shard tasks, then
   `for h in handles { h.await }` (`:4325`),
7. inline scorer flush holding `scorer.write().await` across the await (`:4355`,
   macro at `:3630`),
8. the lj-2 **greedy drain** that re-parses every drained message (`:4409`,
   `:4428`).

The barrier *parallelizes step 6 across W=4 workers*, but steps 1–5, 7, 8 are
**serial on the one dispatcher task**. The fan-out for message *k+1* cannot start
until message *k*'s barrier has fully joined (`:4318` "message k+1 fan-out never
starts before message k's completes"). So the room's entire inbound media rate
from all 20 remote presenters funnels through a **single task with a per-message
critical section that grows with receiver count** (the snapshot clone at `:4187`
is O(receivers), and each shard's `decide` holds the shared `room.read()` at
`forwarder.rs:353`, contending with the inline `room_state.write()` of step 3).

When the **per-message service time × inbound rate** exceeds 1.0 (the single task
is one core), the async-nats subscription queue (`subscription_capacity(16*1024)`
— `nats_connect.rs:196`) backs up and then **tail-drops**, firing only an opaque
process-global `SlowConsumer` (`nats_connect.rs:133`) that *does not close the
subscription* and *cannot be routed to a room*. async-nats keeps the stream open
and keeps delivering a **reduced** trickle.

That reduced trickle is the trap:

- The dispatcher still receives *some* messages, so `last_msg_at` keeps
  refreshing (`:3766`, and on the greedy-drain path `:4420`).
- The vc-9eh silence watchdog ONLY resubscribes when the subscription has been
  **completely silent** for the escalating window (`watchdog_should_resubscribe`,
  gate at `:3839` `silence < window`). A partially-delivering subscription is
  NEVER silent → the watchdog **never trips** → no resubscribe → no recovery.
- A resubscribe wouldn't even help: the bottleneck is the single task's CPU, not
  a dead NATS stream. (This is explicitly acknowledged in the vc-m7k6 comment at
  `:3768`–`:3774`: "A saturated dispatcher keeps draining … even while async-nats
  drops the overflow … the silence watchdog stays blind.")

**The CPU collapse to ~18m is the *post-cliff* steady state**: once async-nats has
shed the bulk of the offered load, the dispatcher only ever sees the small
fraction that survives the 16 KiB tail-drop, so it does very little work — ~18m of
CPU servicing a starved trickle, while 250–400 receivers sit dark. crc=0 because
the bytes that DO get through are forwarded correctly. The room never recovers
because nothing ever lifts the offered-rate back below the single-task ceiling and
nothing resubscribes (and resubscribing wouldn't help anyway).

This matches **every** observed symptom:

| Symptom | Explained by |
|---|---|
| CPU spikes ~1.1 core then **collapses** to 18m | Single task saturates one core, then async-nats sheds load → task starved |
| stays dead, room dark, never recovers | watchdog blind to partial delivery; no rate-based recovery; single-task ceiling unchanged |
| audio (no keyframe dep) dies too | it's the raw inbound *drain* that's behind, not a keyframe-dependency stall |
| 0 pod restarts, /healthz "Ok" after | no panic, no task exit; `note_inbound_drop` ages out of the saturation window so /healthz un-503s once drops stop (they stop *because the load was shed*, not because forwarding resumed) |
| 8 `SlowConsumer` lines in a prior run | the 16 KiB tail-drop firing during the cross-over |
| CO-ARRIVAL at 20p/400 reaches 366 with NO collapse | see §"Why co-arrival survives" |

---

## Walking the ranked hypotheses

### 1. The sharded fan-out BARRIER — *contributes, not the trigger*

`chat_server.rs:4318`–`4338`. The barrier is `for h in handles { h.await }`, NOT a
`join_all` on anything that can hang. Each shard future is **fully synchronous**
between spawn and return: `egress_decide_from_parsed` (`:4584`) →
`forwarder.decide` (`forwarder.rs:310`) takes only short *scoped* `std::sync`
read locks (room.read at `:353` dropped at `:378`; subscriptions.read at `:431`)
and `recipient.try_send` (`:4302`) is non-blocking. There is **no `.await`** inside
a shard, so a shard cannot park indefinitely and cannot hold a lock across an
await. A panicked shard is caught as a `JoinError` (`:4328`) and does NOT wedge the
loop. **No deadlock.**

With K=1 there is one dispatcher + ≤4 shard tasks on the 4-worker fan-out runtime
(`chat_server.rs:1143`–`1151`). The dispatcher parks on `h.await` (yielding its
worker), so the 4 shards get the 4 workers — **no worker-pool deadlock for a
single room.** The barrier's real cost is *latency*: it adds a spawn + cross-thread
join round-trip to the single task's per-message critical section, *raising* the
per-message service time and thus *lowering* the rate at which the single task
crosses the 16 KiB cliff. So the barrier is an **amplifier** of the single-task
bottleneck, not the stall itself.

### 2. lj-2's shed path — *ineffective by construction; NOT a dispatcher exit*

`chat_server.rs:4375`–`4483`. The greedy drain does NOT exit the loop, does NOT
stop re-subscribing, does NOT stop consuming. The dispatcher task only exits via
the `None` arm (`:3781` → break → `:4504 RoomDispatcherExited`) or a failed
resubscribe (`:3919`). Neither happens here (0 restarts confirms no respawn). **So
the 18m floor is NOT a dispatcher-task exit.**

Why lj-2 doesn't prevent the collapse — two structural gaps:

- **It moved the drop boundary but kept the single-task drain.** The greedy drain
  (`now_or_never` at `:4409`) only runs AFTER the inline fan-out + barrier of the
  current message completes. So it can only empty async-nats's buffer as fast as
  the single task finishes each message's full critical section. When per-message
  service time × rate > 1, the greedy drain can never catch up; it caps at
  `DISPATCHER_INBOUND_QUEUE_CAP = 1024` per pass (`:4401`, const at `:3070`) and,
  worse, **re-parses every drained message** (`parse_and_inspect` at `:4428`) on
  the single task — adding cost precisely under overload. The drop just relocates
  from async-nats's invisible 16 KiB buffer to lj-2's 1024-deep `inbound_queue`
  with explicit class-shedding. Media still drops; the room still goes dark.
- **It added no recovery trigger for the partial-delivery state.** lj-2 makes the
  drop *explicit and class-aware* but does not change the recovery net, which is
  still the silence-only vc-9eh watchdog (`:3839`). A perpetually-behind-but-not-
  silent dispatcher is exactly the state lj-2 leaves un-recovered.

### 3. Worker-pool exhaustion / blocking — *not the trigger at K=1, single room*

No blocking op runs inside a shard or across an await on a way that wedges all 4
workers for a single room (see §1). The inline `room_state.write()` at `:4053` and
the `scorer.write().await` at `:3633` run on the **dispatcher** task, not the shard
workers; they serialize the *one* task (cost), they do not wedge the *pool*. This
hypothesis would bite with many rooms (many dispatchers each spawning shards onto
the shared 4-worker runtime) — a real future hazard — but this test is a single
room, so it is not the decisive mechanism here.

### 4. Receiver mailbox backpressure feedback — *non-blocking; drops, never stalls*

`Recipient::try_send` (`:4302`, `:4239`) → actix `AddressSender::try_send(msg,
park=true)`. Verified in vendored actix-0.13.5: even with `park=true` the call
**enqueues and returns immediately** (`channel.rs:367`–`372`), and a previously-
parked-and-not-yet-unparked sender is rejected with `Err(Full)` up front
(`channel.rs:354`–`355`). Either way it is non-blocking; the dispatcher logs
"mailbox full — subscription continues" (`:4306`) and moves on. The WT/WS
`Handler<Message>` then pushes into a *bounded, drop-policy* `PrioritySender`
(`ws_chat_session.rs:290`, `wt_chat_session.rs:336`) — also non-blocking. **No
backpressure feedback can stall the dispatcher.** (The only `ctx.stop()` paths are
P0Control-class-full — `ws_chat_session.rs:318` — which is a per-session teardown,
not a room-wide stall.)

### 5. Caught panic / task abort killing the dispatcher silently — *ruled out*

The only panic-catch is the per-shard `JoinError` at `:4328`, which logs and
continues. No `catch_unwind` wraps the dispatcher loop; a dispatcher panic would
surface as `RoomDispatcherExited` + a respawn (and a restart count). 0 restarts +
no exit log ⇒ the dispatcher task is **alive and looping**, just starved. The
vc-zf8k `forwarding_health` heartbeat (`:4340`) is observational only.

### 6. Why co-arrival survives but slow-join dies

Co-arrival reaches a *steady state* with the receiver set fixed and the remote-
publisher registry warm. In that regime:

- The remote-publisher registry is fully populated and stable, so
  `remote_publisher_write_needed` returns false on the common path
  (`room_state.rs:316`–`331`) → **no inline `room_state.write()` storm**, no
  `RewarmSubscriptionCache` fan (`:4082`–`4086`), AllowSet caches stay hot
  (`resolve_cached` hits).
- The per-message critical section is therefore at its *minimum*, so the single
  task's service time stays under the cliff at 366 receivers.

Slow-join (waves of 50 every 25 s) keeps the system in a *perpetual transient*:
each wave (a) bumps `members_generation` via joins, busting every receiver's
AllowSet cache so the next `decide` per receiver MISSES and recomputes
(`forwarder.rs` resolve path), AND (b) the 20 remote presenters' packets keep
re-warming/refreshing the registry inline (`:4033`). Receiver count is *also*
climbing, so the O(receivers) snapshot (`:4187`) and the barrier spawn/join grow
each wave. The combination pushes per-message service time up exactly as offered
rate is highest — and somewhere around wave 4→5 (200→250 receivers) it crosses
1.0, async-nats tail-drops, and the system falls off the cliff into the
un-recoverable partial-delivery state. The cliff is **rate-triggered and
hysteretic**: once load is shed it stays shed, because nothing re-raises it and
nothing resubscribes. Co-arrival never enters the perpetual-transient regime, so
it never crosses the cliff.

---

## Why the floor is exactly ~18m and *permanent*

After the cliff, the offered media that *reaches* the dispatcher is only what fits
through the 16 KiB async-nats buffer's tail-drop survival rate. The single task
services that trickle — a few decodes + a tiny barrier — at ~18m CPU. Because:

- the offered rate from the 20 presenters does NOT drop (they keep publishing),
- the single-task ceiling does NOT rise,
- the watchdog never resubscribes (never silent),
- and even a resubscribe would re-attach the same single-task drain,

…there is no closed-loop path back to healthy. The room is dark for the duration.

---

## Fix spec

The root problem is **a single-task inbound drain whose per-message critical
section is O(receivers) and grows under join-churn**, with **no rate-based
recovery** once async-nats sheds. Two independent levers; do BOTH.

### Lever A — restore self-healing: rate-based watchdog recovery (HIGHEST PRIORITY)

The watchdog must trip on **"behind"** (drop counter rising while receivers
present AND inbound rate below offered), not only on **"silent"**. We already
publish `sfu_dispatcher_inbound_rate` (`:3654`) and increment a process-global
drop counter on every `SlowConsumer` (`metrics.rs`, set via `nats_connect.rs:145`).
But neither *recovery* nor *load-shed escalation* keys off them today.

Concretely: add a SATURATION arm to `watchdog_should_resubscribe`
(`chat_server.rs:3290`) / the watchdog tick (`:3783`) that, when receivers are
present and the process drop counter's slope is positive over the last window,
takes a **load-shedding action that actually lowers per-message cost** — see
Lever B — rather than (or in addition to) resubscribing. A bare resubscribe is
NOT sufficient because the ceiling is CPU, not a dead stream.

> NOTE per CLAUDE.md: a watchdog/timeout change touches shared connection logic
> and BOTH transports — route the design through **backend-rust-streaming** before
> implementing, and validate against high-latency/lossy links (the rate threshold
> must not false-trip on a slow but healthy 200 ms link).

### Lever B — raise the single-task ceiling so the cliff moves past 400

1. **Get the `room_state.write()` and the re-parse OFF the single drain task.**
   The inline remote-publisher write (`:4033`–`4087`) and the lj-2 greedy-drain
   re-parse (`:4428`) are pure single-task cost added under exactly the overload
   they're meant to survive. Move remote-publisher registration to a batched,
   off-task path (mirror the vc-zexm RewarmSubscriptionCache deferral pattern at
   `:4082`), and carry the already-parsed `ParsedPacket` from the greedy drain so
   no message is parsed twice.

2. **Default K (`SFU_INGEST_SHARDS`) > 1 for large rooms.** K shards the *inbound
   drain itself* across K independent dispatcher tasks (`spawn_room_dispatchers`,
   `:3340`), each on its own NATS subscription — this is the only lever that
   parallelizes steps 1–5/7/8, which the fan-out barrier does NOT. The publish
   subject is already shard-aware (`build_publish_subject(.., K)`). Validate the
   K-shard subscribe/round-trip (vc-kcpg) under this slow-join profile.

3. **Pipeline the fan-out (remove the strict barrier)** so message k+1's drain
   begins before message k's fan-out completes — explicitly deferred at `:4207`.
   This decouples *drain throughput* from *fan-out latency*, which is the single
   biggest per-message-service-time reduction available. Per-class ordering must
   be preserved; route through **backend-rust-streaming**.

4. **Raise `subscription_capacity` above 16 KiB OR add per-subscription pending
   visibility.** 16 KiB (`nats_connect.rs:196`) is a hard cliff with an
   unobservable depth (acknowledged at `:146`). A deeper buffer buys ride-out time
   for transient waves; pairing it with the Lever A rate-watchdog prevents it from
   masking sustained overload. Lower priority than 1–3.

---

## Bead breakdown (priority order)

| # | Bead | Lever | Priority | Owner agent | Notes |
|---|------|-------|----------|-------------|-------|
| 1 | **Rate/drop-slope watchdog recovery + shed escalation** — trip on "behind", not only "silent" | A | **P0 (blocker)** | backend-rust-streaming | Without this the room is *permanently* dark after the cliff. Uses existing `sfu_dispatcher_inbound_rate` + drop counter. Must not false-trip on healthy 200ms links. |
| 2 | **Move remote-publisher write + greedy re-parse off the drain task** | B.1 | **P0** | backend-rust-streaming | Pure single-task cost added under overload; biggest cheap win. Reuse parsed packet end-to-end. |
| 3 | **Pipeline fan-out (drop strict barrier), preserve per-class ordering** | B.3 | **P1** | backend-rust-streaming | Decouples drain throughput from fan-out latency. The `:4207` deferral comes due. |
| 4 | **Default K>1 for large rooms; validate K-shard slow-join** | B.2 | **P1** | backend-rust-streaming | Only lever that parallelizes the inbound *drain*. Confirm 4-token subject migration under this profile. |
| 5 | **Deepen/observe `subscription_capacity`** | B.4 | **P2** | backend-rust-streaming + deploy-sync-expert | Ride-out headroom; only safe paired with bead 1. |
| 6 | **Future: many-room worker-pool guard** (dispatcher + shard tasks share the 4-worker fan-out runtime) | (3) | **P2 / watch** | backend-rust-streaming | Not the trigger here (single room), but a real hazard once many large rooms coexist — spawn budget / dedicated drain workers. |

Run **code-reviewer** + **performance-reviewer** after each substantive change, and
**integration-test-writer** to extend the 200-bot slow-join harness to assert
recovery after the cliff (forwarding rate returns to non-zero within N s while
receivers stay connected) — the missing regression that lets this ship.

---

## Key citations (quick index)

- One dispatcher per room, default K=1: `sfu/config.rs:108`; spawn fan
  `chat_server.rs:3340`–`3368`, single spawn `:3453`.
- Serial per-message loop: `chat_server.rs:3712`; inbound arms `:3753`–`3938`.
- Inline `room_state.write()` remote-publisher storm: `:4033`–`4087`;
  `room_state.rs:310`–`425` (cap 32, throttle 1s, TTL 10s at `:60`/`:82`/`:68`).
- O(receivers) snapshot: `:4182`–`4188`.
- Sharded fan-out barrier: `:4225`–`4338` (await join `:4325`, JoinError caught
  `:4328`, deferred-pipelining note `:4207`).
- `decide` scoped read locks (no await): `forwarder.rs:310`,`353`,`378`,`431`.
- non-blocking `try_send` (actix park=true still returns immediately):
  `channel.rs:354`,`367`–`372`,`496`–`498`.
- lj-2 greedy drain + re-parse + class shed: `:4375`–`4483`; cap
  `DISPATCHER_INBOUND_QUEUE_CAP=1024` `:3070`.
- async-nats 16 KiB cliff + opaque SlowConsumer, subscription stays open:
  `nats_connect.rs:196`,`133`–`167`.
- silence-only watchdog (blind to partial delivery): gate `:3839`,
  predicate `:3290`; saturation acknowledged `:3768`–`3774`.
- fan-out runtime (4 workers, dispatcher + shards share it):
  `chat_server.rs:1143`–`1151`,`3452`–`3454`.
