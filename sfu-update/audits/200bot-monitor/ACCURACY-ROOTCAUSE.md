# Accuracy root cause — controlled co-arrival webinar (5 presenters + 200 listeners, all @ T=0)

Read-only investigation. Branch `experimental-sfu`, HEAD `89d16d3`. All citations
`file:line` against that tip. No code changed.

> **UPDATE (2026-05-25) — the clean A/B is in and it CONFIRMS a regression in the
> four-commit delta. The original "pre-existing, not a regression" conclusion is
> SUPERSEDED. See §R below for the corrected localization.**

Clean A/B (same controlled 5p/200 co-arrival, full decode+crc, ONLY the SFU build differs):

| Build | both (v+a) | audio-only | NEITHER | crc |
|-------|-----------|-----------|---------|-----|
| **2e82095** (validated full-Track1 = vc-c609 + vc-9u8e + vc-kcpg) | **154 (77%)** | 32 | **14** | 0 |
| **HEAD 89d16d3** (= 2e82095 + lj-1 vc-zexm + lj-2 vc-vyg9 + lj-7 vc-j4kz + lj-6 vc-nys3) | **48 (24%)** | 66 | **86** | 0 |

The four post-2e82095 commits **6× the NEITHER (14 → 86)** and open the
`receiver_set` gap. HEAD milestone markers (vc-9eve):

```
member_count=10   receiver_set=10   gap=0
member_count=50   receiver_set=50   gap=0
member_count=100  receiver_set=59   gap=41   forward_total=18027 (allowset_audio=99 allowset_video=99)
member_count=200  receiver_set≈114  gap≈86
```

The disconnect/grace/election orphan-leak (§1) is REAL but explains only the ~14
baseline (2e82095 already has it). §R below identifies what turns 14 into 86.

---

## 0. The load-bearing execution-model facts (verified)

- **One room = one shard = one thread.** Rooms jump-hash to exactly one
  `ChatServer` actor shard (`chat_server.rs:1079-1086`, `:1126-1129`). All of a
  room's `Connect`/`JoinRoom`/`ActivateConnection`/`Disconnect` are serialized on
  that single Arbiter thread (`chat_server.rs:1088-1098`). `room_members`,
  `room_dispatch`, `pending_departures`, `room_states` for the room all live on
  that one shard.
- **`room_members` and the dispatcher `receivers` map are written in lockstep
  inside ONE synchronous `Handler<JoinRoom>` call**: `room_members.push` at
  `chat_server.rs:2776-2782`, the `receivers.write().insert` at
  `chat_server.rs:2914-2936`, with the explicit "DO NOT MOVE BELOW THE spawn"
  ordering invariant (`chat_server.rs:2923-2934`). There is no async/tick boundary
  between them and no separate "register with dispatcher" step. The per-packet
  fan-out re-reads this exact `receivers` Arc live (`chat_server.rs:4620-4626`).
- **The milestone marker reads BOTH counts inside that same handler**, after both
  inserts: `member_count` from `room_members.len()` (`chat_server.rs:2954`),
  `receiver_set` from `receivers_for_room.read().len()`
  (`maybe_emit_join_milestone`, `chat_server.rs:669-672`), `allowset_*` from
  `room_state.members_snapshot()` (`chat_server.rs:681-695`).

**Consequence (decisive):** within the join that crosses a milestone, a JOINING
session is in both maps. A persistent `member_count > receiver_set` gap therefore
cannot be a "join hasn't finished inserting yet" lag. It can only be produced by
sessions that were **removed from `receivers` but deliberately LEFT in
`room_members`**. Exactly one code path does that, and it is the root cause.

---

## 1. Root cause of the receiver-set gap

### Decisive mechanism: the grace-period Disconnect drops the receiver immediately but keeps the `room_members` row — and the (room,user)-keyed reaper leaks orphans under co-arrival churn

`Handler<Disconnect>` (non-observer, non-redirect path),
`chat_server.rs:1391-1435`:

```rust
// chat_server.rs:1391-1398
// vc-q0v: drop this session from the per-room demux receiver map
// immediately ... Keep room_members intact for now — they will be
// cleaned up either on reconnection or when the grace period expires.
self.joined_sessions.remove(&session);
self.drop_room_receiver(&room, &session);   // <-- receivers.remove(session), NOT room_members
```

`drop_room_receiver` removes the session from `receivers` (and aborts the
dispatcher only if the map empties) — `chat_server.rs:1046-1065`. It does **not**
touch `room_members`. The PARTICIPANT_LEFT and the actual `room_members` cleanup
are deferred to `ExecutePendingDeparture` after `RECONNECT_GRACE_PERIOD = 2s`
(`constants.rs:32`), scheduled via `ctx.notify_later` and tracked in
`pending_departures` keyed on **`(room, user_id)`** (`chat_server.rs:1403-1435`).

**Why this fires hard at T=0 co-arrival.** Every client runs an RTT connection
election: it opens MULTIPLE connections (Testing state), probes RTT, elects one,
and the LOSERS disconnect (`connection_controller.rs:44-120`,
`videocall-client/...` election timers). Each connection is its own `SessionId`
that sends its own `JoinRoom`. So per client:

1. Loser session A joins → `room_members += A`, `receivers += A`.
2. A loses election → `Disconnect` (A was never `Active`) → `drop_room_receiver`
   removes A from `receivers`; **A stays in `room_members`** for the 2s grace
   (`chat_server.rs:1397-1398`); a timer keyed `(room,user)` is staged with
   `old_session = A`.
3. Winner session B (same `user_id`) joins. It is treated as `is_reconnection`
   (`chat_server.rs:2593`) and retains-out **only `pending.old_session`** from
   `room_members` (`chat_server.rs:2598-2600`) and `room_state`
   (`chat_server.rs:2604-2610`), then inserts B into both maps.

At 200 clients arriving simultaneously, at any instant a large fraction have a
loser session sitting in `room_members` but absent from `receivers` — that IS the
`member_count(100) − receiver_set(59) = 41` gap. `allowset_audio = 99`
(`= member_count − self`) confirms `room_state.members_snapshot()` is equally
inflated by the same stale losers. This much is a **transient, self-healing** gap
(≤2s per loser) and on its own would only depress the milestone snapshot, not
darken a winner for 240s.

### The PERSISTENT leak (why winners go dark for the full 240s)

`pending_departures` is keyed on `(room, user_id)`, and BOTH the second-disconnect
replace path and the reconnection-cleanup path only ever account for the SINGLE
`old_session` currently stored under that key:

- Second disconnect for the same `(room,user)` BEFORE the first grace fires
  (`chat_server.rs:1403-1410`): `pending_departures.remove(&key)` →
  `ctx.cancel_future(old.spawn_handle)` cancels the FIRST timer, then a NEW entry
  is inserted with `old_session = the newer session`. **The first stale session's
  `room_members` row is now orphaned forever** — no timer will reap it (cancelled)
  and no reconnection retains it out (only `old_session`, now the newer sid, is
  retained — `chat_server.rs:2598`).
- `ExecutePendingDeparture` with `pending.old_session != session`
  (`chat_server.rs:1592-1601`): re-inserts the newer pending and returns **without
  cleaning the stale session's `room_members`**.

Under a >2-candidate election or any disconnect/reconnect chatter inside a 2s
window during the burst (high-latency/jitter links make this the norm, not the
edge — see CLAUDE.md "real-world networks"), a user can churn through ≥2 stale
sessions and permanently strand all-but-one in `room_members`. These orphans are
absent from `receivers` (so they never receive) yet counted as members forever
(240s) — the steady-state `member_count > receiver_set` and the
"connected + room-member but 0 media" population. A winner whose elected session
is itself caught in a replace race (its `receivers` entry removed by a Disconnect
whose later re-join was mis-accounted) is dark for the run — the most consistent
explanation for the 86 NEITHER given `inbound_dropped≈0` (NOT saturation) and
`forward_total` still climbing (the ~59 live winners ARE served).

---

## R. CORRECTED regression localization (14 → 86) — vc-zexm moved an O(R·M) sweep ONTO the single-threaded join/actor path

### R.0 What the four commits actually changed (verified vs `2e82095`)

`git diff 2e82095..89d16d3 -- chat_server.rs` is exactly the four lj commits.
None of them edits the `Disconnect`/`pending_departures`/`drop_room_receiver`
divergence logic — that is unchanged from 2e82095 (this is why §1's leak explains
the **14** baseline that 2e82095 ALSO has). The regression is therefore not a NEW
divergence; it is a NEW way to **manufacture far more Disconnects/reconnects
during the burst**, which then drives the SAME §1 leak ~6× harder.

The decisive change is in **vc-zexm (`d0241be`)**:

- **2e82095 has ZERO `rewarm_subscription_cache` calls** (verified:
  `git show 2e82095:…/chat_server.rs | grep rewarm` → empty). On 2e82095 a join
  bumps `members_generation` + `invalidate_all()` and the per-receiver AllowSet
  recompute happens LAZILY, amortized, on the dispatcher hot path via
  `resolve_cached`'s miss arm (`subscription.rs:resolve_cached`, self-healing) —
  i.e. OFF the actor thread, spread across the fan-out workers.
- **HEAD adds `forwarder.rewarm_subscription_cache()` SYNCHRONOUSLY inside the
  `Handler<JoinRoom>` body** (`chat_server.rs:2873`) — and on the leave path
  (`:888`) and in two new actor-message handlers (`:1824`, `:1881`).

`rewarm_subscription_cache` → `SubscriptionStore::rewarm_cache`
(`forwarder.rs:701-736`, `subscription.rs:389-431`) iterates **every cached
receiver** (`self.cache.iter()`, bounded by room size **R**) and for each STALE
entry calls `resolve_inner`, which is **O(members M)** (`subscription.rs:435-…`).
A join bumps the GLOBAL `members_generation` (`room_state.rs` `insert_member` →
generation++), so on EACH join EVERY cached receiver is stale ⇒ each join's
re-warm rebuilds all R entries: **O(R·M) per join, ALL synchronous on the single
shard actor thread** (`chat_server.rs:1079-1098` — one room = one thread).

### R.1 The mechanism (how O(R·M)-per-join → 86 NEITHER)

Across a 200-co-arrival burst, the actor now performs ~Σ O(R·M) ≈ **O(N³)** AllowSet
resolves serially on ONE core, in the join handler, before each `MessageResult(Ok)`
returns. Compounding it: vc-j4kz fires a `RegisterRemotePublisher` actor message
per new remote-publisher MEDIA packet (`chat_server.rs:1847-1885`, dispatcher
`try_send` per MEDIA), whose handler takes `room_state.write()` and may itself
re-warm O(R·M) again (`:1879-1882`) — on the SAME mailbox/thread, during the same
burst (5 presenters = federated remote publishers). The shard actor's JoinRoom /
Connect / ActivateConnection / Disconnect service latency explodes from µs to many
ms each.

That slowness is the regression engine for §1's leak:

1. Slow actor ⇒ Connect/JoinRoom/ActivateConnection handshakes and heartbeats time
   out client-side ⇒ connections drop and the client re-elects / reconnects (the
   RTT election multi-connection model, `connection_controller.rs:44-120`).
2. Each drop runs `Handler<Disconnect>` → `drop_room_receiver` removes the receiver
   IMMEDIATELY but keeps the `room_members` row for the 2s grace
   (`chat_server.rs:1391-1398`).
3. Under sustained slowness, a user churns ≥2 sessions inside the 2s grace window
   ⇒ the `(room,user)`-keyed reaper (`:1403-1410`, `:1592-1601`) leaks all-but-one
   stale `room_members` row PERMANENTLY (§1). The receiver is gone, the member is
   counted forever ⇒ `member_count > receiver_set` for the full 240s.

This is exactly the signature: `inbound_dropped≈0` (the bottleneck is the
**join/actor** path, NOT the dispatcher subscription — so the vc-vyg9 shed and the
async-nats buffer never engage); `forward_total` still climbing (the ~59 live
winners ARE served); `crc=0`. 2e82095 kept the actor fast (no per-join sweep), so
the election settled cleanly and only the ~14 structural election losers leaked.

### R.2 Is it ONE commit?

**Primary cause: vc-zexm (`d0241be`)** — the synchronous per-join/per-leave
O(R·M) re-warm on the actor thread. This single change converts a hot-path
(fan-out-side, parallel, amortized) cost into a join-path (actor-side, serial,
eager) cost during the precise co-arrival burst, throttling registration.

**Amplifier: vc-j4kz (`9113323`)** — the per-MEDIA `RegisterRemotePublisher`
actor message adds more `room_state.write()` + conditional O(R·M) re-warm
(`:1879-1882`) onto the SAME mailbox during the burst. On its own (without the
join-path re-warm) its mailbox load is bounded by the throttled dedup, so it is
secondary, but it compounds vc-zexm.

**Likely NOT primary: vc-vyg9 (`95513d5`)** — its class-shed only engages when the
internal queue hits `DISPATCHER_INBOUND_QUEUE_CAP = 1024`
(`chat_server.rs` const). With `inbound_dropped≈0` it essentially never fired in
this run, so it is not the 14→86 driver. **vc-nys3 (`bd21d80`)** is a recovery
trigger (resubscribe on drop-slope) that, with `inbound_dropped≈0`, also did not
fire; it does not evict receivers (it reuses the same `receivers` Arc), so it is
not implicated. Both are dispatcher-internal and orthogonal to the actor-path
saturation.

**Conclusion: the regression is surgically isolatable to vc-zexm (with vc-j4kz as a
compounding factor), NOT the whole four-commit delta.**

---

## 2. Root cause of the video-keyframe gap (66 audio-only)

### Decisive mechanism: no keyframe-on-subscribe and no server-initiated KEYFRAME_REQUEST on join — a fresh receiver's video is gated on the sender's NEXT NATURAL keyframe

A receiver that is in `receivers` and whose AllowSet admits the sender gets AUDIO
immediately (audio is admitted unconditionally for a default joiner —
`forwarder.rs` audio path; `receive_mode` default `(true,true)`,
`subscription.rs:554-560`). Video, however, is only DECODABLE from a base keyframe
(T0+S0), which the forwarder forwards when it ORGANICALLY arrives
(`forwarder.rs:547-553`, `is_base_keyframe` always forwards; non-keyframes that
reference an un-delivered T0 are dropped as `REFERENCE_MISS`,
`forwarder.rs:617-645`). The SFU has **no keyframe-on-subscribe trigger**: the
`JoinRoom` handler never fires a KEYFRAME_REQUEST, and the only KFR handling in the
server is RELAYING client-originated KFRs (`chat_server.rs:2125-2163`,
`packet_handler.rs:115-116`). So a receiver that subscribes mid-GOP sees only
P-frames (un-decodable) until the encoder's next periodic keyframe.

Encoder cadence is one keyframe per 150 frames (`bot/src/video_encoder.rs:101-103`;
production CLI matches). At ~30fps that is a ~5s blind window per subscribe —
during which the listener decodes 0 video. This is the residual of Defect-2.

**Caveat — this is partly (in the bot test, primarily) a harness limitation, not
a shipping SFU bug.** `DEFECT2-VIDEO-KEYFRAME.md` (validated against
`spillover-decode/`) shows the bot SENDER (a) drops keyframes indiscriminately
under a shared 100-slot bounded channel (94% drop measured,
`bot/src/video_producer.rs:201-227`) and (b) cannot honor a KEYFRAME_REQUEST at all
(`VPX_EFLAG_FORCE_KF` never set, `bot/src/video_encoder.rs:166`; sender built
without `.with_decode(true)`, `bot/src/orchestrate.rs`). So even a server-fired
KFR-on-subscribe would land on a sender that ignores it. **A keyframe-on-subscribe
fix helps REAL clients (which honor KFRs and prioritize keyframes) but will NOT by
itself move the bot's audio-only number** — that needs the bot-sender fixes from
DEFECT2. The 66 audio-only here are receivers that DID integrate (got audio) but
whose first decodable video keyframe never arrived inside the window.

---

## 3. Fix spec (minimal, targeted)

> Priority order: **Fix R first** (it is the 14→86 regression and the only thing
> that recovers 2e82095's 77%). **Fix A1 second** (it closes the residual ~14
> baseline on TOP of whichever base we land on). **Fix B** is the real-client
> video improvement (does not move the bot number without the DEFECT2 bot-sender
> work).

### Fix R — undo the regression: get the O(R·M) AllowSet re-warm OFF the synchronous join/actor path

The 14→86 driver is the per-join/per-leave synchronous `rewarm_subscription_cache()`
(vc-zexm) on the single-threaded shard actor. Three options, smallest first:

- **R1 (REVERT-the-four, RECOMMENDED for v1): drop all four lj commits and ship
  2e82095 as the v1 base.** Rationale: the four commits are all **overload-targeted
  defense-in-depth** (AllowSet thundering-herd relief, inbound-drain decoupling,
  remote-pub offload, drop-slope recovery) for the lj slow-join / spill-pod /
  10k-soak regimes — NONE of which is the ≤200 single-pod v1 target. 2e82095 is the
  validated full-Track1 base and **already delivers 77% on this exact test** with
  `inbound_dropped≈0` (it is not saturation-bound at 200). Reverting removes the
  regression wholesale with zero residual risk from partially-reverted machinery.
  This is the cleanest v1 move.

- **R2 (surgical, if the team wants to KEEP lj-2/6/7): isolate the revert to
  vc-zexm.** Remove the four synchronous `rewarm_subscription_cache()` calls
  (`chat_server.rs:2873` join, `:888` leave, `:1824`, `:1881`) and the
  `RegisterRemotePublisher` re-warm (`:1879-1882`). Correctness is preserved
  because `resolve_cached` self-heals on a miss (`subscription.rs:resolve_cached`,
  the miss arm computes+stores) — the doc-comment at `chat_server.rs:2870-2872`
  states this explicitly ("correctness never depends on the warm-up being
  current"). This reverts to 2e82095's lazy, hot-path, amortized recompute while
  keeping vc-vyg9/j4kz/nys3. **Caveat:** vc-vyg9's commit message calls vc-zexm
  "the lj-2 root-cause fix it is defense-in-depth FOR"; keeping vyg9 without zexm
  re-exposes the slow-join (NOT co-arrival) AllowSet thundering-herd that vc-zexm
  targeted — acceptable for ≤200 v1, but means lj slow-join scaling is unsolved.

- **R3 (keep vc-zexm but make it non-blocking): move the join/leave re-warm
  off-actor.** Spawn it on `fanout_handle` (the same runtime the dispatcher uses)
  instead of running it inline in the handler, OR debounce it (one coalesced
  re-warm per N joins / per tick) so a 200-burst pays O(R·M) a handful of times,
  not 200×. Larger change, more review surface; only worth it if the team insists
  on keeping eager warming. R1 or R2 is preferred.

### Fix A — make membership and the fan-out set converge (close the residual ~14 baseline)

The defect is the divergence between "counted as a member" and "in the fan-out
set", made permanent by the `(room,user)`-keyed reaper losing orphans. Two
minimal, independent options (A1 is the smaller, safer one):

- **A1 (preferred, minimal): make the reaper account for EVERY stale session, not
  just the latest `old_session`.** Key `pending_departures` (or an auxiliary
  orphan list) on `(room, SessionId)` rather than `(room, user_id)`, OR have the
  second-disconnect replace path (`chat_server.rs:1403-1410`) and the
  stale-`old_session` arm of `ExecutePendingDeparture` (`chat_server.rs:1592-1601`)
  IMMEDIATELY retain-out the session being superseded from `room_members` +
  `room_state` before overwriting/returning. This guarantees a session removed
  from `receivers` is also removed from `room_members` within the grace window —
  no permanent orphan. This is the surgical fix and does not touch the hot path.

- **A2 (consistency hardening): drop the receiver from `receivers` and the member
  from `room_members` ATOMICALLY at Disconnect**, and instead defer only the
  *PARTICIPANT_LEFT broadcast* (not the membership row) for the grace window.
  i.e. stop carrying a member in `room_members` that is intentionally absent from
  `receivers`. This collapses the two-map divergence entirely. Larger blast radius
  (changes reconnection-dedup semantics and the existing-member list a reconnector
  receives), so A1 is preferred for v1.

Either way, the JoinRoom insert ordering (`chat_server.rs:2914-2936`) is ALREADY
synchronous and correct — **no change to the hot/insert path is required**. The
brief's framing ("make receiver-set insertion atomic with join") is already true;
the divergence is on the LEAVE/grace side, so A1 belongs there. NOTE: with Fix R
in place the actor is fast again, the election settles cleanly, and A1's leak is
back down to the ~14 structural floor — A1 then closes that floor toward 0.

### Fix B — keyframe-on-subscribe (prompt video for a new receiver)

When a session is admitted into `receivers` (`chat_server.rs:2935`), fire a
KEYFRAME_REQUEST toward the room's current video senders so a fresh subscriber does
not wait for the next natural keyframe. Reuse the EXISTING KFR publish machinery
(`chat_server.rs:2085-2092` subject build, NATS publish), gated/debounced so a join
burst does not fan a KFR storm at the presenters (coalesce per sender per
`KEYFRAME_REQUEST_MIN_INTERVAL` window — the limiter at
`packet_handler.rs:340-367` already exists and should be reused, NOT bypassed).
Target only senders the joiner's AllowSet admits (the layer-aware drop at
`chat_server.rs:2144-2161` already encodes that predicate). **Note:** this is a
REAL-CLIENT fix; it will not change the bot numbers until the DEFECT2 bot-sender
fixes land (force-KF + keyframe-priority channel).

---

## 4. RISK ASSESSMENT (the gating question)

| Fix | Risk | What could destabilize |
|-----|------|------------------------|
| **R1** (revert all four lj commits → ship 2e82095) | **LOW** | 2e82095 is the VALIDATED full-Track1 base (per MEMORY: "v1 = single-pod + redirect-to-owner, crc-verified, exceeds 200 target") and the A/B shows it at 77% with `inbound_dropped≈0`. Reverting removes the regression wholesale — no partially-reverted machinery, no new code. The ONLY thing lost is overload defense-in-depth (lj slow-join / spill / 10k soak) that v1 (≤200 single-pod) does not need. Hot path, WT+WS, priority queue, E2EE, crc: all return to the validated baseline by definition. This is the lowest-risk option. |
| **R2** (surgical: remove only the vc-zexm re-warm calls, keep vyg9/j4kz/nys3) | **LOW–MEDIUM** | The five re-warm call removals (`chat_server.rs:2873,888,1824,1879-1882`) are correctness-neutral (`resolve_cached` self-heals; doc-comment `:2870-2872` confirms). Does NOT touch the dispatcher hot path, queue, or transport. Crc/E2EE-neutral. MEDIUM only because it leaves vc-vyg9 *without the vc-zexm fix it was defense-in-depth FOR*, re-exposing the slow-join AllowSet thundering-herd — irrelevant to ≤200 co-arrival v1 but means lj slow-join is unsolved. Needs a targeted slow-join re-test if vyg9 is retained. |
| **R3** (keep vc-zexm, move re-warm off-actor / debounce) | **MEDIUM** | Adds a spawn/debounce on the join path; larger change, touches the actor/fanout-runtime boundary. Crc/E2EE/hot-path-neutral, but more review surface and a new coalescing-correctness consideration. Use only if eager warming must stay. |
| **A1** (per-session orphan reap, on TOP of R) | **LOW** | Runs only on the cold Disconnect / grace-timer path (`chat_server.rs:1391-1435`, `:1574-1660`) — NOT the dispatcher hot path, NOT the per-packet fan-out, NOT the priority queue. Transport-agnostic (Disconnect identical WT/WS). Crc/E2EE-neutral. Behavioral risk: a reconnecting user could lose a member row a few ms earlier; mitigated because reconnection JoinRoom re-inserts the live session. Edge to test: same-sid reconnect (keep `is_reconnection` retain-out as fast path, only ADD superseded-session cleanup). |
| **A2** (atomic member+receiver drop) | **MEDIUM** | Changes reconnection-dedup + the reconnector's "existing members" list (`chat_server.rs:2761-2768`) and the ghost-suppression contract (vc-9g7); touches the synthesized-Disconnect redirect path (`:1371-1389`). Off hot path, transport/crc/E2EE-neutral, but membership-lifecycle blast radius is real. Prefer A1. |
| **B** (keyframe-on-subscribe) | **MEDIUM** | Adds a publish on the JoinRoom path. Under a 200-co-arrival burst this is a fan-out hazard: 200 joins × N senders KFRs could storm presenters and, via presenter keyframe blasts (~1.5 MB each), wedge downlinks — the exact hazard the layer-aware KFR drop (`chat_server.rs:2107-2161`) + per-session limiter exist to bound. MUST reuse the limiter + AllowSet gate and coalesce per-sender. KFR publish is off the dispatcher hot path, crc-neutral, E2EE-neutral, WT==WS. Medium for the burst-amplification footgun, not correctness. Ineffective on the bot test without DEFECT2 sender fixes. |

**Bottom line for the product owner.** The gating regression is **vc-zexm** moving
an O(R·M) sweep onto the single-threaded join/actor path. The minimum-risk move
that **recovers 77% at 200 with NO substantial risk to any stable factor** is
**R1 (revert the four lj commits → ship 2e82095 for v1)** — they are
overload-defense-in-depth not needed for the ≤200 single-pod v1. If the team wants
to preserve lj-2/6/7, **R2** (surgically drop only the vc-zexm re-warm calls) is
LOW–MEDIUM and correctness-neutral. **A1** then closes the residual ~14 baseline at
LOW risk. **B** is the real-client video fix (MEDIUM, gated/coalesced) and does not
move the bot number alone. Recommended v1 package: **R1 + A1** (or **R2 + A1** if
keeping the dispatcher-internal commits).

---

## 5. Impact + bottleneck shift

- **Where Fix R moves the bottleneck:** R restores 2e82095's allocation — the
  O(R·M) AllowSet recompute goes back to LAZY, amortized, on the dispatcher
  hot path (across the W fan-out workers) via `resolve_cached` misses, instead of
  EAGER + serial on the single shard actor. This UN-throttles the join/actor path
  during the burst, so registration completes promptly, the RTT election settles,
  and the reconnect churn that was manufacturing orphans stops. Expected effect:
  NEITHER collapses from 86 back toward the ~14 structural baseline (i.e. recover
  the ~77% both), with `inbound_dropped` still ≈0 (R does not stress the
  dispatcher). The cost goes back onto the hot path but, at ≤200 with
  `inbound_dropped≈0`, there is provable headroom (2e82095 ran it there at 77%).

- **Where A1 moves the bottleneck:** essentially nowhere — O(1) `room_members.retain`
  of one stale sid on the already-cold Disconnect/timer path. On TOP of R it drives
  the residual ~14 baseline toward 0 by making the `(room,user)` reaper account for
  every superseded session.

- **Does R + A1 make v1 hit ~100% at 200?** It recovers the *integration* delivery
  (winners in the fan-out set) — should restore ≥77% both and push the residual
  NEITHER toward 0. It does NOT address the video keyframe gap: the **66 audio-only**
  (and 2e82095's own 32 audio-only) need **Fix B + the DEFECT2 bot-sender fixes**
  (force-KF + keyframe-priority channel). So: **R + A1 ⇒ ~100% AUDIO** at 200;
  **~100% BOTH** additionally needs B + DEFECT2. The brief's "does v1 actually hit
  ~100%" answer: **yes for audio/integration with R + A1; video completeness is a
  separate, bot-harness-gated workstream.**

- **Relation to SCALING-BACKLOG (drain parallelism / video-egress budget / multi-pod):**
  Fix R (esp. R1/R2) **removes** lj-1/2/6/7 from the v1 base — those ARE the scaling
  backlog (slow-join thundering-herd, inbound-drain decoupling, drop-slope recovery,
  remote-pub offload). So R **defers** that backlog rather than conflicting with it:
  the four commits should be re-introduced LATER, on the slow-join / spill / >200
  regimes they target, **with vc-zexm's re-warm made non-blocking (R3-style)** so it
  does not re-regress co-arrival registration. A1 is **orthogonal** to all of it
  (membership-lifecycle correctness). B intersects the video-egress-budget item
  (join-storm KFRs feed presenter keyframe blasts) and must be co-designed with it
  — the per-sender coalescing in B is the hand-off point. Net: R is a v1-scoping
  decision (defer overload defense), not a scaling regression; the backlog items
  return post-v1 with the join-path-cost lesson baked in.

---

## 6. Code citation index

- One-room-one-shard serialization: `chat_server.rs:1079-1098`, `:1126-1129`
- Synchronous `room_members` push: `chat_server.rs:2776-2782`
- Synchronous `receivers.write().insert` + ordering invariant: `chat_server.rs:2914-2936`
- Milestone reads member_count/receiver_set/allowset in one handler: `chat_server.rs:2954-2963`, `:669-695`
- **Disconnect drops receiver but keeps member (the divergence):** `chat_server.rs:1391-1398`
- `drop_room_receiver` (receivers-only removal): `chat_server.rs:1046-1065`
- Grace timer staged, `(room,user)`-keyed: `chat_server.rs:1403-1435`; `RECONNECT_GRACE_PERIOD=2s` `constants.rs:32`
- **Second-disconnect cancels prior timer, overwrites old_session (orphan leak):** `chat_server.rs:1403-1410`
- **ExecutePendingDeparture stale-old_session arm returns without cleanup:** `chat_server.rs:1592-1601`
- Reconnection retains-out only `pending.old_session`: `chat_server.rs:2593-2610`
- Per-packet live receiver snapshot (fan-out): `chat_server.rs:4620-4626`
- Client RTT election (multi-connection, losers disconnect): `videocall-client/src/connection/connection_controller.rs:44-120`
- **REGRESSION — synchronous per-join O(R·M) re-warm on the actor (vc-zexm):** `chat_server.rs:2873` (join), `:888` (leave), `:1824` / `:1879-1882` (actor-message handlers)
- `rewarm_subscription_cache` is O(R) receivers × O(M) `resolve_inner`: `forwarder.rs:701-736`, `subscription.rs:389-431`, `:435-…`
- **2e82095 has ZERO re-warm calls (verified):** `git show 2e82095:actix-api/src/actors/chat_server.rs | grep rewarm` → empty; recompute was lazy on the hot path via `resolve_cached` miss arm (`subscription.rs:resolve_cached`)
- `resolve_cached` self-heals on miss (re-warm is correctness-neutral): `subscription.rs:resolve_cached`; doc-comment `chat_server.rs:2870-2872`
- vc-j4kz per-MEDIA `RegisterRemotePublisher` actor message + `room_state.write()` + conditional re-warm (amplifier): `chat_server.rs:1847-1885`
- vc-vyg9 class-shed only at queue cap (did not fire; `inbound_dropped≈0`): `DISPATCHER_INBOUND_QUEUE_CAP=1024`, `chat_server.rs` const + `plan_shed`/`shed_decision`
- The four lj commits: vc-zexm `d0241be`, vc-vyg9 `95513d5`, vc-j4kz `9113323`, vc-nys3 `bd21d80`
- Forwarder base-keyframe always-forward / no keyframe-on-subscribe: `forwarder.rs:547-553`, `:617-645`
- SFU only RELAYS client KFRs (no server-initiated KFR on join): `chat_server.rs:2125-2163`, `packet_handler.rs:115-116`
- Bot sender drops keyframes / cannot force KF (DEFECT2): `bot/src/video_producer.rs:201-227`, `bot/src/video_encoder.rs:101-103,166`, `bot/src/orchestrate.rs`
- Code delta vs base (no Disconnect/member changes): `git diff 2e82095..89d16d3 -- chat_server.rs`
