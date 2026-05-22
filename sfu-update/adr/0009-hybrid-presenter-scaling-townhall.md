# ADR-0009b: Hybrid Presenter Scaling — Multi-Thread Fan-Out (E2EE base) + Dynamic Town-Hall Audio Mixdown at 500

- **Status:** Proposed (design only — no code changes in this ADR). Awaiting product-owner approval before prototyping on a SCRATCH branch.
- **Date:** 2026-05-22
- **Deciders:** product owner (malexander)
- **Branch of record:** `experimental-sfu` (the "main SFU branch"). This design is to be PROTOTYPED on a scratch branch first (see §Validation Plan).
- **Supersedes:** nothing. **Amends** [ADR-0009 Audio Mixdown Deferred](0009-audio-mixdown-deferred.md) by promoting its enumerated future "town-hall" mode from *deferred* to *designed-and-gated*. The base-case (E2EE-preserved) fan-out/ingest rework is new and does not touch the deferral's crypto invariant.
- **Numbering note:** the prior `0009-audio-mixdown-deferred.md` already occupies slot 0009 (it itself notes the 0006→0009 drift). This document is filed as `0009-hybrid-presenter-scaling-townhall.md` per the task and is referenced internally as **ADR-0009b** to disambiguate. A renumber to 0010 can happen at merge time if the editor prefers; no behavior depends on the number.
- **Root cause of record:** [`PRESENTER-SCALING-ROOTCAUSE.md`](../audits/200bot-monitor/PRESENTER-SCALING-ROOTCAUSE.md).
- **Related:** [ADR-0001 routing-header-out-of-encryption](0001-routing-header-out-of-encryption.md), [ADR-0002 active-speaker-detection](0002-active-speaker-detection.md), [ADR-0004 outbound-priority-queue](0004-outbound-priority-queue.md), [ADR-0005 room-affinity-routing](0005-room-affinity-routing.md), [PLAN.md Open Risk #1](../PLAN.md), [capacity-model.md §4b/§9](../capacity-model.md).

---

## 1. Context

### 1.1 The decision being designed to (from the product owner)

1. **Base case: preserve E2EE, NO audio mixdown.** Fix presenter×receiver scaling by reworking the single-room hot path onto **real threads + parallel ingest**, with media staying end-to-end encrypted. This is the always-on path and is correct regardless of room size.
2. **Dynamically-enabled audio mixdown ("town-hall mode") at 500 participants.** Above a configurable threshold the SFU mixes audio (relaxing E2EE *for audio only*) to collapse the audio fan-out term; below it, full per-presenter E2EE audio. The switch must be **live** (cross the threshold mid-session without dropout) and configurable.

### 1.2 The problem (already root-caused)

Per [`PRESENTER-SCALING-ROOTCAUSE.md`](../audits/200bot-monitor/PRESENTER-SCALING-ROOTCAUSE.md), a single room's entire hot path runs on **one tokio task pinned to one current-thread runtime**:

- `ChatServerPool::new` starts each shard on its own `Arbiter::new()` + `start_in_arbiter` (`actix-api/src/actors/chat_server.rs:1032-1033`). actix-rt gives each arbiter one OS thread running a **current-thread** tokio runtime. Rooms map to shards by jump-hash, so one big room → one shard → **one core**.
- The per-room dispatcher is a single `tokio::spawn` (`chat_server.rs:2926`) inside `spawn_room_dispatcher` (`chat_server.rs:2911`), spawned from the `JoinRoom` handler so it lands on **that same current-thread runtime**.
- Ingest is a single wildcard subscription `room.<room>.*` (`chat_server.rs:2927`, subject from `build_subject_and_queue`, `actix-api/src/models/mod.rs:51-53`), drained by one `sub.next()` (`chat_server.rs:3024`).
- Egress is a **serial** per-receiver loop (`chat_server.rs:3386-3408`), each iteration calling `egress_decide_from_parsed` → `Forwarder::decide` (`actix-api/src/sfu/forwarder.rs:297`).
- **Audio is uncapped** (`forwarder.rs:456`: `allow.audio.contains(&sender) || recv_all_audio`) → audio fan-out is the dominant `P×R` term. Video is capped at `MAX_VISIBLE_VIDEO` (`forwarder.rs:499`).
- Under saturation the 16 384-slot subscription buffer (`actix-api/src/nats_connect.rs:170`) overflows and async-nats **silently drops** inbound; the silence watchdog (`chat_server.rs:3021`+) never trips because the stream is busy, not silent. Late joiners get nothing. `vc-m7k6` adds the inbound-rate sample (`chat_server.rs:2994-3000`, `:3031-3039`) so saturation becomes observable.

Per-second load on the one core is **O(P × R)** (root-cause §2).

### 1.3 The existing crypto model (what "E2EE" means here today)

- Each client owns one symmetric `Aes128State` (`videocall-client/src/client/video_call_client.rs:198`, `:341`). Outbound media is serialized then `aes.encrypt(...)` (audio: `videocall-client/src/encode/microphone_encoder.rs:163`; video: `videocall-client/src/encode/transform.rs:79`; screen: `:124`) and wrapped as `PacketType::MEDIA` (`microphone_encoder.rs:167`).
- A sender's AES key is distributed to each peer **RSA-wrapped** as a `PacketType::AES_KEY` `AesPacket` (`packet_wrapper.rs:213`, `video_call_client.rs:39`). The SFU forwards these opaquely; **it never holds plaintext media or the symmetric key**.
- The SFU reads only the **unencrypted `RoutingHeader`** (`audio_level`, `is_speaking`, layer ids — `microphone_encoder.rs:154-159`), per [ADR-0001](0001-routing-header-out-of-encryption.md). Speaker scoring (`actix-api/src/sfu/speaker.rs`) runs on that header, never on decoded audio, per [ADR-0002](0002-active-speaker-detection.md).

**This is the invariant the base case must preserve and the town-hall mode must relax — for audio only.**

---

## 2. Decision

1. **Base case (always on, E2EE preserved):** Move the per-room **fan-out and ingest off the single-threaded arbiter** onto a **dedicated multi-thread tokio runtime ("fan-out pool")** shared by all rooms on a pod. Shard egress across W worker tasks (receiver-set sharding) and shard ingest across K NATS consumers (subject-hash sharding). The forwarder remains a byte relay; **no decryption**. This is the first buildable piece and is correct for any room size.

2. **Town-hall mode (dynamic, E2EE-relaxed for audio only):** When a room's participant count crosses a configurable threshold (default **500**, env `SFU_TOWNHALL_THRESHOLD`), with **hysteresis** (enter ≥500, exit ≤400, env `SFU_TOWNHALL_EXIT`), the SFU switches that room to **server-side audio mixdown**: decrypt the top-K active speakers' audio (K from `speaker.rs` active set), mix to one Opus stream, forward **one mixed audio stream** to every receiver (audio egress `P×R → 1×R`). **Video stays per-presenter and fully E2EE** and is handled entirely by (1). Below the exit threshold the room reverts to per-presenter E2EE audio. The mode is **per-room**, **dynamic**, and **off by default** (`SFU_TOWNHALL_ENABLE=false`).

3. **Town-hall requires an explicit relaxed-crypto handshake.** A room only enters town-hall if (a) the pod is configured to allow it AND (b) clients have negotiated the `TOWN_HALL_AUDIO` capability (a `client_capabilities` bit per [ADR-0001 §4]). Clients that have not consented keep sending E2EE audio and are **not** mixed (their audio is simply forward-all to the small set still able to receive it, or dropped from the mix — see §4.3). No client's audio is decrypted server-side without that client having opted into the town-hall capability.

---

## 3. Design — Part A: Base case (E2EE preserved): multi-thread fan-out + parallel ingest

### 3.1 The fan-out pool (escape the single-thread arbiter)

**Problem:** today the dispatcher shares a current-thread runtime with the room's actor mailbox (`chat_server.rs:1032-1033`, dispatcher spawned at `:2926` from the `JoinRoom` handler). They are mutually exclusive in time; one big room is pinned to one core.

**Design:** introduce a **process-wide multi-thread tokio runtime** dedicated to fan-out, owned by `ChatServerPool` (`chat_server.rs:1006`) and handed to each shard. Concretely:

- Build a `tokio::runtime::Builder::new_multi_thread().worker_threads(N).build()` once at pool construction (`ChatServerPool::new`, `chat_server.rs:1019`), where `N` defaults to the online CPU count (reuse `linux_cpu_load_estimate`'s CPU-count helper at `actix-api/src/sfu/health_beacon.rs:152-159`) and is overridable via `SFU_FANOUT_WORKER_THREADS`. Store its `Handle` in `ChatServerPool` and pass a clone into each shard / each `spawn_room_dispatcher` call.
- `spawn_room_dispatcher` (`chat_server.rs:2911`) **stops calling bare `tokio::spawn`** (which inherits the arbiter's current-thread runtime, `chat_server.rs:2926`) and instead calls `fanout_handle.spawn(...)`. Now the dispatcher runs on the multi-thread pool; the actor mailbox keeps its arbiter thread. They run **in parallel**, so `JoinRoom` registration is no longer blocked by an in-progress fan-out (root-cause §1.3).

**Coexistence with the actix actor model:** the actor (mailbox, `JoinRoom`/`Leave`/registration) stays on its arbiter exactly as today (`chat_server.rs:1032-1033`). Only the **dispatcher closure** moves to the fan-out pool. The shared state it touches is already `Arc`/lock-guarded for cross-thread access:
- `receivers: Arc<RwLock<HashMap<SessionId, Recipient<Message>>>>` (`chat_server.rs:2918`) — already `std::sync::RwLock`, safe across threads.
- `room_state: Arc<RwLock<RoomState>>` (`chat_server.rs:2923`).
- `forwarder: Arc<Forwarder>` (`chat_server.rs:2916`), `scorer: Arc<TokioRwLock<SpeakerScorer>>` (`chat_server.rs:2917`).
- `Recipient<Message>` is `Send` (actix `Recipient` is `Arc`-backed); `try_send` (`chat_server.rs:3397`) is the cross-thread-safe handoff into the receiver's actor mailbox. **This is the load-bearing fact that makes the move legal:** delivery already crosses task boundaries via `try_send`, so moving the producer to another thread changes only *where* the work runs, not the delivery contract.

> **Audit gate (mandatory before merge):** confirm with **backend-rust-streaming** and **code-reviewer** that every captured field is `Send + Sync` and that no `actix::Context`-bound (`!Send`) handle is captured into the dispatcher closure once it runs on a multi-thread runtime. The `RoomDispatcherExited` / watchdog notifications back to the actor must use `Addr::try_send` (already the pattern at `chat_server.rs:2939`), which is `Send`.

### 3.2 Egress sharding (the `P×R → P×R/W` win)

The serial loop at `chat_server.rs:3386-3408` is replaced with a **receiver-set-sharded** fan-out:

- Snapshot receivers once (already done at `chat_server.rs:3373-3379`).
- Partition the snapshot into W shards by `hash(SessionId) % W`. For each shard, `fanout_handle.spawn` a task that runs the existing per-receiver `egress_decide_from_parsed` + `try_send` (`chat_server.rs:3387-3407`) over its slice. `join_all` (or a barrier) before advancing to the next inbound packet, OR pipeline (see below).
- `parse_and_inspect` still runs **once** per inbound message (vc-q0v, `chat_server.rs:3504` doc), and the parsed `Arc` is shared read-only across shard tasks — preserving the parse-once property while parallelizing the decide+send.

**Wall-time per packet drops from `R×decide` to `≈ (R/W)×decide`** on the fan-out pool, the dominant egress term. W defaults to the fan-out pool worker count.

> **Design choice (pipelined vs. barrier):** a strict per-packet barrier preserves in-order delivery per receiver trivially. A pipelined model (per-receiver-shard work queues fed by the ingest stage) yields higher throughput but must preserve **per-(sender,receiver) ordering** — media decode is order-sensitive. **Decision:** start with the **barrier** model (correct, simple) and only move to per-shard persistent worker queues if profiling shows the join overhead dominates. Defer the pipelined variant to a follow-up bead; do not block the base-case win on it.

### 3.3 Parallel / subject-sharded ingest (kill the single `sub.next()` choke)

The single wildcard `room.<room>.*` consumer (`chat_server.rs:2927`, `:3024`) serializes all P presenters through one buffer. Replace it with **K parallel consumers**, each subscribing to a **subject shard**:

- Today every presenter publishes to `room.<room>.<their_session>` (`actix-api/src/actors/session_logic.rs:615`, `:767`). The wildcard `*` matches all. To shard without changing the publish subject, use NATS **queue-group-free partition by subject token**: subscribe K consumers to `room.<room>.<shardN>.*` only if we re-key the publish subject — which would be a wire change. **Cheaper, no-wire-change option:** keep the single subject but front it with K consumers using a **deterministic hash of the session token** is NOT possible on a plain `*` wildcard (NATS routes a wildcard match to one subscriber instance unless queue groups are used, and queue groups load-balance non-deterministically, breaking per-sender ordering).

  **Decision:** introduce a **subject shard token** in the publish subject: `room.<room>.<shard>.<session>` where `shard = hash(session) % K`. The SFU subscribes K consumers to `room.<room>.<shard>.*`. Each presenter's stream lands on exactly one consumer (ordering preserved per sender), and the K consumers run concurrently on the fan-out pool. This changes `build_subject_and_queue` (`models/mod.rs:51-53`) and the publish path (`session_logic.rs:615`) — a **coordinated client+server wire change**, gated behind the same feature flag and capability as everything else here.

  **Migration:** during rollout the SFU subscribes to BOTH `room.<room>.*` (legacy, K=1) and `room.<room>.*.*` (sharded) so old clients keep working; the active set is chosen by the negotiated capability. Old behavior = K=1 = exactly today.

- Each of the K ingest consumers feeds the **same** egress sharding stage (§3.2). Speaker scoring (`scorer.write().await`, `chat_server.rs` audio arm; `speaker.rs:94 observe`) must be fed from all K consumers; the scorer is already `Arc<TokioRwLock<_>>` so concurrent `observe` calls serialize on the lock — acceptable (it is a cheap EWMA update), but profile it; if the scorer write becomes hot, batch observations per tick.

> **Audit gate:** **backend-rust-streaming** must confirm the NATS subject-shard scheme against the existing affinity/spillover subjects (`actix-api/src/sfu/affinity.rs`, `spillover.rs`) so `room.*.system` (`chat_server.rs:3441`) and beacon subjects don't collide with the new shard token.

### 3.4 Cost model after the base-case fix (quantitative)

Let P = active presenters, R = receivers, f = frames/sec/presenter, C = fan-out pool worker threads (≈ pod cores), W = egress shards, K = ingest shards.

- **Before:** `core_load ≈ (P×f) × (R×decide)` on **1 core** (root-cause §2).
- **After egress sharding (§3.2) + multi-thread pool (§3.1):** egress work is spread across C cores: `per_core_load ≈ (P×f) × (R×decide) / C`.
- **After ingest sharding (§3.3):** ingest parse + scorer feed is spread across min(K, C) cores; the single-consumer buffer-overflow choke (root-cause §3.2) is gone — each consumer drains `P/K` presenters into a `16384`-slot buffer.

**New ceiling:** the work is still **O(P×R)** in total, but now divided by **C cores** instead of 1. So the base case scales the presenter side by **≈ C×** (4× on the current 4-CPU pod; near-linear in cores up to memory-bandwidth / NATS-throughput limits). Concretely, the root-cause's worst observed shape — **P=20, R=400, ~400k audio decides/sec on one core** — becomes **~100k/core across 4 cores**, comfortably within a core's budget. **This is what makes late joiners reliable at 20p×400r while keeping full E2EE.**

**Where the base case finally tops out:** audio fan-out is uncapped (`forwarder.rs:456`), so audio egress remains `P×R` *in aggregate* even after sharding — at very large R (the 500+ webinar) the **aggregate `P×R` audio work eventually exceeds even C cores**, AND the per-receiver **bandwidth** term (`N_send_audio × R_audio`, [ADR-0009 audio-mixdown-deferred §Context]: `200×32kbps = 6.4 Mbps/receiver`) saturates the NIC long before CPU. **That is precisely the regime town-hall mode (Part B) is designed for.** The base case buys ≈C× headroom and full E2EE; town-hall collapses the audio term when the room is large enough that the bandwidth/CPU arithmetic no longer closes on one pod.

---

## 4. Design — Part B: Town-hall mode (dynamic audio mixdown at 500)

### 4.1 Trigger, threshold, hysteresis, and where the decision lives

- **Count source:** `RoomState::member_count()` — already maintained on every membership mutation and read under a short lock by the health-beacon hub (`actix-api/src/sfu/health_beacon.rs:391-393`). This is the authoritative per-room participant count. Reuse it; do not add a parallel counter.
- **Decision point:** the **beacon-hub tick** (`spawn_beacon_hub_with_interval`, `health_beacon.rs:343`+) already snapshots `member_count()` per room on a fixed interval. Extend that tick to evaluate the town-hall predicate per room (it is the natural, already-throttled, off-hot-path place to make a per-room mode decision — it does **not** add work to the fan-out pool). Mirror the spillover predicate pattern (`actix-api/src/sfu/spillover.rs:90-96, :141-149`).
- **Threshold + hysteresis (anti-flap):**
  - Enter town-hall when `member_count() >= SFU_TOWNHALL_THRESHOLD` (default 500) for ≥ `SFU_TOWNHALL_ENTER_TICKS` consecutive ticks.
  - Exit when `member_count() <= SFU_TOWNHALL_EXIT` (default 400) for ≥ `SFU_TOWNHALL_EXIT_TICKS` consecutive ticks.
  - The 100-participant dead-band + the consecutive-tick debounce prevents flapping at the boundary, mirroring the de-aligned-timer / debounce discipline already used for the watchdog (`chat_server.rs:3005-3019`) and spillover.
- **Mode state:** add a `RoomMode { Standard, TownHall }` field to `RoomState` (`actix-api/src/sfu/room_state.rs:178`+), flipped only by the beacon-hub evaluator. The dispatcher (`chat_server.rs`) and forwarder read it on the hot path via the `Arc<RwLock<RoomState>>` it already holds (`chat_server.rs:2923`).

### 4.2 Mixdown architecture (audio only)

When a room is in `TownHall`:

1. **Speaker selection (reuse existing).** The active-speaker set is already computed from `RoutingHeader.audio_level` EWMA (`speaker.rs:94 observe`, `top_n` `:133`, `MAX_SPEAKERS=4` `:159`, `ActiveSpeakerSet` published via `NatsSpeakerPublisher` `:240`). Town-hall mixes the **top-K active speakers** (K = `MAX_SPEAKERS`, configurable via `SFU_TOWNHALL_MIX_K`). Non-speaking presenters contribute silence and are excluded from the mix — this is what makes the mix independent of P.
2. **Decrypt → decode → mix → encode (new mixer stage).** A per-room **mixer task** on the fan-out pool:
   - decrypts each top-K speaker's audio `MediaPacket` using the room key (§4.4),
   - decodes Opus → PCM (the SFU gains an Opus codec dependency — reuse the codec crates already in the workspace; confirm with **backend-rust-streaming** which Opus binding is acceptable server-side),
   - sums/normalizes PCM across the ≤K active speakers (simple additive mix with soft clipping; K≤4 keeps this cheap),
   - re-encodes one Opus stream,
   - re-wraps as a single `MediaPacket`/`PacketWrapper` and publishes it as the room's **mixed audio stream** on a reserved subject `room.<room>.__mix__` (a synthetic sender id `__townhall_mix__`).
3. **Egress collapse.** The dispatcher forwards the single mixed audio packet to every receiver. Audio egress goes from `P×R` (forward-all) to **`1×R`** (one mixed stream). The forwarder's audio admit (`forwarder.rs:456`) is bypassed for the synthetic mixed sender (it is always admitted to everyone). Per-presenter audio packets from town-hall-capable clients are **not** fanned out (they only feed the mixer + scorer).
4. **Video is untouched.** Video/screen stay per-presenter, E2EE, capped at `MAX_VISIBLE_VIDEO` (`forwarder.rs:499`), fanned out by Part A. The mixer touches **audio only**. Confirm video fan-out is handled by §3.

**Failure containment:** if the mixer task errors (decode failure, codec panic), the room **falls back to forward-all audio** for that tick rather than going silent — a mixer hiccup must not black-hole the room (this was an explicit Con of mixdown in [ADR-0009 §Consequences]). The mixer runs in its own task with a catch + counter; on repeated failure the beacon evaluator forces the room back to `Standard`.

### 4.3 E2EE relaxation — key handling (the security-critical part)

> **⚠️ SUPERSEDED by [§4 Revision (post-security-review)](#4-revision-post-security-review).** The web-security-auditor returned a **BLOCKER**: the codebase has **no audio/video key separation** — there is exactly one `Aes128State` per client (built at `video_call_client.rs:198/:341`, handed to every encoder) and `AesPacket` carries only `{key, iv}` (`protobuf/types/aes_packet.proto:3-6`, serialized at `video_call_client.rs:1598-1606`) with no media-type tag, key-id, or epoch; encryption is AES-128-**CBC** with a **static reused IV** (`aes.rs:31, :47-58, :77-79`) and key transport is RSA **PKCS#1 v1.5** (`rsa.rs`). So the original §4.3/§4.4 claim — "distribute a room AUDIO key; the SFU never gets the video key" — is **structurally false today**: any key handed to the SFU is indistinguishable from a full media key and would decrypt video too. The §4 Revision below redesigns this concretely. The §4.3/§4.4/§4.5 text is retained for history but is **not the design of record**.

The SFU **must** access audio plaintext to mix. The design must make this **explicit, opt-in, and scoped to audio**:

- **Room audio key, not per-sender keys.** In town-hall mode, town-hall-capable clients encrypt their **audio** with a **room audio key** that the SFU also holds, instead of (or in addition to) their personal `Aes128State`. The room audio key is generated by the room owner / first town-hall participant and distributed to (a) every town-hall-capable client and (b) the SFU, RSA-wrapped, using the **existing `AES_KEY` / `AesPacket` exchange** (`packet_wrapper.rs:213`, `video_call_client.rs:39`). The SFU is treated as one more recipient of the wrapped room audio key. **Video keeps using each sender's personal `Aes128State`** (`transform.rs:79`) — the SFU never gets the video key.
- **Why a room key, not "send audio plaintext":** sending audio in the clear to the SFU would also expose it on the wire to any NATS-path observer. A room audio key keeps audio encrypted in transit and decryptable only by the room+SFU, which is the minimal relaxation. (Final scheme — room key vs. SFU-as-group-member — to be ratified by **web-security-auditor**; the room-key approach is the recommendation.)
- **Client change:** when a client is in town-hall audio mode, `transform_audio_chunk` (`microphone_encoder.rs:114`) encrypts with the room audio key (`microphone_encoder.rs:163`) rather than the personal AES state. Video transform (`transform.rs`) is unchanged.
- **Strict scope:** the SFU decrypts **audio MediaPackets only**, only in `TownHall` mode, only from clients that presented the `TOWN_HALL_AUDIO` capability. The forwarder/dispatcher must assert media_type == AUDIO before any decrypt path is reachable. Non-consenting clients' audio is never decrypted; it is excluded from the mix (their audio simply isn't carried in town-hall mode — see consent UX in §4.5).

### 4.4 Client signaling + the live transition

- **Capability negotiation (entry gate):** add a `TOWN_HALL_AUDIO` bit to `client_capabilities` ([ADR-0001 §4] bitmask, the `p1-3` pattern). Clients advertise it at connect via the existing `CONNECTION` packet (`PacketType::CONNECTION = 4`, `packet_wrapper.rs:217`). A room only enters town-hall if all (or a configurable quorum of) audio senders advertise it; otherwise the room stays `Standard` even above 500 (and the operator sees a `townhall_blocked_no_capability` counter).
- **Mode-switch control packet:** the SFU announces the room mode change with a **control message** broadcast to the room (reuse the `room.<room>.system` subject used for meeting-info, `chat_server.rs:3441`, and the `CONNECTION`/system packet path). The control packet carries `{ mode: TownHall|Standard, mix_subject, room_audio_key_epoch }`. New joiners above threshold receive the current mode in their join response so they **start in town-hall directly**.
- **Live transition Standard → TownHall (room crosses 500 mid-session):**
  1. Beacon evaluator flips `RoomState.mode = TownHall` (after the enter-debounce).
  2. SFU obtains/derives the room audio key and distributes it (RSA-wrapped) to the SFU and to all capable clients; broadcasts the mode-switch control packet.
  3. **Overlap window:** for `SFU_TOWNHALL_TRANSITION_MS` (e.g. 1–2 s) the SFU forwards **both** per-presenter E2EE audio (old) AND the mixed stream (new). Clients, on receiving the control packet, switch their audio **decode** from per-presenter to the single mixed stream, and switch their **encode** to the room audio key. The overlap covers in-flight packets and clock skew so there is **no audio dropout**.
  4. After the window, the SFU stops forwarding per-presenter audio; only the mix flows. Clients tear down per-peer audio decoders, keep per-peer **video** decoders.
- **Reverse transition TownHall → Standard (drops below 400):** symmetric. SFU broadcasts `mode: Standard`; during the overlap window it forwards both the mix and resumes per-presenter audio; clients switch decode back to per-presenter and encode back to personal AES. The room audio key is retired.
- **New joiners above threshold:** join straight into town-hall (mode in join response), decode the mix immediately, encode with the room audio key. No per-presenter audio decoders are ever created for them.

> **Audit gate:** the live transition touches connection/session lifecycle. Per CLAUDE.md Change-Impact policy it must be traced for BOTH WebTransport and WebSocket and validated by **frontend-rust-webtransport-and-websocket** (client decode/encode switch, overlap handling) and **backend-rust-streaming** (dual-forward window, key distribution). The overlap window length must tolerate 200ms+ links (CLAUDE.md real-world-networks rule) — it is a configurable duration, not a hardcoded localhost value.

### 4.5 Security posture (state precisely what is/isn't E2EE)

| Plane | Standard mode (<500) | Town-hall mode (≥500) |
|---|---|---|
| **Video / screen** | E2EE end-to-end. SFU forwards opaque ciphertext (personal `Aes128State`, `transform.rs:79/:124`). | **E2EE end-to-end (unchanged).** SFU never holds video key. |
| **Audio** | E2EE end-to-end. SFU forwards opaque ciphertext (`microphone_encoder.rs:163`). | **Relaxed: SFU decrypts audio** (room audio key) to mix. Audio is NOT end-to-end opaque to the server in this mode. |
| **Routing header** | Cleartext metadata only (`audio_level`, `is_speaking`, layers) per [ADR-0001]. | Same. |
| **Consent** | n/a | Required: `TOWN_HALL_AUDIO` capability; user-visible indicator that audio is server-mixed (UX via **ux-ui-expert**). |

**The tradeoff, stated plainly:** town-hall mode trades **audio E2EE** for the ability to host 500+ participants on bounded infrastructure. Video remains E2EE in both modes. This is the relaxation [ADR-0009 audio-mixdown-deferred] deferred and PLAN Open Risk #1 flagged; this ADR makes it explicit, opt-in, dynamically scoped, and reversible. **web-security-auditor must sign off** on: the room-audio-key distribution, the capability/consent gate, the trust-indicator UI (so users know audio is no longer E2EE), and the assertion that video keys never reach the SFU.

---

## 5. Design — Part C: Observability, config, safety

### 5.1 Config (all env, parsed in `actix-api/src/sfu/config.rs:120 from_env`, mirroring existing `SFU_*` parse-and-warn discipline `:155-265`)

| Env | Default | Meaning |
|---|---|---|
| `SFU_FANOUT_WORKER_THREADS` | #cores | Fan-out pool size (§3.1) |
| `SFU_EGRESS_SHARDS` (W) | = workers | Egress receiver shards (§3.2) |
| `SFU_INGEST_SHARDS` (K) | 1 (off) | Subject ingest shards (§3.3); 1 = today's behavior |
| `SFU_TOWNHALL_ENABLE` | false | Master switch for town-hall (off by default) |
| `SFU_TOWNHALL_THRESHOLD` | 500 | Enter participant count |
| `SFU_TOWNHALL_EXIT` | 400 | Exit participant count (hysteresis) |
| `SFU_TOWNHALL_ENTER_TICKS` / `_EXIT_TICKS` | 3 / 3 | Debounce |
| `SFU_TOWNHALL_MIX_K` | `MAX_SPEAKERS` (4) | Speakers in the mix |
| `SFU_TOWNHALL_TRANSITION_MS` | 1500 | Dual-forward overlap window |

### 5.2 Metrics (Prometheus, alongside `SFU_DROPPED_TOTAL` `forwarder.rs:512` and the vc-m7k6 inbound-rate sample `chat_server.rs:2994-3000`)

- `sfu_room_mode{room}` gauge (0=Standard, 1=TownHall) — verify mode per room.
- `sfu_townhall_active_rooms` gauge.
- `sfu_audio_egress_streams{room}` gauge — should read ~1 in town-hall, ~P in standard (the headline collapse, verifiable directly).
- `sfu_mixer_failures_total{room}` counter + `sfu_mixer_encode_seconds` histogram.
- `sfu_townhall_transitions_total{direction}` counter; `sfu_townhall_blocked_no_capability_total`.
- `sfu_fanout_egress_shard_lag_seconds` (per-packet join wall-time, §3.2) — confirms the egress parallelism is real.
- `sfu_dispatcher_inbound_rate` (vc-m7k6, already designed) per ingest shard — confirms ingest sharding spread.
- Reuse the saturation signal (vc-m7k6) to detect that the base-case fix actually removed the silent-drop regime.

### 5.3 Safety

- **Kill switch:** `SFU_TOWNHALL_ENABLE=false` (default) makes town-hall code unreachable — the room stays in Part A behavior at any size.
- **Graceful degradation:** mixer failure → forward-all fallback for the tick; repeated failure → forced `Standard` (§4.2).
- **No regression for small rooms:** `K=1`, `W=workers` with one big-enough pool reduces to today's path semantically; the parse-once + forwarder logic is unchanged.

---

## 6. Validation / Practice Plan (test the plan; practice before main)

**Hard rule (per CLAUDE.md local-only + this task):** prototype on a **scratch branch**, NOT on `experimental-sfu`. Merge to `experimental-sfu` only after validation. No push to origin without explicit go-ahead.

### 6.1 Branch + flag staging

1. `git switch -c scratch/townhall-prototype` off `experimental-sfu`.
2. Everything lands behind flags defaulting to **today's behavior** (`SFU_TOWNHALL_ENABLE=false`, `SFU_INGEST_SHARDS=1`). The base-case fan-out pool (§3.1/§3.2) is the only always-on change and must be a behavior-preserving refactor (parse-once + forwarder semantics identical; verify against `sfu::tests::forwarder_parity_tests`, `chat_server.rs:3453`).
3. Validate on scratch → only then `git switch experimental-sfu` and merge locally.

### 6.2 Bot-harness test (the 500-crossing live-switch test)

Using the existing 200-bot monitor harness (`sfu-update/audits/200bot-monitor/`, `sfu-update/bot-spec.md`):

- **T1 — base-case scaling (E2EE, no town-hall):** P=20, R=400, town-hall disabled. Assert crc>0 for **late joiners** (the regression that root-caused this), assert `sfu_dispatcher_inbound_rate` spread across K shards, assert no `SlowConsumer` drop regime (vc-m7k6 saturation metric flat). This proves Part A independently.
- **T2 — cross 500 live:** ramp a single room past 500 with town-hall enabled. Assert: `sfu_room_mode` flips to 1 after the enter-debounce; `sfu_audio_egress_streams` drops to ~1; **in-flight bots experience no audio dropout** across the transition (continuity check over the `SFU_TOWNHALL_TRANSITION_MS` window); late joiners arriving above threshold get the mix immediately (crc>0).
- **T3 — reverse transition:** drain below 400; assert mode→Standard, per-presenter audio resumes, no dropout, video continuous throughout both transitions.
- **T4 — both-modes crc:** assert crc>0 for video in BOTH modes (video path unchanged) and for audio in BOTH modes (per-presenter below, mixed above).
- **T5 — flap resistance:** oscillate around 450–550; assert no mode thrash (debounce + dead-band hold).
- **T6 — capability gate:** mixed capable/non-capable bots; assert non-capable audio is never decrypted (negative test) and `townhall_blocked_no_capability` increments when quorum not met.

### 6.3 Staged rollout

scratch (build + T1–T6 green) → merge to `experimental-sfu` → soak (reuse `soak-10k`, `stress-500-1000` harnesses) with town-hall **disabled** first (prove Part A in soak), then enable town-hall in soak. Helm/K8s flag wiring via **deploy-sync-expert**. E2E user-facing transition (the audio-mode-switch UX + trust indicator) via **e2e-test-sync** and **ux-ui-expert**.

---

## 7. Bead Breakdown (build order, after approval)

Dependencies flow top-down. **B1 is the first buildable piece — needed regardless of town-hall and preserves E2EE.**

| Bead | Scope | Surface | Depends on | Notes |
|---|---|---|---|---|
| **B1 (P0, FIRST)** | Fan-out multi-thread pool: move dispatcher off arbiter onto a pool `Handle` (`chat_server.rs:2911/2926/1019`) | SFU | — | Behavior-preserving; E2EE intact. Verify with parity tests. **code-reviewer + backend-rust-streaming gate on Send/Sync.** |
| **B2 (P0)** | Egress receiver-set sharding (`chat_server.rs:3386-3408`), barrier model | SFU | B1 | Parse-once preserved. perf-reviewer on shard count. |
| **B3 (P1)** | Subject-sharded ingest + wire subject change (`models/mod.rs:51`, `session_logic.rs:615`); dual-subscribe migration | SFU + client + bot | B1 | Coordinated wire change; client via frontend agent. |
| **B4 (P1)** | `RoomMode` state + beacon-hub evaluator with threshold/hysteresis (`room_state.rs:178`, `health_beacon.rs:343/391`, `spillover.rs:90` pattern). **REVISED (§4 Rev):** add `SFU_TOWNHALL_THRESHOLD_FLOOR`; count must come only from the authenticated owner-pod `member_count()` (`health_beacon.rs:391`), never a client-asserted count — sybil/count-inflation defense. | SFU | B1 | Off-hot-path decision. |
| **B5 (P1)** | `TOWN_HALL_AUDIO` capability bit ([ADR-0001 §4]) + **authenticated** mode-switch control packet on `room.*.system` (`chat_server.rs:3441`, `PacketType::CONNECTION` `packet_wrapper.rs:217`). **REVISED (§4 Rev R3):** packet carries `{mode, key_id, epoch, owner_pod_id, mac/sig}`; clients **refuse unauthenticated/forged switches** and refuse downgrade without a valid owner-pod signature. | SFU + client + types | B4 | **web-security-auditor mandatory** (forged-downgrade + count-inflation defense). |
| **B6 (P1) — RE-SCOPED (§4 Rev R1+R2)** | **Media-scoped key type.** New `MediaScope`-tagged key: extend `AesPacket` (`aes_packet.proto:3-6`) with `scope`, `key_id`, `epoch`, `cipher` fields; add **AES-128-GCM with per-message nonce** (`aes.rs:72-92` gains a GCM path); generate a **separate** town-hall audio key on the client, distinct from the personal `Aes128State` (`video_call_client.rs:198`). The personal video/screen key is **NEVER** serialized into a `scope=AUDIO_TOWNHALL` packet and **NEVER** sent to the SFU. RSA transport hardened (PKCS#1 v1.5 → OAEP, `rsa.rs`). | client + SFU + types | B5 | **web-security-auditor mandatory.** Carries the **negative test** (R1): SFU rejects any key whose `scope ≠ AUDIO_TOWNHALL`; CI fails if a video-usable key ever reaches the SFU. |
| **B7 (P0 of town-hall) — REVISED (§4 Rev R5)** | Mixer task: **fail-closed decrypt site** — hard assert `mode==TownHall && media_type==AUDIO && sender has TOWN_HALL_AUDIO cap && key.scope==AUDIO_TOWNHALL && key.epoch==current` before any decrypt; decode→mix top-K→encode→publish `room.*.__mix__`; forward-all fallback; **no plaintext PCM/Opus logging**; **zeroize** room keys on `mode!=TownHall`. (`speaker.rs top_n`, new Opus dep.) | SFU | B6 | backend-rust-streaming on Opus binding; **web-security-auditor on the decrypt-gate + zeroization.** Carries the assertion metric + negative test (R5). |
| **B8 (P1)** | Egress collapse: forward mix as single audio stream; bypass `forwarder.rs:456` for synthetic mixed sender. **REVISED:** non-consenting senders' audio is forwarded per-presenter E2EE to consenting+capable receivers only, never decrypted (R4 outcome). | SFU | B7 | |
| **B9 (P1) — REVISED (§4 Rev R3+R4)** | Client live transition: decode/encode switch + overlap window, both WT & WS; **verify owner-pod signature on the switch packet before acting**; **affirmative per-session user-consent prompt** before first audio is sent under the room key; epoch-bump key rotation on each entry; key zeroization on exit/teardown. | client | B5,B6 | frontend agent; e2e-test-sync; **web-security-auditor on consent flow.** |
| **B10 (P2)** | Metrics + config (§5 + §4 Rev): mode gauge, `sfu_townhall_decrypt_scope_violations_total`, `sfu_townhall_key_epoch{room}`, `sfu_townhall_consent_declined_total`, `sfu_townhall_unauth_switch_rejected_total`. | SFU | B4,B7 | |
| **B11 (P1) — REVISED (§4 Rev)** | Bot-harness T1–T7 + soak wiring; adds **T-SEC** suite: scope-violation negative test (R1), unauthenticated-switch rejection (R3), count-inflation/sybil rejection (R3), consent-decline outcome (R4), zeroization-on-exit (R5). | bot + helm | B2,B9 | integration-test-writer; deploy-sync-expert. |
| **B12 (P2) — REVISED (§4 Rev R4)** | **Persistent, non-dismissable** "Audio is not E2EE — processed by the server" trust indicator (security-critical rendering); pre-consent modal copy stating the server-side-recording property. | client UI | B5,B9 | **ux-ui-expert + web-security-auditor** (trust-indicator is security-critical per CLAUDE.md). |

**Track 1 (E2EE-base, ship independently — UNAFFECTED by §4 Rev):** B1→B2→B3→B11(T1). **Track 2 (town-hall, gated, re-scoped by §4 Rev):** B4→B5→**B6 (crypto foundation: media-scoped key + GCM + OAEP — now the long pole)**→B7→B8/B9→B10→B11(T2–T7 + T-SEC)→B12.

---

## 8. Consequences

**Pro:**
- Base case scales the presenter side ≈C× (4× now) with **E2EE fully preserved** — fixes the late-joiner failure without any crypto relaxation; correct at any room size; ships first and independently.
- Town-hall collapses audio egress `P×R→1×R` and audio bandwidth `200×32kbps→1×48kbps` per receiver, making 500+ rooms fit bounded infra; dynamic, reversible, opt-in.
- Video stays E2EE in **both** modes.
- Reuses existing machinery: member-count + beacon hub for the trigger, speaker EWMA for top-K, `AES_KEY` exchange for key distribution, `CONNECTION`/system subject for signaling, vc-m7k6 for saturation observability.

**Con / risks:**
- Town-hall **relaxes audio E2EE** server-side — the core security tradeoff; requires explicit consent + trust UI + security sign-off. Stated plainly in §4.5.
- Moving the dispatcher to a multi-thread runtime requires a careful `Send/Sync` audit of every captured handle (§3.1 gate) — the highest-risk part of Part A.
- Ingest subject-sharding is a coordinated wire change (B3) with a dual-subscribe migration; mis-sequenced rollout could split a room across schemes — mitigated by capability gating.
- Mixer is a per-room single point of audio failure; mitigated by forward-all fallback + forced-Standard on repeated failure (§4.2).
- The live transition is lifecycle-sensitive (WT + WS, high-latency links); must be traced end-to-end per CLAUDE.md Change-Impact policy.

**Explicitly NOT done in this ADR:** no code changes; no final ratification of the room-key vs. SFU-group-member crypto scheme (web-security-auditor decides); no capability-bit value reserved yet; no PLAN.md/capacity-model.md edits.

---

## Performance Review (performance-reviewer)

Scope: Part A only (E2EE-base multi-thread fan-out + parallel ingest). Reviewed against current source on `experimental-sfu`. **Verdict: the direction is sound and the C× claim is *directionally* correct, but the headline "≈4× / near-linear in cores" is over-stated as written, and the ADR mislocates the lock-contention risk.** Two design changes are required before prototyping (per-shard `recent_t0`, batched scorer feed) and the cost model needs the corrections below. Line numbers in §1–§3 of the ADR are stale relative to current source — corrected citations are inline below.

### A. Citation drift (fix before prototyping so the beads target real lines)

The ADR/root-cause cite pre-refactor line numbers. Current source:
- Egress fan-out loop: `chat_server.rs:3386-3408` (ADR is correct here).
- Per-receiver `format!` + `.replace(' ', "_")`: **`chat_server.rs:3532`** (ADR/root-cause say `:3487`). It is **two** heap allocations per receiver per packet (the `format!` String *plus* the `.replace` which always allocates a new String even when no space is present).
- Dispatcher `tokio::spawn`: **`chat_server.rs:2926`** (fn signature `spawn_room_dispatcher` at `:2911`, the spawn at `:2926`). Correct.
- `Forwarder::decide`: `forwarder.rs:297`. Room read lock `forwarder.rs:340`; subscriptions read lock `forwarder.rs:417-432`; `recent_t0.write()` **`forwarder.rs:608`** (correct), `recent_t0.read()` `:614`.
- Scorer feed on the ingest task: **`chat_server.rs:3218`** (`scorer.write().await.observe(...)`), not the `:3173` the root-cause cites.
- NATS subscription buffer: **`16 * 1024` at `nats_connect.rs:196`** (ADR says `:170`; value 16384 correct, line stale).

### B. Lock-contention risk — the ADR mislocates it (the most important correction)

Per-lock answer to "do shared locks erode the C× gain under W parallel shard-workers":

1. **`room.read()` (`forwarder.rs:340`) and `subscriptions.read()` (`forwarder.rs:417`) are `std::sync::RwLock` *read* acquisitions.** W shard-workers all taking read locks **do not block each other** — `RwLock` permits concurrent readers. Inside, the AllowSet lookup is a `DashMap` cache hit (`subscription.rs:289`, lock-free shard read returning `Arc::clone`). **These are NOT a contention bottleneck for parallel egress.** Readers serialize only against a concurrent *writer* (join/leave membership bump, or a `resolve_cached` *miss* doing `cache.insert` at `subscription.rs:324`) — both off-hot-path or rare. **The ADR's worry here is largely unfounded — good.** *Caveat:* `SFU_ROOM_SIZE...set()` (`forwarder.rs:344`) and `observe_decide_latency` run per receiver and hit shared Prometheus atomics; under W threads this is modest contended-atomic cache-line bouncing (not a lock). Set the room-size gauge **once per packet before the shard fan-out**, not per receiver inside `decide`.

2. **`recent_t0.write()` (`forwarder.rs:608`) IS a single per-room exclusive `std::sync::RwLock` write taken *inside* the per-receiver path — the real contention hazard the ADR does NOT address.** Under W parallel shard-workers, every worker handling a video **T0 delta** (`!is_keyframe && temporal_layer_id == 0`, `forwarder.rs:604-612`) contends for **one** room-wide write lock, fully serializing those workers for the write's duration. **Required design change: shard `recent_t0` per egress shard, or convert to `DashMap<(SessionId,SessionId), RecentT0Set>` so writes hit independent shards keyed by `(receiver, sender)`.** The key is already `(receiver, sender)` and receivers are partitioned by `hash(SessionId)%W`, so a per-shard map is the clean fix. **Put this in B2's design, not deferred.**

   **However — and this materially changes the C× story — `recent_t0` is on the VIDEO path only. Audio never touches it** (guarded by `matches!(media_type, VIDEO | SCREEN)` at `forwarder.rs:534`). The root cause identifies **uncapped audio (`forwarder.rs:456`) as the dominant `P×R` term** (~400k audio decides/s vs. video capped at `MAX_VISIBLE_VIDEO`, `forwarder.rs:499`). So the recent_t0 write lock does **not** erode the C× gain on the *dominant* (audio) term — it erodes it on the *secondary* (video) term. The audio decide path has no shared write lock and parallelizes cleanly under W workers; fix recent_t0 so the video path keeps up.

3. **`scorer.write().await` (`chat_server.rs:3218`) is the genuine new serialization point §3.3 under-weights.** Today one ingest task → uncontended async `TokioRwLock` write. §3.3 proposes **K parallel ingest consumers all calling `scorer.write().await` per inbound audio packet**. At P=20 × ~50 pps ≈ 1000 audio writes/s/room the EWMA update is short, so K-way contention is *tolerable at P=20* — but it is an async write lock on the hot ingest path whose contention scales with **P**, the exact axis town-hall pushes to 500+. **Promote the ADR's "if it becomes hot, batch per tick" from optional to a required B3 element:** each consumer accumulates `(sender, level, hint)` per-consumer and flushes once per scorer tick (already a 200ms cadence), turning K×P per-packet writes into K writes/tick. Without this, K-way ingest sharding reintroduces a serialization point as P grows.

### C. C× claim and the new ceiling — corrected

**The "≈C× / 4× on a 4-CPU pod" is an upper bound, not the expected gain.** Corrections to §3.4:

- **`worker_threads(N) = #cores` over-subscribes.** The actix arbiters (one current-thread runtime per shard, `chat_server.rs:1032-1033`), the HTTP/WS workers, and the async-nats client already consume threads. Sizing the fan-out pool to **all** online cores means total runnable threads > cores, so the pool time-slices with arbiters/IO rather than getting C full cores. **Realistic egress speedup ≈ `0.6–0.75·C`, i.e. ~2.5–3× on a 4-core pod, not 4×.** Recommend defaulting `SFU_FANOUT_WORKER_THREADS` to `max(1, #cores - 1)` and restating the claim as "up to ≈C×, realistically ~0.6–0.75·C on a shared pod."
- **The barrier model (§3.2) serializes on the slowest shard per packet** (`join_all` ⇒ wall-time = `max` over shards). With non-blocking `try_send` to actix mailboxes (`chat_server.rs:3397`) and uniform cheap `decide`+`try_send`, the barrier cost is low for the audio-dominated workload. It only hurts if a shard hits the `recent_t0` write lock (B.2) or a slow `cache.insert` — both addressed by the per-shard `recent_t0` fix. With that fix the barrier is acceptable for B2; deferring the pipelined variant is correct.
- **The per-receiver `format!`+`replace` (`chat_server.rs:3532`) is a real, removable cost.** At P=20, R=400 audio-only ≈ 400k forwards/s ⇒ **~800k String allocations/s** (format + replace) on the egress hot path — real allocator pressure and cache pollution, and a shared-allocator contention point under W threads. Fix per the root cause: self-skip via `SessionId` equality as `forwarder.rs:368` already does (`packet_wrapper.session_id == receiver_sid`), and move the `' ' → '_'` room sanitization to subject-construction time, not per receiver. **Fold into B2.**

**The real new ceiling for E2EE-base (no mixdown), corrected:** total work stays **O(P×R)**, divided across ~`0.6–0.75·C` effective cores. The binding limit at moderate R is **egress bandwidth, not CPU** — the ADR's §3.4 bandwidth number is the true ceiling. For one prod-class pod (assume ~1–2 Gbps usable egress, 4 cores):
  - **At P=20, audio bandwidth/receiver ≈ 20 × 32 kbps = 640 kbps.** A 1 Gbps NIC ⇒ **~1560 receivers** on audio bandwidth alone (2 Gbps ⇒ ~3100). **CPU after Part A:** audio decides ≈ P×R / (0.7·C); at R=400 ≈ ~114k decides/s/core — within budget and consistent with the ADR's "~100k/core" (that figure checks out).
  - **So at P=20 the post-Part-A pod is bandwidth-bound at R≈1560 and CPU-bound only well beyond that.** The 20p×400r target is sustained with **≈3–4× headroom — the late-joiner fix the ADR claims is VALIDATED.**
  - The CPU crossover (~0.7·C × ~150k decides/s/core ≈ 420k decides/s ⇒ R≈21000 at P=20) is far past the **bandwidth cap (~1560)**. **Name the ceiling explicitly: for E2EE-base it is the NIC, reached around R≈1500 at P=20** — squarely in the 500+ webinar regime town-hall (Part B) targets. The ADR's shape is correct; soften "4×" to "~0.6–0.75·C" and rename the ceiling from CPU to **egress bandwidth**.

### D. Ingest subject-sharding (§3.3) — does it actually parallelize?

Mostly yes, with caveats:
- **NATS delivery parallelizes across K distinct subjects** (`room.<room>.<shard>.*`), each with its own `Subscriber` and its own 16384-slot buffer (`nats_connect.rs:196`). This genuinely removes the single `sub.next()` choke (root-cause §3.2); each consumer drains `P/K` presenters. **Validated.**
- **But the merge re-serializes at two shared points:** (1) `scorer.write().await` (B.3 — required fix), and (2) the **egress stage is shared** — all K consumers feed the same W egress shards. If egress is the bottleneck (it is, on audio), K-way ingest helps only up to what egress can emit. **K>1 buys drop-resistance and parse parallelism; it does NOT raise the egress ceiling.** Its real value is eliminating the silent buffer-overflow regime (root-cause §3), not added throughput. Default `K=1` (§5.1) is the right conservative default.
- **Memory:** K consumers × 16384-slot buffers, each slot a ref-counted `Bytes`. At K=4 and ~1 KB avg payload ≈ ~64 MB/room of buffer *headroom* (rarely full). Fine for one big room on a 4 Gi pod; with many medium rooms × K it accumulates. **Cap aggregate `rooms × K × 16384` buffer commitment per pod, or keep K=1 for small rooms and raise K only above a size threshold** (reuse the beacon-tick evaluator). Do not unconditionally set K=#cores per room.

### E. Low-power / bandwidth / constrained clients

- **Client-side is unchanged in Part A** — confirmed. No new decode/encode, no extra streams, no payload growth reaches the client. The `format!` removal and threading are server-internal. **No client regression.** (Part B does change the client — out of scope here.)
- The HEALTH_BEACON drop (`chat_server.rs:3527`), which prevents fanning ~70 B beacons to every client every 5 s, lives in `egress_decide_from_parsed` and is preserved by the sharded loop (shards call the same function). **Verify B2 keeps this drop inside the sharded path** (it currently does).

### F. Required design changes before prototyping (gating B1/B2/B3)

1. **B2 (required):** shard `recent_t0` per egress shard or convert to `DashMap` keyed by `(receiver, sender)` — remove the per-room exclusive write lock inside the per-receiver loop (`forwarder.rs:608`). Without this, video-path parallelism collapses under W workers.
2. **B2 (required):** remove the per-receiver `format!`+`replace` self-subject build (`chat_server.rs:3532`); use `SessionId` equality self-skip as `forwarder.rs:368` already does. ~800k allocs/s removed at the target shape.
3. **B2 (recommended):** move `SFU_ROOM_SIZE.set()` (`forwarder.rs:344`) out of `decide` to once-per-packet before fan-out — avoid W-way contended-atomic writes per receiver.
4. **B3 (required):** batch scorer feed — per-consumer accumulation flushed once per scorer tick, not `scorer.write().await` per audio packet (`chat_server.rs:3218`). Prevents K-way async-write contention scaling with P.
5. **§3.1 (required):** size `SFU_FANOUT_WORKER_THREADS` to `#cores - 1` (or document the over-subscription); restate the gain as "up to ≈C×, realistically ~0.6–0.75·C," and name **egress bandwidth (NIC), not CPU**, as the binding ceiling for E2EE-base.
6. **§3.3 (recommended):** make K adaptive (raise only for large rooms via the beacon tick) and cap aggregate `rooms × K × 16384` buffer commitment per pod.

**Net:** Part A's architecture is correct and the late-joiner fix at 20p×400r is **validated with ≈3–4× headroom**. Soften the C× number to ~0.6–0.75·C and rename the ceiling to **egress bandwidth**. The single real lock hazard is `recent_t0` (video path); the single real new serialization point is the scorer write under K-way ingest — both have clean, required fixes (F.1, F.4). The RwLock read-contention the task worried about is a non-issue (concurrent readers + DashMap cache). Proceed to prototype on the scratch branch with F.1–F.4 folded into the B2/B3 designs.

> **Roster note:** per CLAUDE.md, the `Send/Sync` audit gate (§3.1) and the NATS subject-shard scheme (§3.3) must still be ratified by **backend-rust-streaming**, and B2/B3 reviewed by **code-reviewer**, before merge. This performance review does not substitute for those gates.

---

## Security Review (web-security-auditor)

**Reviewer:** web-security-auditor · **Date:** 2026-05-22 · **Scope:** design-only gate before prototyping. This design intentionally relaxes a core security property (audio E2EE), so it is held to a high bar.

**Overall verdict: BLOCKER — do not prototype Track 2 (town-hall, B4–B12) as written.** Track 1 (Part A, B1–B3, B11/T1) is E2EE-preserving and may proceed under its own existing audit gates. The town-hall crypto design rests on a premise — *audio/video key separation* — that **does not exist in the current codebase** and is **not created by this ADR**. Several §4 claims are aspirational, not upheld by the proposed scheme. Required design changes are enumerated below; each is a precondition to prototyping the relevant bead.

### Code-grounded finding that invalidates the §4.3 premise

There is exactly **one symmetric key per client**, used for video, screen, AND audio:
- `Aes128State` is constructed once per client (`video_call_client.rs:198`, stored `:341`) and handed unchanged to every encoder: audio `microphone_encoder.rs:308`, video `camera_encoder.rs:336`, screen `screen_encoder.rs:315` — all call `client.aes()`. There is **no per-media key**.
- `AesPacket` carries only `{ key, iv }` (`video_call_client.rs:1598-1606`). It has **no media-type tag, no key-id, no epoch**. The receiver installs it wholesale as the peer's media key via `set_peer_aes` (`video_call_client.rs:1174-1183`) and uses it to decrypt that peer's video, screen, and audio alike.
- Therefore the ADR's repeated assertion that "the SFU never gets the video key" (§1.3, §4.3, §4.5) is only true **if and only if** a brand-new, separate audio-only key is introduced AND the client is changed so video/screen keep using a *different* key the SFU never receives. **That separation does not exist yet and §4.3/B6 under-specify it.** As written, "distribute the room audio key via the existing AES_KEY/AesPacket exchange, SFU added as a recipient" would hand the SFU a key that is structurally indistinguishable from a full media key. Audio/video key separation is **aspirational, not real**, until B6 builds it explicitly.

**Two pre-existing crypto weaknesses materially raise the stakes of this relaxation** (they are not introduced here, but the ADR must account for them):
- **Static, reused IV.** `Aes128State` generates one IV at key creation and reuses it for every packet (`aes.rs:47-58`, `:77-79` — `new_from_slices(&self.key, &self.iv)` per call with a fixed `self.iv`). AES-CBC with a fixed key+IV across many messages leaks equality of plaintext prefixes. A room audio key shared by *all* presenters with a *single shared IV* is strictly worse (cross-sender prefix correlation). The town-hall key scheme MUST specify per-sender, per-message nonce/IV.
- **RSA PKCS#1 v1.5** wrapping (`rsa.rs:20,61,69`). Pre-existing; the new key-distribution path inherits it. Note as a known limitation; do not expand its use without flagging.

### Per-threat verdicts

**1. Key isolation — BLOCKER (premise not upheld).**
The claim is false against current code: there is no audio/video key split, and `AesPacket` cannot express one. Required before B6: (a) introduce a distinct **audio-only room key** type with an explicit media-scope tag and key-id/epoch in the wire format (extend `AesPacket` or add a new packet — do NOT overload the existing single-key path); (b) keep video/screen on the **per-sender personal `Aes128State`** which is *never* serialized to or wrapped for the SFU — assert this with a negative test that fails if the SFU ever receives a key usable for `MediaType::VIDEO`/`SCREEN`; (c) the client encrypt path (`transform.rs:79/:124`) must remain bound to the personal key and be statically incapable of using the room audio key for video. Until the type system / wire format enforces "this key decrypts audio only," the separation is a comment, not a control.

**2. Consent & trust — BLOCKER (silent auto-downgrade is unacceptable).**
At 500 the server gains the ability to hear every participant. The ADR's §4.5 row says "user-visible indicator (UX via ux-ui-expert)" and gates on the `TOWN_HALL_AUDIO` capability bit — but a capability bit advertised at connect (`§4.4`) is **not consent**; it is a client build flag the user never sees. Required: (a) **explicit, affirmative, per-session user consent** surfaced *before* a participant's audio is first sent under the room key — not a silent capability negotiation; (b) a **persistent, non-dismissable trust indicator** whenever the room is in TownHall ("Audio is processed by the server in this large meeting and is no longer end-to-end encrypted"), treated as security-critical rendering per CLAUDE.md; (c) a user who declines must have a defined outcome (stay E2EE-but-unmixed, or be told they cannot join the large room) — §4.3's "their audio simply isn't carried" silently drops a non-consenting user's audio, which is a footgun and must be a visible, explained state, not a quiet mute. What a malicious/compromised SFU gains is now **full audio plaintext of the whole room** (today it gains only routing headers per ADR-0001) — this is a qualitative escalation and must be stated as such to users and operators.

**3. Downgrade / coercion attack — BLOCKER as specified (trigger authority + control-packet authenticity unproven).**
The mode switch is driven by `member_count()` and announced via a control packet on `room.<room>.system` (§4.1, §4.4). Two attack surfaces are unaddressed:
  - **Count inflation / forced downgrade.** If participant count can be inflated (sybil joins, a non-owner/spill pod's view, or a bug that double-counts), an attacker could push a room over 500 to *force* audio E2EE off. The ADR says the beacon hub reads `member_count()` — but does NOT establish that **only the owner pod** decides mode, nor that spill/non-owner pods cannot induce or observe-and-act-on the transition. Required: the town-hall decision MUST be made by exactly one authoritative actor (owner pod) and the resulting key distribution MUST NOT be honorable from any other source.
  - **Forged control packet.** `room.<room>.system` is the same subject clients already receive meeting-info on; the ADR reuses it. The design MUST specify how a client distinguishes an *authentic* mode-switch from the authoritative server vs. a forged/replayed one (e.g., a NATS-path observer or a malicious peer publishing a `mode: TownHall` packet to coerce clients into encrypting under an attacker-known room key). There is **no authentication of the mode-switch packet specified**. Required: the mode-switch and the room-audio-key epoch must be authenticated to the owner pod (signature or server-only subject the client trusts), and the client must refuse to switch its encrypt key on an unauthenticated signal. Absent this, a downgrade-to-attacker-key is feasible. **This is the highest-severity finding after the key-isolation premise.**

**4. Transition window — NEEDS-CHANGE.**
During `SFU_TOWNHALL_TRANSITION_MS` the SFU forwards both per-presenter E2EE audio AND the mix (§4.4). Two concerns: (a) the SFU holds plaintext audio for the *duration of town-hall*, not just the window — acceptable only if §2/§5 fail-closed scoping (finding 5) holds; (b) **no key rotation / forward-secrecy is specified** across mode flips. When a room enters TownHall the SFU obtains the room key; if that same room later drops below 400 and re-enters, reusing a stale room key (or the static-IV scheme) compounds the IV-reuse problem. Required: a fresh room audio key per TownHall *entry* (epoch-bumped), retired and discarded by all parties (including the SFU's in-memory copy) on exit; specify the SFU key-zeroization step on exit and on room teardown. Also specify that per-presenter E2EE audio sent during the overlap is NOT decryptable with the room key (different key) so the window does not accidentally hand the SFU two ways into the same audio.

**5. Scope creep of the relaxation — NEEDS-CHANGE (fail-closed not proven).**
The intent (off by default, per-room, hysteresis, kill switch) is sound and well-aligned with fail-safe defaults. But the ADR does not prove the SFU **cannot** hold an audio key for a room that is NOT in TownHall: key distribution (§4.4 step 2) and key retirement (§4.4 reverse) are described narratively, not as invariants. Required: (a) the SFU must reject/zeroize any room key when `RoomState.mode != TownHall`; (b) the decrypt path must assert `mode == TownHall && media_type == AUDIO && sender presented TOWN_HALL_AUDIO` at the call site (the ADR says this in §4.3 — make it a hard assertion with a metric, and a negative test in T6); (c) a misconfig where `SFU_TOWNHALL_ENABLE=true` but threshold is set very low (e.g. 0 or 2) must be rejected at config parse — add a floor on `SFU_TOWNHALL_THRESHOLD` so an operator cannot trivially turn a small room into a server-decrypted room. Fail direction is otherwise correct (default OFF).

**6. Recording / exfil — NEEDS-CHANGE (must be documented as an accepted property).**
Once the SFU decrypts audio to mix it, **nothing technically prevents silent server-side recording of all participant audio** in TownHall mode. This is an inherent property of server-side mixdown, not a bug — but it is currently unstated. Required: (a) document explicitly in §4.5 and in user-facing copy that in TownHall mode audio is available in plaintext to the server operator and could be recorded/retained; (b) state the operator's data-handling commitment (retention, logging-off-of-plaintext) as a policy item; (c) ensure the mixer path does not log or persist decrypted PCM/Opus (no debug dumps of mixed audio) — add to the B7 review checklist. Users consenting under finding 2 must be consenting to *this* property, in plain language.

### Mandatory user-facing consent / trust requirements (gating B5/B6/B9/B12)

1. **Affirmative consent** before any audio is sent under a room key — not a silent capability bit. Per-session, re-prompted on a new TownHall entry epoch.
2. **Persistent visible indicator** for the entire duration of TownHall ("audio not end-to-end encrypted — processed by server"). Security-critical rendering; covered by web-security-auditor in B12, not UX-only.
3. **Defined, visible outcome for non-consenting users** (no silent audio drop).
4. **Plain-language disclosure that the server can hear and may record** audio in this mode (finding 6).

### Required design changes before prototyping (preconditions, by bead)

- **B6 (BLOCKER):** Introduce a real, media-scoped audio-only key (new/extended wire type with media-scope + key-id + epoch); guarantee video/screen keys are never serialized to the SFU (negative test). Specify per-sender, per-message IV/nonce — do NOT reuse the static-IV `Aes128State` scheme for the shared room key. (cites `aes.rs:47-58`, `AesPacket` `video_call_client.rs:1598-1606`)
- **B5 (BLOCKER):** Authenticate the mode-switch control packet and the key epoch to the owner pod; client MUST refuse to switch encrypt key on an unauthenticated/forged signal. Establish owner-pod-only mode authority; spill/non-owner pods cannot induce TownHall. (cites `room.*.system` `chat_server.rs:3441`)
- **B4 (NEEDS-CHANGE):** Config floor on `SFU_TOWNHALL_THRESHOLD`; count source must be the authoritative owner-pod count and resistant to inflation/sybil; document the trigger authority.
- **B7 (NEEDS-CHANGE):** Fresh room key per entry epoch; SFU zeroizes key on exit/teardown and when `mode != TownHall`; hard assertion `mode==TownHall && media_type==AUDIO && capability present` at the decrypt site; no logging/persistence of decrypted audio.
- **§4.5 / B12 (NEEDS-CHANGE):** Document the recording/exfil property; affirmative consent + persistent trust indicator + non-consent outcome as specified above.

### What may proceed now

Track 1 (Part A): the multi-thread fan-out pool (B1), egress sharding (B2), and subject-sharded ingest (B3) **preserve E2EE** — the forwarder stays a byte relay and no key reaches the SFU. These carry their own `Send/Sync` and NATS-subject audit gates (already noted in §3.1/§3.3) and are out of scope for this crypto blocker. They may be prototyped on the scratch branch independently. **Track 2 (B4–B12) is gated on the BLOCKER items above being resolved in a revised §4 and re-reviewed.**

---

## §4 Revision (post-security-review)

**Status:** This section is the **design of record for Track 2 (town-hall)**, superseding §4.3, §4.4, and §4.5. Track 1 (B1–B3) is unaffected and proceeds in parallel. Authored to clear the web-security-auditor BLOCKER above. **Ready for security re-audit.**

### Changelog vs. original §4

- §4.3's "room audio key, SFU never gets the video key" was **structurally impossible** on the current wire: one undifferentiated key per client, no scope/id/epoch, static-IV CBC, RSA PKCS#1 v1.5. This revision introduces a real media-scoped key type, GCM with per-message nonces, OAEP transport, an authenticated owner-pod-only mode switch, an affirmative consent + persistent trust model, and fail-closed scope containment with negative tests. Each maps to one of the auditor's five required items (R1–R5).

### Confirmed current-state (the constraints this revision designs around)

- **One key per client, no media separation.** `Aes128State { enabled, key:[u8;16], iv:[u8;16] }` (`videocall-client/src/crypto/aes.rs:27-32`) is built once per client (`video_call_client.rs:198`, stored `:341`) and the **same `Rc<Aes128State>`** is handed to the audio encoder (`microphone_encoder.rs:163` calls `aes.encrypt`), the video encoder (`transform.rs:79`), and the screen encoder (`transform.rs:124`). There is no per-media key.
- **`AesPacket` has no scope/id/epoch.** Proto is `{ bytes key=1; bytes iv=2; }` (`protobuf/types/aes_packet.proto:3-6`); serialized verbatim from the personal state (`video_call_client.rs:1598-1606`); a peer reconstructs a full `Aes128State` from it (`video_call_client.rs:1172-1184` → `Aes128State::from_vecs`, `aes.rs:60-70`). A packet handed to the SFU is **indistinguishable from a full media key** and would decrypt video.
- **Static, reused IV.** The IV is fixed at key creation (`aes.rs:50-58`) and reused for *every* message of *every* media type (`aes.rs:77-79`, CBC `Aes128CbcEnc::new_from_slices(&self.key, &self.iv)`). Reusing one IV across a key **shared among many presenters** is a hard fail (CBC IV reuse leaks equality of identical prefixes; for any AEAD it is catastrophic).
- **RSA PKCS#1 v1.5** key transport (`videocall-client/src/crypto/rsa.rs`), via `RSA_PUB_KEY`/`AES_KEY` (`packet_wrapper.rs:211-213`).

### R1 — Real media-scoped key type (audio-only; video/screen key never serialized to the SFU)

**Wire change — extend `AesPacket`** (`protobuf/types/aes_packet.proto`), regenerate `videocall-types/src/protos/aes_packet.rs`:

```proto
message AesPacket {
  bytes key   = 1;
  bytes iv    = 2;          // legacy CBC seed; ignored for GCM scopes (kept for back-compat parse)
  MediaScope scope    = 3;  // NEW
  bytes      key_id   = 4;  // NEW: 16-byte random id; binds packets to a key
  uint64     epoch    = 5;  // NEW: monotonic per-room key generation
  CipherSuite cipher  = 6;  // NEW: AES_128_CBC_STATIC_IV (legacy) | AES_128_GCM (townhall)
}
enum MediaScope   { LEGACY_ALL = 0; AUDIO_TOWNHALL = 1; }   // 0 == today's full-media key
enum CipherSuite  { AES_128_CBC_STATIC_IV = 0; AES_128_GCM = 1; }
```

`scope=LEGACY_ALL, cipher=AES_128_CBC_STATIC_IV` is the **wire-compatible default** — proto3 zero values mean an unmodified peer/SFU sees exactly today's behavior. No flag-day.

**Client key model — two distinct keys, never conflated.** Replace the single `Rc<Aes128State>` with a small key-set:
- `personal_media_key: Rc<Aes128State>` — the existing per-client key (`video_call_client.rs:198`), used for **video + screen always** and for **audio when not in town-hall**. **This key is `scope=LEGACY_ALL` and MUST NEVER be serialized into a `scope=AUDIO_TOWNHALL` packet, and MUST NEVER be sent to the SFU recipient.** Enforced by construction: the SFU-targeted serializer only accepts a key whose in-memory type is `TownHallAudioKey` (a distinct newtype, not `Aes128State`), so a video key is a compile-time type error to send to the SFU.
- `townhall_audio_key: Option<TownHallAudioKey>` — a **new** newtype `{ key:[u8;16], key_id:[u8;16], epoch:u64 }` (GCM; no stored IV). Built fresh on town-hall entry (R5), used by `transform_audio_chunk` (`microphone_encoder.rs:114`) **only** while the room is in town-hall AND the client consented (R4).

**SFU never holds a video-capable key.** The SFU is a recipient only of a `scope=AUDIO_TOWNHALL` `AesPacket`. The forwarder/mixer's key store accepts a key **iff** `scope==AUDIO_TOWNHALL && cipher==AES_128_GCM`; any other scope is rejected and counted (`sfu_townhall_decrypt_scope_violations_total`).

**NEGATIVE TEST (R1, CI-blocking, lives in B6/B11 T-SEC):**
1. *Client serializer test:* attempting to build an SFU-bound `AesPacket` from the `personal_media_key` (or any `scope!=AUDIO_TOWNHALL` key) **fails to compile / panics in test** — the newtype boundary makes it unrepresentable.
2. *SFU ingest test:* feed the SFU key-store an `AesPacket` with `scope=LEGACY_ALL` → must be **rejected**, increment the violation counter, and the key must **not** be installed. Feed a `scope=AUDIO_TOWNHALL` key but then attempt to use it to decrypt a `media_type=VIDEO` packet → the decrypt site asserts media_type==AUDIO and **refuses** (R5).
3. *End-to-end:* run a town-hall room and assert that no key material reaching the SFU can decrypt any VIDEO/SCREEN `MediaPacket` captured on the wire. **CI fails if any SFU-held key decrypts video.**

### R2 — Per-message IV/nonce for the room audio key (no static-IV reuse)

The town-hall room key is **shared across all presenters**, so static-IV reuse (`aes.rs:77-79`) is unacceptable. Switch the town-hall audio path to **AES-128-GCM with a per-message 96-bit nonce**:

- **Nonce construction (deterministic, collision-free, no shared counter):** `nonce[12] = sender_id_trunc(4 bytes) || epoch(4 bytes, low) || message_seq(4 bytes)`. `sender_id_trunc` partitions the nonce space per presenter so two presenters sharing the room key never collide; `message_seq` is the per-sender monotonic audio sequence already present in `AudioMetadata.sequence` (`microphone_encoder.rs:146`); `epoch` is the key generation (R5) so a re-keyed room restarts seq safely. This is a standard partitioned-nonce scheme and needs **no cross-client coordination**.
- **Wire:** the GCM nonce is reconstructible by the SFU from the cleartext `RoutingHeader`/`AudioMetadata.sequence` + the sender id + the current epoch — so it need not be transmitted in full; the SFU recomputes it. (If the auditor prefers explicit transmission, carry the 12-byte nonce in a new `MediaPacket` field; recommendation is recompute-from-metadata to avoid a packet-size bump on the hot path — confirm with **performance-reviewer**.)
- **GCM auth tag** additionally gives the SFU integrity on inbound audio (a forged audio packet fails decryption and is dropped + counted) — a strict improvement over today's unauthenticated CBC.
- **`aes.rs` change:** add a GCM code path (`aes-gcm` crate) selected by `CipherSuite`; the existing CBC path (`aes.rs:72-92`) is untouched for `LEGACY_ALL`. The town-hall key type only ever uses GCM.

### R3 — Authenticated, owner-pod-only mode switch (anti forged-downgrade + anti count-inflation/sybil)

**Threats:** (a) a forged `mode:Standard` packet downgrades a town-hall room so the attacker can capture per-presenter audio it can't otherwise mix; (b) a forged `mode:TownHall` or an inflated participant count forces a room into server-decrypt mode (sybil/count-inflation) to exfiltrate audio.

**Design:**
- **Owner-pod is the sole mode authority.** Mode is decided only by the room's **owner pod** (the affinity owner, `actix-api/src/sfu/affinity.rs`; the same authority that owns `member_count()` at `health_beacon.rs:391`). Spillover/non-owner pods **cannot** induce a mode change — they observe and relay only. The evaluator (B4, in the beacon-hub tick) runs on the owner pod and reads the **authoritative server-side `RoomState::member_count()`** (`room_state.rs:member_count`), **never** a client-asserted count → count-inflation/sybil at the wire cannot move the threshold; an attacker would have to actually open ≥ threshold real authenticated sessions.
- **Signed mode-switch control packet.** The mode-switch packet on `room.<room>.system` (`chat_server.rs:3441`) carries `{ mode, key_id, epoch, owner_pod_id, issued_at, mac }`. `mac` is an HMAC (or Ed25519 signature) over the tuple using an **owner-pod signing key** whose public half is distributed to clients at join (piggy-backed on the existing `RSA_PUB_KEY` exchange the SFU↔client already performs, `video_call_client.rs:1191`+). Clients **verify the signature before acting**; an unauthenticated or badly-signed switch is **refused** and counted (`sfu_townhall_unauth_switch_rejected_total`). A replayed older packet is rejected by `epoch`/`issued_at` monotonicity.
- **Downgrade safety.** A `mode:Standard` (town-hall→standard) switch is honored only if signed by the current owner pod with a fresh epoch; clients keep encrypting with the **town-hall key** until they have verified the downgrade, so a forged downgrade cannot trick a client into emitting per-presenter audio prematurely.
- **`SFU_TOWNHALL_THRESHOLD_FLOOR` (config floor):** `SFU_TOWNHALL_THRESHOLD` is clamped to `>= SFU_TOWNHALL_THRESHOLD_FLOOR` (default 500; floor default 50, configurable but with a logged warning below, mirroring the parse-and-warn discipline at `config.rs:155-265`). Prevents an operator misconfiguration (or a compromised config path) from setting the threshold to e.g. 2 and silently turning every small room into a server-decrypt room.

### R4 — Consent + trust model (affirmative consent; persistent indicator; defined non-consent outcome)

- **Affirmative, per-session consent.** Before a client sends **any** audio under the town-hall room key, the UI presents a **blocking, affirmative-action modal**: "This room has grown past N participants. To continue with audio, your microphone audio will be **decrypted and mixed by the server** (it will no longer be end-to-end encrypted). Video remains end-to-end encrypted." Consent is **per session** (not remembered) and required again on each fresh town-hall entry (epoch change). Until consent, the client does **not** install/use the town-hall audio key and does **not** transmit town-hall audio. (UX owned by **ux-ui-expert**, gated by **web-security-auditor** — `B12`/`B9`.)
- **Persistent, non-dismissable trust indicator (security-critical rendering).** While the room is in town-hall AND the local client is transmitting town-hall audio, a **persistent, non-dismissable** banner/badge is shown: "Audio not E2EE — processed by server." It is rendered by the same trust-surface code path as other security indicators and is treated as security-critical per CLAUDE.md (web-security-auditor must review the rendering). It must **not** be suppressible by room config or by a peer.
- **Defined non-consent outcome (NOT a silent audio drop).** A user who declines:
  1. **Stays in the room** with **full video (E2EE) and full audio receive** — they still **hear** the mixed stream (receiving the mix requires no plaintext from them).
  2. Their **microphone is explicitly disabled** with a visible "Mic off — town-hall audio declined" state and a one-tap "Enable & consent" affordance. This is an explicit, user-visible mute, **not** a silent black-hole — the user always knows why they are not being heard.
  3. `sfu_townhall_consent_declined_total` is incremented. Non-consenting senders are **never** decrypted server-side (R5 assertion holds); if a non-consenting client erroneously sends `scope=AUDIO_TOWNHALL` audio it is dropped + counted, never mixed.

### R5 — Fail-closed + scope containment

- **Hard decrypt-site assertion (fail-closed).** The mixer's decrypt call site (B7) asserts, before touching ciphertext:
  `mode == TownHall && media_type == AUDIO && sender_has_capability(TOWN_HALL_AUDIO) && key.scope == AUDIO_TOWNHALL && key.cipher == AES_128_GCM && key.epoch == room.current_epoch`.
  If any clause is false → **no decrypt**, packet dropped, `sfu_townhall_decrypt_scope_violations_total{reason}` incremented. The assertion is a `debug_assert!` **plus** a runtime guard (the guard runs in release; the decrypt is unreachable without it).
- **Key lifecycle / zeroization.** The town-hall room key (SFU side and client side) is wrapped in a `Zeroizing<_>` newtype (the `zeroize` crate) and is **dropped + zeroized** when: (a) the room leaves town-hall (`mode != TownHall`), (b) the room is torn down, (c) on epoch rotation the previous epoch's key is zeroized after the overlap window. The SFU **refuses to retain** any town-hall key for a room not currently in town-hall — periodic sweep + on-transition zeroization. Negative test: after a town-hall→standard transition, assert the SFU key store holds no `AUDIO_TOWNHALL` key for the room.
- **Fresh epoch-bumped key per entry.** Each town-hall entry generates a **new** `townhall_audio_key` with a new random `key_id` and an incremented `epoch` (owner-pod authoritative). No key is reused across entries; this bounds the blast radius of any single-epoch compromise and makes the partitioned nonce (R2) safe to restart at seq 0.
- **No plaintext logging.** PCM samples, decoded Opus frames, and decrypted `MediaPacket` payloads are **never** logged, traced, or persisted. The mixer code path is annotated and reviewed for this; logging guards reject byte buffers from the decrypt→mix→encode stage. (This is the inherent server-side-recording surface — see §4.5 update below.)

### §4.5 update — security posture (revised)

| Plane | Standard mode (<exit threshold) | Town-hall mode (≥ threshold) |
|---|---|---|
| **Video / screen** | E2EE end-to-end (`transform.rs:79/:124`, personal `Aes128State`, `scope=LEGACY_ALL`). | **E2EE end-to-end (unchanged).** The personal/video key is a distinct newtype that is **never serialized to the SFU** (R1); SFU only ever holds an `AUDIO_TOWNHALL` GCM key. |
| **Audio** | E2EE end-to-end (`microphone_encoder.rs:163`, personal key). | **Relaxed for consenting senders only:** SFU decrypts audio (`AUDIO_TOWNHALL` GCM key, R1/R2) to mix top-K → one stream. Non-consenting senders are mic-disabled (R4), never decrypted. Audio is **not** E2EE-opaque to the server in this mode. |
| **Routing header** | Cleartext metadata only (`audio_level`, `is_speaking`, layers) per ADR-0001. | Same; additionally `AudioMetadata.sequence` feeds the GCM nonce (R2). |
| **Mode authority** | Owner pod only; signed switch (R3). | Owner pod only; signed switch + epoch + `THRESHOLD_FLOOR` (R3). |
| **Consent / trust** | n/a. | Affirmative per-session consent before audio TX; persistent non-dismissable "audio not E2EE" indicator; defined decline outcome (R4). |

**Inherent property, stated for users and operators:** in town-hall mode the server holds **plaintext audio** of consenting speakers and is therefore **technically capable of recording or relaying that audio**. This is an unavoidable consequence of server-side mixing and is disclosed in the consent copy ("decrypted and mixed by the server") and the persistent indicator. Video is never exposed. Operators must treat town-hall pods as in-scope for any audio-recording/retention policy and compliance review.

### Re-audit checklist (what the security re-audit should confirm)

1. R1 — media-scoped key type + the newtype boundary make a video-capable key **unrepresentable** as an SFU-bound packet; negative tests are CI-blocking.
2. R2 — GCM + partitioned per-message nonce; no static IV on the shared key; nonce-collision argument holds.
3. R3 — owner-pod-only signed mode switch; clients refuse unauthenticated/forged/replayed switches; count is server-authoritative; `THRESHOLD_FLOOR` present.
4. R4 — affirmative per-session consent gates first audio TX; persistent non-dismissable indicator; decline outcome is explicit mic-off, not silent drop.
5. R5 — fail-closed decrypt assertion (metric + negative test); zeroization on exit/teardown/rotation; fresh epoch key per entry; no plaintext logging; recording property documented.

**§4 is ready for security re-audit.** Track 1 (B1–B3) continues in parallel, unaffected.

---

## Security Re-Audit (web-security-auditor)

**Reviewer:** web-security-auditor · **Date:** 2026-05-22 · **Scope:** the §4 Revision (post-security-review) + revised B4–B12 bead specs, re-checked against the five original BLOCKER/NEEDS-CHANGE findings. Verified against current crypto source (`crypto/aes.rs`, `crypto/rsa.rs`, `client/video_call_client.rs`, `encode/microphone_encoder.rs`, `encode/transform.rs`).

**Verdict: CLEARED TO PROTOTYPE on the scratch branch, with two MUST-FIX-IN-B6 nonce-uniqueness items and three documented residuals.** The revision is a major improvement and the design intent is now correct: a real media-scoped key type with a compile-time newtype boundary, GCM with per-message nonces, owner-pod-only authenticated mode switching, affirmative consent + persistent trust indicator, and fail-closed scope containment with CI-blocking negative tests. The two MUST-FIX items are **GCM nonce-uniqueness edge cases** (catastrophic if missed, but fixable inside B6's design — they do not require re-architecting). They must be resolved in the B6 spec/prototype and re-checked at the B6/B7 implementation gate; they do not block starting the scratch prototype, which begins with B1–B3 (E2EE-preserving) anyway.

### Per-finding re-verdict

**1. Key isolation — CLEARED.**
The revision makes a video-capable key **structurally unrepresentable** as an SFU-bound packet: `personal_media_key: Rc<Aes128State>` (video/screen, scope=LEGACY_ALL) vs. a distinct `TownHallAudioKey` newtype, with the SFU-targeted serializer accepting only the newtype — so sending a video key to the SFU is a compile-time type error (R1). `AesPacket` gains `scope`/`key_id`/`epoch`/`cipher`; the SFU key store rejects `scope != AUDIO_TOWNHALL` and counts violations; the CI-blocking negative test (B6/B11 T-SEC, three layers: serializer / SFU-ingest / end-to-end wire capture) fails if any SFU-held key decrypts VIDEO/SCREEN. This is the right shape — the separation is now enforced by the type system and a wire-capture test, not by a comment. proto3 zero-value default (`LEGACY_ALL`/`CBC_STATIC_IV`) preserves wire compat with no flag-day. Cleared.

**2. Consent & trust — CLEARED.**
Affirmative per-session blocking modal **before any town-hall audio is transmitted**, re-prompted on each epoch entry; persistent non-dismissable "Audio not E2EE — processed by server" indicator routed through the security-critical trust-surface code path (web-security-auditor reviews the rendering in B12, not UX-only); and a **defined, visible** non-consent outcome — explicit mic-off with a labeled reason and one-tap consent affordance, never a silent drop (R4). The decline path correctly keeps the user receiving the mix (no plaintext required from them) while disabling their TX. Cleared. (B9/B12 implementation must still be reviewed when built — the *design* is sound.)

**3. Downgrade / coercion — CLEARED (design), with one residual on MAC-key trust root (below).**
Owner-pod is sole mode authority; the evaluator reads server-authoritative `RoomState::member_count()` on the owner pod, so count-inflation requires opening ≥threshold *real authenticated sessions* (not a wire-asserted count) — sybil is reduced to a genuine-cost attack. The mode-switch packet is signed `{mode, key_id, epoch, owner_pod_id, issued_at, mac}`; clients verify before acting and refuse unauthenticated/forged/replayed switches (epoch/issued_at monotonicity); a forged downgrade cannot trick a client into emitting per-presenter audio because it keeps using the town-hall key until a valid signed downgrade is verified (R3). `SFU_TOWNHALL_THRESHOLD_FLOOR` blocks the misconfig-to-2 attack. The downgrade-safety direction is exactly right. Cleared, modulo Residual A.

**4. Transition window — CLEARED.**
Fresh epoch-bumped key per entry; previous epoch's key zeroized after the overlap window; clients keep the town-hall key until a verified downgrade so the overlap can't hand the SFU two routes into the same plaintext; the decrypt-site epoch check (`key.epoch == room.current_epoch`, R5) rejects stale-epoch audio. Forward-secrecy across mode flips is now addressed by per-entry rekey + zeroization. Cleared.

**5. Scope creep / fail-closed — CLEARED.**
Hard decrypt-site guard runs in **release** (not just `debug_assert!`): `mode==TownHall && media_type==AUDIO && capability && scope==AUDIO_TOWNHALL && cipher==AES_128_GCM && epoch==current` → else no decrypt, drop, count. `Zeroizing<_>` keys, zeroized on `mode!=TownHall` / teardown / rotation, with a periodic sweep and a negative test asserting no `AUDIO_TOWNHALL` key remains after a town-hall→standard transition. No plaintext PCM/Opus logging, enforced on the mix path. Default OFF preserved. Cleared.

**6. Recording / exfil — CLEARED (as documented accepted property).**
The §4.5 revision states plainly that the server holds plaintext audio of consenting speakers and is technically capable of recording/relaying it; the consent copy and the persistent indicator disclose this; operators are told town-hall pods are in-scope for audio-retention/compliance review. This is the correct handling for an inherent property — it is disclosed, not hidden. Cleared.

### MUST-FIX in B6 (GCM nonce-uniqueness — catastrophic if missed; fixable within R2's scheme)

The R2 partitioned nonce `sender_id_trunc(4) || epoch_low(4) || message_seq(4)` is the right idea, but as written it has **two reuse paths that are fatal for AES-GCM** (nonce reuse under one key = forgery + partial key/plaintext recovery). Both must be closed in the B6 design before the GCM path is implemented:

- **NF-1 — `sender_id_trunc` 32-bit collision.** Session ids are `u64` (`response.session_id`, e.g. `video_call_client.rs:1241`). Truncating to 4 bytes means two presenters whose session ids collide in the low 32 bits share a nonce partition under the same `key_id`/`epoch` → **GCM nonce reuse across two senders on the shared room key**. With hundreds of concurrent senders this is not negligible (birthday bound on 32 bits). **Fix options:** (a) derive a **per-(epoch,sender) unique sub-key** via HKDF(room_key, sender_id || epoch) so each sender encrypts under a distinct key and the nonce need only be unique per-sender (strongly preferred — removes cross-sender nonce coupling entirely); or (b) have the owner pod assign a **dense, unique small sender-slot index** (e.g. u16) at town-hall entry and use that in the nonce instead of truncated session id, with explicit collision rejection. Option (a) is the cleaner control and also shrinks the blast radius of any single key.

- **NF-2 — `message_seq` reset on encoder restart.** The audio `sequence_number` is a per-encoder-handler `u64` initialized to `0` (`microphone_encoder.rs:324`) and incremented per frame (`:393`); it **resets to 0 every time `start()` rebuilds the encoder** (mute→unmute, device switch, `switching`). Within a single town-hall epoch a sender who toggles their mic will **re-emit seq 0,1,2… under the same key+nonce partition → direct GCM nonce reuse for that sender.** **Fix:** the town-hall nonce counter MUST be monotonic for the lifetime of the (epoch, sender) pair, independent of encoder restarts — e.g. a town-hall-scoped send counter held on the client key state (not the encoder closure), persisted across mic toggles, and reset only on epoch change. Do not derive the GCM nonce from `AudioMetadata.sequence` unless that field is re-sourced from the epoch-scoped monotonic counter. This interacts with NF-1 option (a): with a per-sender sub-key, the counter still must not repeat within the epoch.

Add an explicit **nonce-uniqueness invariant + test** to T-SEC (B11): assert no `(key_id, nonce)` pair is ever observed twice across a town-hall session including a mid-session mute/unmute and a 2-presenter low-32-bit-session-id-collision scenario. These are design-level fixes inside R2; they tighten the spec, they do not change the architecture.

### Residual issues (track; not blockers for scratch prototyping)

- **Residual A — MAC/signature key trust root for R3.** The design piggy-backs the owner-pod signing public key on "the existing `RSA_PUB_KEY` exchange the SFU↔client already performs (`video_call_client.rs:1191`+)." Confirm in B5: (i) the client learns the owner pod's signing public key over a channel it already trusts (TLS to the SFU is the de-facto trust root — the SFU can already see/route everything, so trusting it to *authenticate mode* is consistent with the threat model), and (ii) HMAC vs. signature is chosen deliberately — a **symmetric HMAC key shared to all clients** would let any client forge a switch, so R3 MUST use an **asymmetric signature (Ed25519)** with only the public half distributed, OR a per-client MAC keyed separately. The ADR says "HMAC (or Ed25519)" — **resolve to Ed25519** (or equivalent asymmetric) so a malicious participant cannot mint switch packets. This is a B5 design decision, gate it at B5 review.

- **Residual B — RSA PKCS#1 v1.5 → OAEP migration (now in scope via B6).** B6 explicitly hardens transport to OAEP (`rsa.rs`). Good — but note this changes the `RSA_PUB_KEY`/`AES_KEY` decrypt path (`video_call_client.rs:1167`, `rsa.rs:61`) which is **shared with the legacy LEGACY_ALL key exchange**. Migrating it touches existing E2EE for *all* rooms, not just town-hall. Requires: a negotiated cipher indicator so OAEP is only used when both ends support it (mirror the proto3-default back-compat discipline used for `AesPacket`), and a parity test that legacy CBC E2EE rooms are byte-for-byte unaffected. Treat the OAEP migration as its own sub-bead with a back-compat gate; do not let it regress existing-room E2EE. Confirm scope at B6 review.

- **Residual C — GCM tag/ciphertext wire framing.** R2 recommends recomputing the nonce SFU-side from cleartext metadata (good for hot-path size) but the **16-byte GCM auth tag** must be carried with the ciphertext. Confirm the `MediaPacket.data` framing for `cipher=AES_128_GCM` defines tag placement unambiguously (e.g., appended) and that the existing RED redundancy packer (`microphone_encoder.rs:102` `pack_redundant_audio`) is reconciled with GCM framing — the redundant-frame path currently packs raw Opus; under town-hall GCM the redundancy format and the per-frame nonce must stay consistent (each redundant frame is a distinct (seq) → distinct nonce). Flag for B6/B7; a botched RED+GCM interaction could silently break the integrity guarantee or the decode. Not a security blocker for prototyping but must be designed before B7.

### Bottom line

All six original findings are **CLEARED at the design level.** The revision correctly converted aspirational claims into enforced controls (newtype boundary, release-mode fail-closed guard, signed owner-pod authority, affirmative consent, zeroization). **Cleared to prototype on the scratch branch.** Track 1 (B1–B3) was never gated and proceeds. Track 2 may prototype, with these binding conditions carried into the bead gates: **B6 MUST close NF-1 and NF-2 (nonce uniqueness) and resolve Residual B's OAEP back-compat; B5 MUST resolve Residual A to an asymmetric signature; B7 MUST resolve Residual C (GCM+RED framing).** Re-audit the B5/B6/B7 implementations at their review gates (web-security-auditor is already on those beads). No remaining architectural blockers.
