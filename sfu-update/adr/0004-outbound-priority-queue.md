# ADR-0004: Outbound Priority Queue (class-aware drop)

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** overseer (malexander)
- **Related:** [`PLAN.md` — Phase 5](../PLAN.md#phase-5--outbound-priority-queue-with-class-aware-drop-23-days), [ADR-0001](0001-routing-header-out-of-encryption.md) (`RoutingHeader` supplies the classification inputs), [`capacity-model.md`](../capacity-model.md), bead `vc-c4e.6`.

## Context

Every per-receiver outbound path in the server today is a single bounded `tokio::sync::mpsc` channel that carries *every* class of traffic for that receiver in FIFO order. The current size is 256 slots, defined at `actix-api/src/webtransport/mod.rs:351`:

```rust
let (outbound_tx, outbound_rx) = mpsc::channel::<WtOutbound>(256);
```

When the channel is full, the producer side (`WtChatSession::handle` at `wt_chat_session.rs:324-353`) calls `send_auto` → `try_send`; on `Err(Full)` it returns `WtSendResult::Dropped` and notifies `CongestionTracker` via `self.logic.on_outbound_drop(sender_session_id)` (`session_logic.rs:77-155`). The tracker counts drops in a 1s window (`CONGESTION_WINDOW`) and, when `CONGESTION_DROP_THRESHOLD = 5` drops occur, emits a `CONGESTION` `PacketWrapper` to the sender so it steps down quality.

This design has three failure modes that get worse as we move from "pub/sub fanout" to "real SFU":

1. **Head-of-line block on audio behind video bursts.** A VP9 keyframe at simulcast S2 is ~1.5 MB. After Phase 1, that frame is sent as ~1250 sequential `MediaPacket` chunks (≤1200 B payload per WebTransport UniStream/datagram). At 200 receivers in a webinar, every receiver's outbound queue sees the same burst arriving inside ~10–20 ms. A receiver whose link can't drain 1250 packets in 20 ms tail-drops indiscriminately at slot 256 — and *the next packet to arrive may be an Opus frame*. Audio gets buried behind video that the receiver was going to discard the layer for anyway. Audio loss is what users hear; video stutter is what users tolerate. The current queue inverts that priority.

2. **Recovery cost is asymmetric.** Audio packets are independent (Opus is self-contained per 20 ms frame). Video packets are not — drop one chunk of a keyframe and the *whole frame* is unusable for the receiver, who then sends a `KEYFRAME_REQUEST` on the existing recovery path. Median keyframe-request recovery time is ~500 ms (measured in `tests/integration/test_keyframe_recovery.rs`). So a tail-drop that loses 1 chunk of a keyframe costs 500 ms of black video; a tail-drop that loses 1 audio frame costs 20 ms of muted audio. The FIFO design dispenses drops in proportion to traffic volume (mostly video), which maximizes the *expected* recovery cost.

3. **Congestion signal is too coarse to act on.** `CongestionTracker` today knows "this receiver dropped a packet from this sender" but not "what *kind* of packet was dropped". A receiver that drops 5 enhancement-layer P-frames in a second is fine — they can decode T0/S0 only. A receiver that drops 1 keyframe chunk is *not* fine — they're already heading for a 500 ms black-screen recovery and should be told to step *down* immediately, not after 5 such drops. The single threshold conflates "fine" with "broken".

The Phase 1 wire work (ADR-0001) lifts the routing-relevant fields out of the encrypted payload. `RoutingHeader.is_keyframe`, `temporal_layer_id`, and `spatial_layer_id` are now readable on the unencrypted envelope. We have, for the first time, enough information on the outbound path to decide *which packet to drop* without decrypting anything.

The proposal is to replace the FIFO `mpsc(256)` with a **5-class priority queue** keyed off `PacketWrapper.packet_type` + `RoutingHeader`, with per-class depth, per-class drop policy, strict priority dequeue with a fairness quantum, and a class-aware `CongestionTracker`. This is the standard SFU outbound design (LiveKit, mediasoup, Janus all run some variant); ADR-0001 is what makes it implementable while preserving E2EE.

## Decision

**The per-receiver outbound `mpsc::channel::<WtOutbound>(256)` is replaced by a `PrioritySender` of 5 inner bounded channels, with strict priority dequeue plus an 8-packet fairness quantum. `CongestionTracker` gains a class-aware drop API so P2 (keyframe + base T0 video) triggers `CONGESTION` after 1 drop and P4 (enhancement + screen) keeps the current 5-drops-per-1s threshold.**

### 1. Class taxonomy

| Class | Slot count | Drop policy | What goes here |
| --- | --- | --- | --- |
| P0 Control | 32 | never drop; if full, log + stop session | RTT echo, heartbeat, `SESSION_ASSIGNED`, `MEETING_*`, `CONGESTION`, `SPEAKER_UPDATE`, `SUBSCRIPTION_UPDATE`, `ADMISSION_DECISION`, `LAYER_HINT`, `CAPABILITY_ANNOUNCE` |
| P1 Audio | 128 | tail-drop oldest in P1 | every `PacketType::MEDIA` packet whose media kind is AUDIO |
| P2 Keyframe + base T0 video | 128 | tail-drop oldest in P2 | `MEDIA` video with `routing_header.is_keyframe == true` **or** (`temporal_layer_id == 0 && spatial_layer_id == 0`) |
| P3 Base-spatial video P-frames | 256 | tail-drop oldest in P3 | `MEDIA` video, non-keyframe, `spatial_layer_id == 0`, `temporal_layer_id > 0` |
| P4 Enhancement + screen | 256 | head-drop oldest in P4 | `MEDIA` video with `spatial_layer_id > 0`; all SCREEN-share media |

Slot counts are chosen so each class can absorb one full normal-shape unit of its own traffic at the worst-case rates from `capacity-model.md` (one Opus frame stream's 20 ms cadence for P1, one keyframe burst chunked over UniStreams for P2, etc.) without spilling into the next class's queue.

**Drop policy rationale.** Tail-drop preserves the head — the oldest packet is what the writer is about to send. For audio and base-layer video, that ordering is what the decoder wants. Head-drop on P4 inverts it: if enhancement frames are backing up, the freshest one is the only one a receiver still wants (they're going to drop the old ones at the decoder anyway because they reference already-late base frames). Head-drop on enhancement minimizes the *time-to-current* of what the receiver actually decodes.

### 2. Classification

Classification happens **on the producer side** (`WtChatSession::handle`/`WsChatSession::handle`) using the *already-parsed* `PacketWrapper` at `wt_chat_session.rs:333-338`. No second `parse_from_bytes` call. The pseudocode:

```rust
fn classify(pw: &PacketWrapper) -> Class {
    use PacketType::*;
    match pw.packet_type.enum_value_or_default() {
        // Anything not MEDIA / not a media envelope is control.
        AGGREGATE | RTT | HEARTBEAT | CONNECTION |
        SESSION_ASSIGNED | MEETING_LEAVE | MEETING_BANNED |
        MEETING_JOIN | MEETING_ACTION | MEETING_CONFIG |
        CONGESTION | SPEAKER_UPDATE | SUBSCRIPTION_UPDATE |
        ADMISSION_DECISION | LAYER_HINT | CAPABILITY_ANNOUNCE => Class::P0,

        MEDIA => {
            let media = MediaPacket::parse_from_bytes(&pw.data).ok();
            let kind = media.as_ref().and_then(|m| m.media_type.enum_value().ok());
            let header = media.as_ref().and_then(|m| m.routing_header.as_ref());
            match kind {
                Some(MediaType::AUDIO) => Class::P1,
                Some(MediaType::SCREEN) => Class::P4,
                Some(MediaType::VIDEO) => match header {
                    Some(h) if h.is_keyframe
                        || (h.temporal_layer_id == 0 && h.spatial_layer_id == 0) => Class::P2,
                    Some(h) if h.spatial_layer_id > 0 => Class::P4,
                    Some(_)  => Class::P3,
                    // Legacy clients without RoutingHeader: treat as base-layer P-frame.
                    None => Class::P3,
                },
                _ => Class::P3,
            }
        }
    }
}
```

The `MediaPacket::parse_from_bytes` cost here is the same parse `WtChatSession::handle` already does at `wt_chat_session.rs:333` to extract `session_id` and `packet_type`. Phase 1 + Phase 2 changes thread the parsed `MediaPacket` through `OutboundDecision` (see `PLAN.md` §p2-4) so classification reuses it; the ADR's contract is "no extra parse on the hot path", not "exactly one parse".

Legacy clients (no `SFU_ROUTING_HEADER` capability) land in P3 by default, which is the right thing: they get base-layer behaviour with a generous queue. They never benefit from P2 keyframe prioritization (because the SFU can't tell what's a keyframe), but they also never get worse treatment than today's FIFO gives them.

### 3. Dequeue: strict priority with 8-packet fairness quantum

The consumer (`webtransport/bridge.rs::spawn_writer`, currently a single `outbound_rx.recv().await` loop, and the WebSocket analog) becomes:

```rust
loop {
    let class = select_next_class(&mut state);  // strict priority + quantum
    let msg = state.queues[class].recv().await; // or break if shut down
    write_to_transport(msg).await;
    state.served[class] += 1;
}
```

`select_next_class` is strict priority with a **fairness quantum of 8**: after 8 consecutive packets from class C, the next dequeue *must* check class C+1..P4 first. If a lower class has work, serve one packet from it, then resume strict priority. This caps the worst-case starvation of P4 by a sustained P0–P2 stream and keeps screen-share alive during talky-with-keyframes bursts.

The quantum of 8 is chosen to be small relative to the audio cadence (P1 generates one packet per ~20 ms; the writer drains a queue in microseconds, so 8 P1 packets is ~160 ms wall-clock at *generation* rate but milliseconds at *drain* rate — the quantum bites only when a higher class is *continuously* full, which is the starvation condition we care about).

**Worst-case audio scheduling latency** = P0 queue depth × per-packet wire time. With P0 = 32 slots and a 25 Mbps egress link, P0 drains in ≈4 ms even fully loaded; P1 sees P0 ahead of it but never more than P0's depth — so audio scheduling latency stays under ≈4 ms in the worst case. This is well inside Opus's 20 ms frame budget.

### 4. Class-aware CongestionTracker

`CongestionTracker` (`session_logic.rs:77-155`) gains a sibling of `record_drop`:

```rust
pub fn record_drop_with_class(
    &mut self,
    sender_session_id: u64,
    class: Class,
) -> Option<u64>;
```

Per-class thresholds:

| Class | Threshold | Window | Notify min interval |
| --- | --- | --- | --- |
| P0 | n/a — never drops; channel-full is a session-fatal log + `ctx.stop()` |  |  |
| P1 | 3 drops | 1 s | 1 s (existing `CONGESTION_NOTIFY_MIN_INTERVAL`) |
| P2 | **1 drop** | 1 s | 1 s |
| P3 | 5 drops | 1 s | 1 s (matches today's `CONGESTION_DROP_THRESHOLD`) |
| P4 | 5 drops | 1 s | 1 s |

The P2 threshold of **1** is the key behavioural change: a single dropped keyframe chunk costs ~500 ms of black video (see Context §2), so we want the `CONGESTION` packet on the wire *before* the receiver issues a `KEYFRAME_REQUEST`. Sending `CONGESTION` to the *sender* lets it step down its spatial layer or temporal-layer cadence before the next keyframe; in practice, this collapses the loop from "drop → 500 ms recovery → step down" to "drop → step down before next keyframe".

P4's 5-drops-in-1s preserves today's threshold for enhancement traffic, which is what most production receivers see most of the time. P3 also stays at 5 because base-layer P-frames are independently decodable until the next keyframe — a single drop is a glitch, not a recovery cycle.

The existing `record_drop(sender_session_id)` API stays for backwards compat (callers that don't yet know the class); it delegates to `record_drop_with_class(.., Class::P3)`. New constants for the per-class thresholds live in `constants.rs` next to the existing `CONGESTION_DROP_THRESHOLD`, with intra-doc cross-references.

### 5. Wiring on producer side

`WtChatSession::handle` (and the WS twin) replace their single `outbound_tx: mpsc::Sender<WtOutbound>` field with a `PrioritySender<WtOutbound>` carrying the 5 inner senders. The hot path becomes:

```rust
let class = classify(&parsed_packet_wrapper);
match self.outbound_tx.try_send_class(class, outbound) {
    Ok(()) => {},
    Err(TrySendError::Full(_)) => {
        if class == Class::P0 {
            error!("P0 control queue full on session {}", self.logic.id);
            ctx.stop();  // P0 never drops; fail the session.
            return;
        }
        // Apply class drop policy and record.
        self.outbound_tx.apply_drop_policy(class);
        if sender_session_id != 0 {
            self.logic.on_outbound_drop_with_class(sender_session_id, class);
        }
    }
    Err(TrySendError::Closed(_)) => ctx.stop(),
}
```

`apply_drop_policy(class)` is the source of "tail-drop oldest" / "head-drop oldest" — it operates on the *inner* channel for that class so we never disturb higher-priority traffic. For tail-drop it does a non-blocking `try_recv` to evict the oldest and then retries the `try_send`; for head-drop (P4) it consults the freshly-pushed item and evicts the *new* one (i.e., the writer just discards the newly arrived enhancement packet — that's the "freshest received but the queue was full of fresher" inversion P4 wants).

### 6. Metrics

`metrics.rs` gains per-class Prometheus counters at `sfu_outbound_dropped{class=P1|P2|P3|P4}` and a gauge `sfu_outbound_queue_depth{class=...}` sampled lazily on each enqueue. P0 gets a separate alarm-path counter `sfu_outbound_p0_full_total` because every increment is a session-fatal event (see Risk #3). This is the Phase 5 contribution to Open Risk #5 (Observability) in `PLAN.md`.

### 7. Both transports

The change applies to both WebTransport (`webtransport/{mod.rs, bridge.rs}` + `transports/wt_chat_session.rs`) and WebSocket (`transports/ws_chat_session.rs` + the WS analog of the spawn-writer). The two transports already have parallel session actors and parallel outbound channels (see `PLAN.md` §"Critical Files"); `PrioritySender<T>` is generic over the payload type so both reuse the same wrapper.

## Consequences

**Pro:**

- **Audio survives video bursts.** The principal goal: a 10 MB video burst into a 1 Mbps receiver drops *enhancement* video first, then base-layer P-frames, then base-layer keyframes — and audio is unaffected because P1 has its own queue that the video burst cannot back up into. The exit criterion in `PLAN.md` Phase 5 ("audio loss <0.1% during a 10 MB burst to a 1 Mbps receiver") is the measurable form of this.
- **Keyframe-aware backpressure.** P2 with threshold 1 means a single keyframe-chunk drop produces a `CONGESTION` packet to the sender *before* the receiver issues a `KEYFRAME_REQUEST`. The recovery loop collapses from ~500 ms (drop → wait for receiver request → next keyframe) to ~one wire RTT (drop → server tells sender → next encoded frame at lower layer).
- **No new parse on the hot path.** Classification reads `packet_type` + `routing_header` from the same `MediaPacket` the actor is already parsing for `session_id`. The cost added per outbound packet is a 5-arm match.
- **Class-aware drop counter feeds the layer selector.** Phase 4's layer selector (`sfu/layer_selector.rs`) consumes `CongestionTracker`'s output; when it can distinguish "P2 distress" from "P4 noise" it can downgrade the sender's spatial budget without overreacting to enhancement-layer churn.
- **Backwards-compatible with legacy clients.** Receivers without `SFU_ROUTING_HEADER` capability get P3 behaviour by default — a 256-slot tail-drop queue, which is no worse than today's `mpsc(256)`. Senders without `RoutingHeader` produce video that the SFU can't classify as keyframe; those land in P3 too. Mixed rooms work without coordination.
- **Generic over transports.** A single `PrioritySender<T>` wrapper covers both WebTransport (`WtOutbound`) and WebSocket; no transport-specific drop logic.

**Con:**

- **5× the channel state per session.** Memory: 5 channels × ~256 slots × 1500 B avg payload ≈ 1.9 MB per session × 200 sessions ≈ **380 MB RAM per pod** in the worst case. `PLAN.md` §"Capacity Model" budgets 200 MB for this (slightly under-counted) — actual is ~2× that. Fine on 8 GB pods, worth noting in the capacity model. Action item in the Implementation section bumps `capacity-model.md`.
- **Producer-side classification adds a `MediaPacket::parse_from_bytes` on the outbound path *for receivers that don't already parse it*.** Today, only some outbound handlers do the full `MediaPacket` parse — the `PacketWrapper`-only path is enough for legacy fanout. Phase 5 makes the full media parse mandatory on every outbound enqueue. Cost is ~1–2 µs per packet on contemporary x86 (protobuf-rust benchmarks); at ~14 000 outbound packets/s peak per pod (capacity model), that's ~30 ms/s of CPU per pod. Negligible, but a real ~1% baseline CPU increase. Phase 2's `OutboundDecision` plumbing should be designed to *share* this parse with the forwarder (single parse, both decisions made on the same in-memory `MediaPacket`).
- **Fairness quantum is a tunable, not a proof.** "8 packets before peeking lower" is a knob, not a guarantee. We chose 8 because it caps starvation at ≤8 P0/P1/P2 packets between any two P4 packets while still being small enough that drain order stays predictable. If we observe P4 starvation in load tests, we tune; if we observe P1 latency growth, we tune the other way. This is an operational decision, not a wire/contract one — no other component depends on the value.
- **P2 threshold of 1 makes `CONGESTION` noisier in the steady state.** A single transient drop now generates a `CONGESTION` packet, where today it takes 5. Existing rate-limiting (`CONGESTION_NOTIFY_MIN_INTERVAL = 1 s`) caps the *rate* of notifications, but the *count* of CONGESTION packets on the system goes up. Client-side, the existing `congestion_step_down` handler (`videocall-client/src/client/video_call_client.rs:1364-1381`) is idempotent — it sets a flag, not a state machine — so duplicate `CONGESTION`s collapse harmlessly. Acceptable; called out so future client work doesn't accidentally make the handler stateful.
- **P0 saturation is now fatal.** Today, a full outbound channel drops the packet and continues. With the new scheme, a full *P0* channel is "log + `ctx.stop()`" because control packets must never be silently dropped (a dropped `SESSION_ASSIGNED` is unrecoverable for the affected receiver; a dropped `CONGESTION` is a feedback-loop failure). We're trading "occasional silent control-packet loss" for "rare loud session abort", which is the right direction for a system that needs to be diagnosable, but it means a misbehaving link can kill a session that today might limp on. Monitored via `sfu_outbound_p0_full_total`; if we see it fire, that's a real bug to chase, not noise.
- **Test surface area grows.** The unit tests for `PrioritySender` need to cover: priority ordering under contention, fairness-quantum non-starvation, tail-drop vs head-drop semantics, P0-full-stops-session, and class-classification correctness on legacy + new clients. `PLAN.md` §p5-8 has these; this ADR's only addition is to insist they be in `actix-api/src/sfu/priority_queue.rs` next to the implementation, not in the transports.

**Mitigations / things this ADR explicitly does NOT do:**

- Does not implement per-receiver congestion-controlled pacing (BBR/GCC). The class boundaries here are not a substitute for end-to-end congestion control; they're a fairness policy on a *bounded* queue. Pacing happens at the QUIC layer (WebTransport) and at the receiver layer (browser's underlying CC). If we later need server-side pacing across receivers, it's a sibling ADR, not an extension of this one.
- Does not change the inbound queueing discipline. `PLAN.md` §locked-decision #8 explicitly keeps inbound as bounded-with-drop; this ADR touches only the outbound path. Inbound saturation has different physics (per-room ingest is bounded by sender count, not receiver count) and a different fix surface.
- Does not add per-stream priority within a class. P2 doesn't distinguish "keyframe chunk 1 of N" from "keyframe chunk N-1 of N". If we observe that intra-keyframe ordering matters (e.g., dependency-descriptor-based salvaging of partial keyframes), that's a future refinement; today's decoders treat a partial keyframe as a whole-frame loss.
- Does not change the wire format. Class assignment is purely a server-side decision computed from existing `RoutingHeader` + `PacketWrapper` fields. Clients don't know which class their packet ended up in and don't need to.
- Does not adjust the existing `mpsc(256)` *inbound* echo path (RTT, heartbeat). Those are control-class on the producer side already; the inbound channel that delivers them to the actor is unchanged.

## Implementation

Tracked under `PLAN.md` §"Convoy P5" / beads `p5-1`..`p5-10` (children of `vc-c4e.6`):

- [ ] `actix-api/src/sfu/priority_queue.rs` — `PrioritySender<T>` + `PriorityReceiver<T>` + 5 inner bounded `mpsc`s + `try_send_class`/`recv`/`apply_drop_policy` + classification fn (`p5-1`, `p5-2`, `p5-3`).
- [ ] `actix-api/src/webtransport/mod.rs:351` — replace `mpsc::channel::<WtOutbound>(256)` with `PrioritySender::<WtOutbound>::new(class_sizes)` (`p5-4`).
- [ ] `actix-api/src/webtransport/bridge.rs::spawn_writer` — strict-priority dequeue with 8-packet fairness quantum (`p5-2`).
- [ ] `actix-api/src/actors/transports/wt_chat_session.rs` — classify on the *already-parsed* `PacketWrapper`/`MediaPacket`; `try_send_class`; route `Err(Full)` into `on_outbound_drop_with_class` (`p5-4`).
- [ ] `actix-api/src/actors/transports/ws_chat_session.rs` — WS analog of the above wiring (`p5-5`).
- [ ] `actix-api/src/actors/session_logic.rs` — `record_drop_with_class`; per-class state in `CongestionTracker`; keep `record_drop` as a `Class::P3` shim (`p5-6`, `p5-7`).
- [ ] `actix-api/src/constants.rs` — per-class `CONGESTION_DROP_THRESHOLD_{P1,P2,P3,P4}`; `OUTBOUND_QUEUE_SIZE_{P0,P1,P2,P3,P4}`; `OUTBOUND_FAIRNESS_QUANTUM` (`p5-1`).
- [ ] `actix-api/src/metrics.rs` — per-class Prometheus counters + queue-depth gauges + P0-full alarm counter (`p5-10`).
- [ ] `actix-api/src/sfu/tests/priority_queue_test.rs` — unit: ordering, fairness, drop policy per class, P0-stop-on-full, classification for legacy + new clients (`p5-8`).
- [ ] `actix-api/tests/integration/test_outbound_burst.rs` — synthetic 10 MB burst into a 1 Mbps receiver; assert audio loss <0.1%, video loss rises smoothly, no HOL (`p5-9`).
- [ ] `sfu-update/capacity-model.md` — update mpsc-backlog row to ~380 MB / pod worst case.

Phase 5 depends on Phase 1 (`RoutingHeader` on the wire) and Phase 2 (forwarder plumbing through `OutboundDecision`) to source the classification inputs; it precedes Phase 6 (room affinity) but is independent of it.

## Rejected alternatives

**Alternative A — Single FIFO with a larger queue (e.g., `mpsc(2048)`).** Keep one channel, just bump its capacity so bursts don't tail-drop. **Rejected** because it trades the *probability* of drop for the *latency* of drained packets — a 2048-slot queue full of video adds ~30 ms of *head-of-line* delay to the next audio packet at typical drain rates. Worse for users than the current design. Also doesn't solve the "audio drops behind video" inversion: a tail-drop at slot 2048 still throws away the most recent audio packet rather than the dispensable enhancement-layer video chunk.

**Alternative B — Two queues only: audio-vs-everything-else.** A minimal "P1 / not-P1" split that preserves audio without the operational complexity of 5 classes. **Rejected** because Phase 4 (layer selector) needs to *distinguish* keyframe-loss distress (P2) from enhancement-layer churn (P4) to make sensible downgrade decisions. A two-queue split solves the audio HOL problem but loses the class-aware `CongestionTracker` signal, which the layer selector consumes. The marginal complexity of going from 2 to 5 classes is small (one match expression, one extra inner channel each) and the layer-selector value is large.

**Alternative C — Per-sender queues instead of per-class queues.** Give each *sender* a private queue on each receiver's outbound side; drain round-robin. **Rejected** because the cardinality is wrong — at 200 senders × 200 receivers we'd have 40 000 queues per pod, each tiny. Fairness across senders is already provided by the active-speaker selection (ADR-0002), which decides *which* senders this receiver gets at all. We need fairness across packet *classes* (audio vs base video vs enhancement), not across senders.

**Alternative D — `tokio::sync::mpsc::WeakSender` + custom select-loop instead of explicit `PrioritySender`.** Use Tokio's primitive select with priority bias to avoid wrapping the channels. **Rejected** because Tokio's `select!` doesn't natively express "strict priority with N-packet quantum". We'd build the equivalent of `PrioritySender` inside `bridge.rs` with macros instead of methods, sacrificing testability. The explicit wrapper localizes the policy in one file with unit tests, which is worth the small abstraction layer. The wrapper is ~150 lines.

**Alternative E — Drop on the *consumer* side instead of the producer.** Always enqueue into an unbounded channel and have the writer (`bridge.rs`) drop packets at dequeue time based on class. **Rejected** because unbounded channels are a memory-safety hazard — a stuck WebTransport session that can't drain would accumulate the entire room's egress in RAM. Bounded with class-aware drop at *enqueue* keeps the back-pressure where it belongs (at the actor, where we already have `CongestionTracker` wired in) and caps per-session memory deterministically.

**Alternative F — Defer Phase 5 entirely; rely on Phase 4's layer selector to prevent overflow.** If the layer selector is doing its job, the queue should never saturate, so the queue's drop discipline shouldn't matter. **Rejected** because (a) the layer selector reacts on a 1–3 s time-scale (it needs to observe `CONGESTION` and downgrade), whereas a keyframe burst happens in ~10 ms — the queue *will* saturate during transients even with a working selector, and (b) packet loss on bad networks is independent of the SFU's selector — a receiver on a flapping mobile link sees drops the selector can't prevent. The two mechanisms compose: the selector handles steady-state allocation, the queue handles transient bursts.

## Status

**Accepted** 2026-05-17. Frozen for the v1 SFU refactor. Class taxonomy and per-class threshold table are part of the v1 wire contract only at the level of `CONGESTION` semantics (i.e., what causes a `CONGESTION` packet to be emitted); the class definitions themselves are server-internal and can be evolved without a wire bump. Implementation gated on Phase 1 (`RoutingHeader` on the wire) and the Phase 2 `OutboundDecision` plumbing landing first. Supersedes the implicit "single mpsc(256) FIFO" design at `webtransport/mod.rs:351`. Superseded by: none.
