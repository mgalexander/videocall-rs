# ADR-0003: Hybrid Subscription Model

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** overseer (malexander)
- **Related:** [`PLAN.md` — New Wire Surface](../PLAN.md#new-wire-surface-consolidated), [`PLAN.md` — Locked Decisions](../PLAN.md#locked-decisions-from-interview) §3 (hybrid selection), [`PLAN.md` Phase 3](../PLAN.md#phase-3--active-speaker-detection--subscription-model-35-days), [ADR-0001](0001-routing-header-out-of-encryption.md), [ADR-0002](0002-active-speaker-detection.md), [ADR-0004](0004-outbound-priority-queue.md), [`packet-diagrams.md` SUBSCRIPTION_UPDATE / SPEAKER_UPDATE](../packet-diagrams.md#subscription_update-packettype--10), bead `vc-c4e.5`.

## Context

In a 200-participant webinar (≤10 active video senders, ~190 listeners) a receiver cannot decode every sender's video — the capacity model in [`PLAN.md` lines 305–309](../PLAN.md#capacity-model-200-participant-webinar) caps each receiver at ~8.8 Mbps under top-6 video + room-wide audio, and the egress budget for the whole pod (1.76 Gbps) is the binding constraint. Blind fanout in `SFU_MODE=sfu` doesn't help: the server must know which streams each receiver actually wants and forward only those.

Two extremes frame the design space:

1. **SFU-driven.** The server picks the top-N speakers each tick and forwards that set uniformly to all receivers. Simple, no client-side control-plane traffic, but it ignores UI state — pinned tiles, grid resize, screen-share focus — and a receiver who *wants* to keep watching a quiet participant has no way to say so. UX failure.
2. **Client-driven.** Each receiver sends an explicit subscription set on every visibility change. Honours UI state perfectly, but every grid re-layout costs a round trip, late joiners have to bootstrap their own speaker discovery, and the control plane fans out per-user instead of per-room.

The locked decision ([`PLAN.md` §17](../PLAN.md#locked-decisions-from-interview) item 3) is **hybrid**: the SFU is authoritative for the default active-speaker set (top-N from [ADR-0002](0002-active-speaker-detection.md)); the client is authoritative for pins and grid-visibility slots. The server reconciles them per-receiver into a single AllowSet that gates forwarding.

This ADR specifies the split, the wire schema (`SubscriptionUpdate`, `VisibilitySlot`), the reconciliation algorithm, the legacy-capability fallback, and the cap policy. It consumes [ADR-0001](0001-routing-header-out-of-encryption.md)'s `RoutingHeader.is_speaking` / `audio_level` indirectly via [ADR-0002](0002-active-speaker-detection.md)'s `default_speaker_set`, and feeds [ADR-0004](0004-outbound-priority-queue.md) the AllowSet against which the priority queue classifies P2/P3/P4 traffic.

## Decision

**Subscription is a declarative, per-receiver replace operation. The SFU reconciles the receiver's declared state with the SFU's authoritative speaker set into an `AllowSet`. Forwarding consults `AllowSet`. No deltas, no acks.**

Concretely:

1. **Split of responsibility.** Server owns `default_speaker_set` (top-N=4 from [ADR-0002](0002-active-speaker-detection.md), recomputed every 200 ms tick). Client owns `pinned_sessions` and `slots` (visibility slots from the grid). The reconciliation formula is exactly:

   ```text
   AllowSet = pinned ∪ default_speaker_set ∪ slot_sessions
   ```

   capped at `max_visible_video = 6` for video. Audio is room-wide when `receive_all_audio = true` (the v1 default). The formula is quoted verbatim from [`packet-diagrams.md` SUBSCRIPTION_UPDATE](../packet-diagrams.md#subscription_update-packettype--10).

2. **Declarative state, not delta.** A `SubscriptionUpdate` packet **replaces** the receiver's prior subscription state in full — there is no `add`/`remove`. Rationale:
   - One-message recovery from packet loss: the next `SubscriptionUpdate` re-establishes ground truth.
   - No missed-delta correctness bugs (lost `remove` → leaked subscription forever).
   - Matches the existing client surface: `videocall-client/src/client/video_call_client.rs:849` (`set_peer_visibility`) already emits whole-set visibility changes when the grid re-layouts, so the wire shape mirrors what the UI already produces.

3. **Wire schema.** Per [`PLAN.md` lines 254–265](../PLAN.md#new-wire-surface-consolidated):

   ```protobuf
   message SubscriptionUpdate {
     repeated uint64         pinned_sessions   = 1;
     repeated VisibilitySlot slots             = 2;
     uint32                  max_video_kbps    = 3;
     bool                    receive_all_audio = 4;   // v1 default true
   }
   message VisibilitySlot {
     uint64 session_id        = 1;
     uint32 preferred_spatial = 2;
     uint32 preferred_temporal = 3;
   }
   ```

   `PacketWrapper.PacketType.SUBSCRIPTION_UPDATE = 10`. Direction: receiver → SFU.

4. **Emit triggers (client).** `SubscriptionUpdate` is emitted from `videocall-client/src/sfu_client.rs` (new in Phase 3) on:
   - Pin / unpin from the UI.
   - Grid resize via `set_peer_visibility` (`videocall-client/src/client/video_call_client.rs:849`).
   - Periodic reconcile tick — a coarse safety net (~5 s; exact cadence is Phase 3 tuning, not part of this ADR). The tick is *not* the fast path; pin/unpin and grid resize are. The tick exists only so a receiver that somehow lost sync can self-heal.

5. **Reconciliation algorithm (server, `actix-api/src/sfu/subscription.rs`).** Two entry points:
   - On `SubscriptionUpdate` arrival: replace the receiver's `(pinned, slots, max_video_kbps, receive_all_audio)` state; recompute `AllowSet` for that receiver only.
   - On `SpeakerUpdate` generation change ([ADR-0002](0002-active-speaker-detection.md)): the `default_speaker_set` changed, so recompute `AllowSet` for **every** receiver in the room. This is O(receivers × cap) — at 200 × 6 it is trivial.

   On sender join: walk every receiver's pending-list; promote any entry whose `session_id` now resolves into the AllowSet computation. On sender leave: drop that `session_id` from every receiver's `pinned`, `slots`, and `pending` lists silently.

6. **Edge cases.**
   - **Stale `session_id`** (peer already left the room): silently dropped from the reconciliation input. No error packet is emitted — this matches [ADR-0001](0001-routing-header-out-of-encryption.md) §"Server can lie too": receivers verify content authenticity end-to-end and the server has routing authority. Surfacing every stale entry would be noise.
   - **Pre-join `session_id`** (peer in the room directory but not yet connected — common during connection waves): held in a per-receiver `pending` list capped at 50. Promoted on peer arrival. The cap of 50 is a deliberate denial-of-service cap, not a UX limit; documented in Consequences.
   - **Cap exceeded.** When `|pinned ∪ default_speaker_set ∪ slot_sessions| > max_visible_video`, the union is truncated. Priority ordering (explicit, since the union notation does not encode it): `pinned` > `slot_sessions` > `default_speaker_set`. Pins are user intent and always honoured; slots are user-visible UI state; the speaker set is a server inference and yields first. If `|pinned| > max_visible_video` itself, the cap holds and all non-pinned senders are dropped.

7. **Capability fallback.** Receivers that do not advertise `client_capabilities & SUBSCRIPTION` get legacy full-fanout for **their own deliveries only** (per-receiver gating, matching [ADR-0001](0001-routing-header-out-of-encryption.md) §4). A SUBSCRIPTION-capable receiver that has not yet emitted a `SubscriptionUpdate` is also treated as legacy fanout until the first packet arrives (`packet-diagrams.md:144-147`) — there is no implicit "subscribe to the room" handshake.

8. **Audio policy.** `receive_all_audio = true` is the v1 default. Justification from [`PLAN.md` lines 305–309](../PLAN.md#capacity-model-200-participant-webinar): 200 senders × 32 kbps = 6.4 Mbps audio per receiver, well within the per-receiver 8.8 Mbps budget. The alternative — server-side audio mixdown — would require the SFU to decrypt and re-encode the audio payload, which is exactly what [ADR-0001](0001-routing-header-out-of-encryption.md) §Rejected A forbids. A future ADR can introduce mixdown for a town-hall room flag with an explicit relaxed-crypto declaration.

9. **Interplay with ADR-0002.** [ADR-0002](0002-active-speaker-detection.md) emits `SpeakerUpdate.generation` on every set change. The SFU's reconciliation does **not** wait for the client to acknowledge a `SpeakerUpdate` — it recomputes `AllowSet` immediately on the new generation and switches forwarding on the next packet. The generation counter lets a client detect that its tile UI is stale relative to what the SFU is sending, but the SFU is not blocked on the client.

10. **`max_video_kbps` is a budget hint, not a subscription mechanism.** It feeds [ADR-0004](0004-outbound-priority-queue.md) / Phase 4's `layer_selector`. The `AllowSet` decides *which* senders are forwarded; `max_video_kbps` (combined with the receiver's estimated downlink) decides *which layers* of each allowed sender. These two concerns are orthogonal and live in separate modules (`subscription.rs` vs `layer_selector.rs`).

## Consequences

**Pro:**

- **UI fidelity.** Pins are always honoured (subject to the cap). A receiver who wants to keep watching a quiet participant gets exactly that, with one packet of round-trip cost.
- **Server-driven discovery.** A new speaker in the room becomes visible to every receiver via `default_speaker_set` without any client-initiated subscription action. Late joiners do not need to subscribe to "the room" — the speaker set gives them a sensible default immediately.
- **Trivial recovery.** Declarative replace means a lost or reordered `SubscriptionUpdate` is corrected by the next one. No reconciliation protocol, no resync RPC.
- **Bounded per-receiver state.** Per receiver: `pinned` (small `Vec<u64>`), `slots` (`HashMap<u64, VisibilitySlot>` ≤ 6 entries), `pending` (≤ 50). At 200 receivers per pod the total memory footprint is in the low MB.
- **Legacy clients unaffected.** Per-receiver capability gating mirrors [ADR-0001](0001-routing-header-out-of-encryption.md): a mixed room of new and old clients runs without any operator coordination — the old clients just don't get the AllowSet optimisation.
- **No subscription handshake on join.** A new receiver gets legacy fanout immediately and upgrades to AllowSet-gated forwarding on its first `SubscriptionUpdate` — a single message, not a multi-step negotiation.

**Con:**

- **Speaker-churn fan-out cost.** On every `SpeakerUpdate` generation change, the SFU recomputes `AllowSet` for every receiver — O(receivers × cap). At today's webinar shape (200 × 6) this is trivial; at conference shape (200 × 200) it becomes the dominant cost in `subscription.rs`. Flag for §K when scaling beyond webinar.
- **Pin-churn rate.** A user mashing pin/unpin emits one `SubscriptionUpdate` per click. The existing `KEYFRAME_REQUEST` per-receiver rate-limit at `actix-api/src/actors/packet_handler.rs:115-143` is the structural analogue; a similar limit on `SUBSCRIPTION_UPDATE` is advisable. Tuning deferred to Phase 3.
- **Pre-join cap is a silent drop.** A user who pre-pins 51 future participants loses the 51st silently. Recovery is re-pinning post-join. Documented; not surfaced to UI for v1.
- **Empty declarative update is a footgun.** A client that emits `SubscriptionUpdate { pinned=[], slots=[], … }` will revert to default speaker set + empty pins/slots. The mitigation is on the client: never emit empty unless empty is intended.
- **Capability-bit space is shared with ADR-0001.** `SUBSCRIPTION = 4` consumes a bit in the `client_capabilities` bitmask defined in [ADR-0001](0001-routing-header-out-of-encryption.md) §4. Future capability additions must coordinate across both ADRs.

**Mitigations / things this ADR explicitly does NOT do:**

- Does **not** specify the per-receiver rate-limit value for `SubscriptionUpdate`. Phase 3 tuning concern.
- Does **not** introduce a server-emitted `SubscriptionAck`. Declarative replace makes acks redundant — the receiver's next emission is its own confirmation, and the AllowSet is observable in the forwarding behaviour.
- Does **not** address simulcast-layer mapping inside the `AllowSet`. The AllowSet governs *which* senders are forwarded; layer choice within an allowed sender is [ADR-0004](0004-outbound-priority-queue.md) territory.
- Does **not** support per-track subscription (e.g. "camera but not screen-share from session X"). v1 keys subscriptions by `session_id` only. A future ADR can add a `track_kind` field with an additive proto bump and a new capability bit.

## Implementation

- [ ] `protobuf/types/subscription_packet.proto` (new) — `SubscriptionUpdate`, `VisibilitySlot` (Phase 1, beads `p1-4`).
- [ ] `protobuf/types/packet_wrapper.proto` — `SUBSCRIPTION_UPDATE = 10` PacketType addition (Phase 1).
- [ ] `actix-api/src/sfu/subscription.rs` — reconciliation engine: state replace, AllowSet computation, sender-join promotion, sender-leave cleanup, pending-list cap (Phase 3, beads `p3-7`/`p3-8`).
- [ ] `actix-api/src/sfu/forwarder.rs` — consult AllowSet in `Forwarder::decide`; legacy-capability fallback path (Phase 3).
- [ ] `videocall-client/src/sfu_client.rs` (new) — emit `SubscriptionUpdate` from pin/unpin, grid hooks, periodic tick. Wires to `videocall-client/src/client/video_call_client.rs:849` (`set_peer_visibility`).
- [ ] `videocall-client/src/decode/peer_decode_manager.rs` — surface visibility changes to `sfu_client`.
- [ ] `actix-api/src/sfu/tests/subscription_tests.rs` — reconciliation matrix: pin, slot, stale, pre-join, oversize-with-pin-priority, no-SUBSCRIPTION-capability legacy fallback, speaker-churn recompute (bead `p3-10`).

Phase 1 lands the wire; Phase 3 lands the reconciliation engine and the client emit path.

## Rejected alternatives

**Alternative A — Pure SFU-driven (top-N only).** Drop pins and slots entirely; the SFU forwards only its computed `default_speaker_set` to every receiver. **Rejected** because it ignores UI state. Pinning a quiet participant is impossible. Screen-share focus is impossible. The user's request for "show me person X" cannot be satisfied. The webinar UX explicitly depends on user-controllable pinning (host pinning a panellist; attendee pinning a translator) — this alternative would force every such use case onto a different mechanism.

**Alternative B — Pure client-driven (every visibility change is a round trip).** Drop `default_speaker_set` from the AllowSet; receivers must explicitly subscribe to everyone they want to see. **Rejected** because (a) new joiners would receive nothing until they do speaker discovery in client code — a pattern every client implementation would have to reinvent; (b) every receiver pays the round-trip cost of subscribing to new speakers as they arrive; (c) legacy clients can't subscribe at all, so they would receive nothing. The whole point of "hybrid" is that the server's speaker inference is free, useful, and works across capabilities.

**Alternative C — Delta-based subscription (`add: [...], remove: [...]`).** Replace the whole-set `SubscriptionUpdate` with incremental add/remove. **Rejected** because deltas open a class of correctness bugs — a lost `remove` leaks the subscription forever, reordered add/remove pairs produce wrong final state, reconnection requires a full resync RPC. Subscription update is not on the hot path (it fires on UI events, not per-media-packet), so the bandwidth savings of deltas are negligible against media traffic. Declarative replace is strictly simpler with strictly better failure modes.

**Alternative D — Server-emitted `SubscriptionAck`.** Have the SFU acknowledge each `SubscriptionUpdate` with a confirmation packet. **Rejected** because the declarative-replace model makes acks redundant: the receiver's next emission is its own confirmation, and any inconsistency is healed by the periodic reconcile tick. Acks would double the control-plane traffic for no correctness gain.

**Alternative E — Per-track subscription (camera vs screen-share separately).** Allow `SubscriptionUpdate` to subscribe to a specific track kind from a session rather than the whole session. **Rejected for v1** because it inflates the wire surface (`VisibilitySlot` would need a `track_kind` enum, AllowSet keying becomes a tuple, layer selection per-track per-sender), and the dominant v1 use case is "show this participant's main video" — screen-share is currently delivered as a separate `MediaPacket.MediaType=SCREEN` from the same session, which the client already handles as a distinct tile. A future ADR can revisit if real product demand emerges.

## Status

**Accepted** 2026-05-17. Applies to all subscription-model work from Phase 3 forward. Wire shape (`SubscriptionUpdate`, `VisibilitySlot`) is frozen for the v1 refactor; additions require a new ADR and (if non-additive) a capability bit. Supersedes nothing. Superseded by: none.
