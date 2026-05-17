# ADR-0001: Routing Header Out of Encryption (SFrame-style)

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** overseer (malexander)
- **Related:** [`PLAN.md` — New Wire Surface](../PLAN.md#new-wire-surface-consolidated), [`PLAN.md` — Locked Decisions](../PLAN.md#locked-decisions-from-interview) §2 (E2EE posture), [`packet-diagrams.md`](../packet-diagrams.md), [`rfc-2-sfu-architecture.md`](../../rfc/rfc-2-sfu-architecture.md), bead `vc-c4e.3`.

## Context

`videocall-rs` today is a NATS pub/sub fanout: every encrypted `MediaPacket` is republished to every peer in a room. The README markets "No SFUs", but at the webinar shape we're now targeting (≤10 active video senders, ~190 listeners, up to 200 participants per room) blind fanout collapses on outbound bandwidth — `ARCHITECTURE.md` §"Scaling Considerations" already calls out simulcast and tiered quality as future work, and the per-session `mpsc::channel::<WtOutbound>(256)` at `actix-api/src/webtransport/mod.rs:351` drops packets FIFO once egress saturates.

Turning the server into a real Selective Forwarding Unit means the server has to make per-receiver forwarding decisions:

- **Keyframe gating** — don't forward inter-frames to a receiver that hasn't seen the matching keyframe.
- **Temporal/spatial layer dropping** — give a bandwidth-constrained receiver only the base layer.
- **Active-speaker selection** — pick the top-N speakers each tick from per-packet audio level / VAD hints.
- **Layer-aware keyframe routing** — don't blast a 1.5 Mbps keyframe at a 200 kbps receiver.
- **Class-aware drop** in the outbound priority queue ([ADR-0004](0004-outbound-priority-queue.md)) — the class of a packet (P2 keyframe vs P4 enhancement) is a function of its `is_keyframe` / `temporal_layer_id` / `spatial_layer_id` fields.

Every one of those decisions needs information that today lives **inside** the encrypted payload: VP9 SVC layer ids in the encoded frame metadata, audio RMS in the Opus payload, keyframe flag in the codec bitstream. The end-to-end encryption posture this project ships with is one of its load-bearing product claims — the server is **not** a member of the E2EE group and must never hold the media key. So the SFU literally cannot read what it needs.

There are two ways out:

1. **Server-side decryption.** Add the server to the E2EE group, decrypt the payload, route, then re-encrypt or pass through. This is what classic SFUs do without E2EE; it requires giving up the project's core security property.
2. **Lift routing fields out of the encrypted payload.** Carry them in the clear on `MediaPacket`, SFrame-style ([draft-ietf-sframe-enc](https://datatracker.ietf.org/doc/draft-ietf-sframe-enc/) is the IETF reference). The server reads the header for routing; the payload stays opaque and is decrypted only by participants. This preserves E2EE in evolved form: the *content* of frames is still end-to-end encrypted, only the *metadata required to route them* travels in the clear.

This ADR captures the decision to adopt (2). It is the central security/architecture trade-off of the whole SFU refactor — almost every downstream ADR ([0002](0002-active-speaker-detection.md), [0003](0003-hybrid-subscription-model.md), [0004](0004-outbound-priority-queue.md)) consumes one or more of the fields defined here.

## Decision

**`MediaPacket` gains an unencrypted `RoutingHeader` submessage. The media payload remains encrypted end-to-end. The SFU reads the header to make per-receiver forwarding decisions; it never decrypts the payload.**

Concretely:

1. **Wire format.** Add a new submessage to `protobuf/types/media_packet.proto`, exactly as enumerated in `PLAN.md` §"New Wire Surface":

   ```protobuf
   message RoutingHeader {
     bool   is_keyframe       = 1;
     uint32 temporal_layer_id = 2;   // 0=base, 1..N=enhancement
     uint32 spatial_layer_id  = 3;   // 0=base, 1..N=enhancement
     float  audio_level       = 4;   // 0..1 RMS, AUDIO only
     bool   is_speaking       = 5;   // VAD/threshold hint
     uint32 frame_marker      = 6;   // bitfield: START_OF_FRAME=1, END_OF_FRAME=2, REFERENCES_T0=4
     uint64 picture_id        = 7;   // for SVC dependency tracking
   }
   ```

   `MediaPacket.routing_header` is a new optional field (tag 10). All fields are proto3-optional with sensible defaults so legacy clients that omit the header are still parseable — the forwarder treats a missing header as "no hints, forward as legacy fanout to this receiver".

2. **Server contract.** The server (`actix-api/src/sfu/forwarder.rs`, introduced in Phase 2) **reads `RoutingHeader` and the unencrypted `PacketWrapper` envelope only**. It never attempts to parse the encrypted payload. The forwarder consumes the header through the `OutboundDecision` plumbing on `actix-api/src/actors/session_logic.rs` and produces `Forward(bytes) | Drop` decisions per receiver.

3. **Client contract.** Senders populate the header from sources already available pre-encryption:
   - **Video** (`videocall-client/src/encode/camera_encoder.rs`, `screen_encoder.rs`): `is_keyframe`, `temporal_layer_id`, `spatial_layer_id`, `frame_marker`, `picture_id` come from WebCodecs `EncodedVideoChunk` metadata (`svc.temporalLayerId`, chunk `type === 'key'`, dependency descriptor).
   - **Audio** (`videocall-client/src/encode/microphone_encoder.rs`): `audio_level` is computed as RMS of the **pre-Opus PCM** sample buffer. `is_speaking` is the existing VAD/threshold bit already used by `HeartbeatMetadata.is_speaking`, promoted from heartbeat-only to per-AUDIO-packet.

   Because routing data is sampled **before** encryption, the encrypted payload remains the authoritative content; the header is a hint, not a duplicate of payload data.

4. **Capability negotiation.** `CONNECTION` packets carry a `client_capabilities` bitmask (`SFU_ROUTING_HEADER=1`, `SVC=2`, `SUBSCRIPTION=4`) so the forwarder can identify legacy clients and fall back to whole-fanout per-receiver. Legacy clients that don't advertise `SFU_ROUTING_HEADER` continue to work in `SFU_MODE=sfu` — they just don't get layer dropping or active-speaker filtering.

5. **Server posture.** Headers are read on the inbound path **only** to make routing decisions. The server does **not** rewrite the header before forwarding; the egress byte sequence for the payload is identical to the ingress. (Tampering with the unencrypted header is detectable by participants if they wish — see Consequences.)

6. **Status: Accepted.** Phase 1 of the SFU plan implements this wire change ([`PLAN.md` Phase 1](../PLAN.md#phase-1--wire-protocol-routing-header--new-packet-types-12-days), beads `p1-1`..`p1-13` / `vc-c4e.17`-equivalent). All downstream ADRs assume these fields exist.

## Consequences

**Pro:**

- **E2EE is preserved for media content.** The actual video/audio payload is still encrypted between participants; the server can neither read it nor inject content into it. This matches WebRTC Encoded Transform / SFrame, which IETF and major browser vendors have already settled on as the standard pattern for SFU + E2EE coexistence.
- **The SFU can do its job.** Every routing decision the forwarder, speaker scorer, layer selector, and priority queue need is sourced from a tiny well-defined header. No payload decryption, no codec parsing in the server.
- **Legacy clients keep working.** Missing-header behaviour is "forward as legacy" per-receiver. Capability negotiation lets the server make a per-receiver split: a mixed room of new-and-old clients runs without manual coordination.
- **Header is small.** ~30–40 bytes per `MediaPacket` after proto3 varint encoding; trivial vs. typical media frame size (audio ~150B, video keyframes ~1.5MB, P-frames 5–30kB). Negligible bandwidth overhead.
- **Header is computed pre-encryption from sources the sender already has.** No extra CPU for the sender beyond an RMS calculation that's already cheap on audio frames.
- **Downstream ADRs become tractable.** [ADR-0002](0002-active-speaker-detection.md), [ADR-0003](0003-hybrid-subscription-model.md), and [ADR-0004](0004-outbound-priority-queue.md) all reduce to "read these fields from `RoutingHeader`"; without this decision they would each need their own out-of-band signalling channel.

**Con:**

- **Metadata leakage.** The header reveals to the SFU (and to any observer with TLS-decryption capability inside the server perimeter): which sender is speaking, layer structure of each frame, keyframe cadence, picture-id sequence numbers. This is a real reduction in the security posture vs. classic E2EE-everywhere: a passive server-side observer can infer who's talking and when, even without payload access. This is the cost of routing in the clear. We accept it as the cost of running an SFU at all; see Rejected alternatives §A.
- **Sender can lie.** A malicious sender can claim `is_keyframe=true` on a non-keyframe, or `audio_level=0.99` while silent. The forwarder must treat header fields as *hints*, not trusted facts:
  - Speaker scoring uses EWMA over time, so a single spoofed packet doesn't dominate ([ADR-0002](0002-active-speaker-detection.md)).
  - Layer selection treats `is_keyframe + T0 + S0` as a routing invariant but defers correctness to the decoder — a wrong flag costs the lying sender a useless forward, not a security breach.
  - Receivers can detect a lying sender post-decryption (the payload doesn't match the header) and surface it in client diagnostics; this is logged but not enforced server-side.
- **Server can lie too.** The server can drop, reorder, or selectively forward based on header content — that's the *point* of having a routing header. Receivers can detect drop patterns (gaps in `picture_id`, missing keyframes after `KEYFRAME_REQUEST`) and signal via `DiagnosticsPacket`. The trust model is: server has routing authority; participants verify content authenticity end-to-end.
- **Field set is now part of the wire contract.** Adding fields later requires another proto bump and capability bit. The seven fields above were chosen to cover Phase 1–6 needs; if a future decoder needs additional dependency info (e.g. AV1 OBU header), that's a new ADR / capability bit, not a quiet schema extension. See Open questions.
- **Encrypted Transform integration deferred.** This ADR specifies *that* the header is unencrypted and the payload is encrypted, but not *how* the existing E2EE wrapper (today applied to the whole `MediaPacket`) gets re-scoped to wrap the payload only. The mechanical proto/client work for that re-scoping is in Phase 1 (`p1-1`..`p1-11`); this ADR is the policy decision.

**Mitigations / things this ADR explicitly does NOT do:**

- Does not add an HMAC over the header. We considered it (see Rejected alternatives §B) and rejected as ceremony — receivers verify payload authenticity directly, and a tampered header costs the attacker a misrouted packet, not a confidentiality breach.
- Does not add ACL on which clients can populate which fields. `IS_RECORDER` capability is a forwarder-behaviour switch (skip layer dropping), not a header-field gate.
- Does not specify codec-specific header semantics beyond VP9 SVC. AV1 / H.264 mappings are explicitly out of scope for v1 (see [`PLAN.md` Out of Scope](../PLAN.md#out-of-scope-for-v1)).

## Implementation

- [ ] `protobuf/types/media_packet.proto` — add `RoutingHeader` submessage + field 10 (`p1-1` / `vc-c4e.17`).
- [ ] `protobuf/types/connection_packet.proto` — `client_capabilities` bitmask (`p1-3`).
- [ ] `videocall-client/src/encode/camera_encoder.rs` — populate header from WebCodecs chunk metadata (`p1-7`).
- [ ] `videocall-client/src/encode/microphone_encoder.rs` — pre-Opus RMS for `audio_level`; propagate existing `is_speaking` (`p1-8`).
- [ ] `videocall-client/src/encode/screen_encoder.rs` — passthrough header (`p1-9`).
- [ ] `videocall-client/src/connection/connection_manager.rs` — advertise `SFU_ROUTING_HEADER` capability (`p1-10`).
- [ ] `actix-api/src/actors/packet_handler.rs` — parse-and-log on inbound; Phase 1 is logging-only (`p1-11`).
- [ ] `actix-api/src/sfu/forwarder.rs` — consume header in `Forwarder::decide` (Phase 2, `p2-3`).

Phase 1 lands the wire and the producer side; Phase 2 lands the consumer (forwarder). Phase 4 (beads `p4-7`..`p4-11`) is where the header's full value cashes in via per-receiver layer dropping.

## Rejected alternatives

**Alternative A — Server-side decryption (classic SFU).** The server joins the E2EE group, decrypts payloads, routes, and re-encrypts (or doesn't). **Rejected** because it permanently destroys the project's E2EE claim — the server becomes a member of the trust group and can read all media. This is what makes most commercial SFUs not E2E-encrypted. Out of scope for this project; if we ever need server-side mixing (the deferred "town hall" audio-mixdown mode noted in [`PLAN.md` Open Risk #1](../PLAN.md#open-risks-escalate-before-each-phase)), it gets a separate room flag and an explicit relaxed-crypto declaration.

**Alternative B — Encrypted header with key escrow to SFU.** Keep the routing fields encrypted, but escrow a routing-only key to the SFU. **Rejected** because the operational cost (key rotation, per-room key material, audit trail) far exceeds the value — an attacker who compromises the server has the routing key by definition, so this is "encrypted in transit but not at the destination" and provides only marginal benefit over an unencrypted header against the realistic threat model. SFrame's design conclusion was the same: routing metadata in the clear is acceptable, content metadata is not.

**Alternative C — Out-of-band signalling channel for routing hints.** Senders publish `audio_level`, `is_speaking`, `temporal_layer_id` etc. on a sidecar NATS subject (`room.{room}.{session}.hints`), keyed to packet sequence numbers, while `MediaPacket` stays fully encrypted. **Rejected** because (a) it doubles message rate (hint message per media packet), (b) ordering between media and hints across two NATS subjects is unreliable, (c) it just moves the metadata leak to a different channel without reducing it. The header-on-packet design is strictly simpler.

**Alternative D — Smaller header (just `is_keyframe` + `temporal_layer_id`).** A more minimal field set, deferring `audio_level` / `is_speaking` / `picture_id` until phases 3–4. **Rejected** because the cost of an additive proto field bump is the same per field, and the downstream ADRs already need every field listed; a partial header would force a second wire bump mid-refactor. Settling the header shape once, before Phase 2, is worth the small extra fields landed unused for one phase.

**Alternative E — Padding / dummy traffic to obscure speaker identity.** Mitigate metadata leakage by having the SFU emit dummy `RoutingHeader` packets from silent senders so passive observers can't infer who's speaking. **Rejected** as v1 over-engineering — the threat model is "honest-but-curious SFU", not "passive attacker observing decrypted server-side traffic". If the threat model strengthens, dummy traffic is a future ADR.

## Status

**Accepted** 2026-05-17. Applies to all SFU wire work from Phase 1 forward. Field set is frozen for the v1 refactor; additions require a new ADR and a capability bit. Supersedes nothing (this ADR introduces the routing-header concept). Superseded by: none.
