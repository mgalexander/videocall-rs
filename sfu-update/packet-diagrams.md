# Packet Diagrams — SFU Refactor

> **Status:** Skeleton. To be filled out under bead `vc-c4e.9`.
>
> **Source of truth:** [`PLAN.md` — New Wire Surface](./PLAN.md#new-wire-surface-consolidated).
>
> Each section below should contain:
> - Field layout (proto3 message + `PacketType` enum value)
> - Direction (client→SFU, SFU→client, both)
> - Trigger / lifecycle
> - Forwarding semantics (per-receiver, broadcast, ACL)
> - Backwards compatibility notes (`client_capabilities` gating)

## RoutingHeader (additive on `MediaPacket`)

_TBD — diagram of fields: `is_keyframe`, `temporal_layer_id`, `spatial_layer_id`,
`audio_level`, `is_speaking`, `frame_marker`, `picture_id`. Show how the SFU
reads these without decrypting the payload (ADR-0001)._

## SUBSCRIPTION_UPDATE (PacketType = 10)

_TBD — `SubscriptionUpdate { pinned_sessions, slots, max_video_kbps,
receive_all_audio }`, `VisibilitySlot { session_id, preferred_spatial,
preferred_temporal }`. Direction: client → SFU. Trigger: UI visibility change._

## SPEAKER_UPDATE (PacketType = 11)

_TBD — `SpeakerUpdate { top_speakers, generation }`, `SpeakerEntry { session_id,
score, is_speaking }`. Direction: SFU → all receivers. Trigger: EWMA threshold
crossing on `audio_level`._

## LAYER_HINT (PacketType = 12)

_TBD — per-receiver preferred temporal/spatial layer hint (SVC). Direction:
client → SFU. Trigger: bandwidth estimate change or visibility change._

## ADMISSION_DECISION (PacketType = 13)

_TBD — SFU → client; emitted when a join is accepted/redirected/rejected.
Used by room-affinity routing (ADR-0005)._

## CAPABILITY_ANNOUNCE (PacketType = 14)

_TBD — both directions; carries `client_capabilities` bitfield
(`SFU_ROUTING_HEADER=1`, `SVC=2`, `SUBSCRIPTION=4`). Used to fall back to legacy
fanout for incapable receivers._

## CONNECTION (existing) — `client_capabilities` extension

_TBD — show how `CONNECTION` packet gains the capability bits and how the SFU
caches them per-session._
