# ADR-0003: Hybrid Subscription Model

- **Status:** Proposed (skeleton — to be filled out under bead `vc-c4e.5`)
- **Date:** 2026-05-15
- **Deciders:** TBD
- **Related:** [`PLAN.md` — New Wire Surface](../PLAN.md#new-wire-surface-consolidated), ADR-0001, ADR-0002

## Context

In a 200-participant webinar, receivers cannot decode every sender's video.
The SFU needs to know which streams each receiver actually wants. Two
extremes:

1. **SFU-driven**: the SFU picks top-N by active-speaker score and forwards
   that set uniformly to all receivers. Simple; ignores UI state (pinning,
   visible tiles, screen share focus).
2. **Client-driven**: each receiver sends an explicit subscription set. Honours
   UI state perfectly; costs round-trips on every visibility change and bloats
   control traffic.

The proposal is a **hybrid**: clients emit `SUBSCRIPTION_UPDATE`
(`PacketType = 10`) declaring pinned sessions, visibility slots with preferred
spatial/temporal layers, and an `max_video_kbps` budget; the SFU fills any
remaining slots from `SPEAKER_UPDATE` (ADR-0002) and applies layer dropping
under congestion.

This ADR captures the split of responsibilities, the wire schema, and the
fallback when `client_capabilities` advertises no `SUBSCRIPTION` support.

## Decision

_TBD — to be filled out under bead `vc-c4e.5`._

## Consequences

_TBD — to be filled out under bead `vc-c4e.5`._
