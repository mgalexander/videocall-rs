# ADR-0002: Active Speaker Detection (EWMA on audio_level)

- **Status:** Proposed (skeleton — to be filled out under bead `vc-c4e.4`)
- **Date:** 2026-05-15
- **Deciders:** TBD
- **Related:** [`PLAN.md` — New Wire Surface](../PLAN.md#new-wire-surface-consolidated), ADR-0003

## Context

A 200-participant webinar cannot forward every sender's video to every
receiver. The SFU must pick a "top-N speakers" set and forward those video
streams; the rest are either suppressed or downgraded. To do this without
decrypting audio, the SFU uses the `audio_level` (RMS, 0..1) and `is_speaking`
hint carried in the `RoutingHeader` (see ADR-0001).

The proposal is to compute an EWMA over per-sender `audio_level` samples,
threshold the EWMA to declare "speaking", and emit `SPEAKER_UPDATE`
(`PacketType = 11`) whenever the top-N set changes. Receivers consume the
update to drive UI ordering and subscription hints.

This ADR captures the choice of detector (EWMA vs. alternatives), the
parameters (window, threshold, hysteresis, update generation), and how the
output interacts with the subscription model (ADR-0003).

## Decision

_TBD — to be filled out under bead `vc-c4e.4`._

## Consequences

_TBD — to be filled out under bead `vc-c4e.4`._
