# ADR-0001: Routing Header Out of Encryption (SFrame-style)

- **Status:** Proposed (skeleton — to be filled out under bead `vc-c4e.3`)
- **Date:** 2026-05-15
- **Deciders:** TBD
- **Related:** [`PLAN.md` — New Wire Surface](../PLAN.md#new-wire-surface-consolidated), [`packet-diagrams.md`](../packet-diagrams.md)

## Context

The SFU must make per-receiver forwarding decisions (keyframe gating, temporal /
spatial layer dropping, active-speaker selection, congestion-aware drops)
without holding the end-to-end media decryption keys. Today, all routing-
relevant information (keyframe flag, temporal/spatial layer ids, audio level,
speaking hint, frame markers, picture id) is buried inside the encrypted media
payload.

The proposal is to lift these fields into a `RoutingHeader` carried in the
clear on `MediaPacket`, SFrame-style: the SFU reads the header for routing; the
payload remains opaque to the SFU and is decrypted only by participants.

This ADR captures the decision to adopt that split and the security /
operational trade-offs it implies.

## Decision

_TBD — to be filled out under bead `vc-c4e.3`._

## Consequences

_TBD — to be filled out under bead `vc-c4e.3`._
