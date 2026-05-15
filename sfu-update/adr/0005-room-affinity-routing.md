# ADR-0005: Room-Affinity Routing (hybrid)

- **Status:** Proposed (skeleton — to be filled out under bead `vc-c4e.7`)
- **Date:** 2026-05-15
- **Deciders:** TBD
- **Related:** [`PLAN.md` — Phase 6](../PLAN.md#phase-6--room-affinity-routing--capacity-validation-35-days), [`PLAN.md` — Capacity Model](../PLAN.md#capacity-model-200-participant-webinar)

## Context

The SFU forwards only within a single pod. For a meeting to fan out across
pods (egress is the binding constraint — see [`capacity-model.md`](../capacity-model.md))
all participants of a room must land on the same pod, or pods must relay to
each other. v1 chooses the former: **room affinity**.

Approaches considered:

1. **Pure consistent hashing** on `room_id` — deterministic, but cross-region
   clients pay a fixed RTT penalty (~250 ms) and capacity is room-size-bound.
2. **Pure dynamic placement** — best load balance, complex coordination, hard
   to keep stable across rolling deploys.
3. **Hybrid** (proposed): each region's StatefulSet hashes locally on
   `room_id` to pick a home pod; the first joiner's region sets the room's
   home region; out-of-region joiners are redirected via
   `ADMISSION_DECISION` (`PacketType = 13`).

This ADR captures the hashing scheme, the home-region election, the redirect
protocol, and the failover behavior when the home pod dies mid-meeting.

## Decision

_TBD — to be filled out under bead `vc-c4e.7`._

## Consequences

_TBD — to be filled out under bead `vc-c4e.7`._
