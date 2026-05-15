# ADR-0004: Outbound Priority Queue (class-aware drop)

- **Status:** Proposed (skeleton — to be filled out under bead `vc-c4e.6`)
- **Date:** 2026-05-15
- **Deciders:** TBD
- **Related:** [`PLAN.md` — Phase 5](../PLAN.md#phase-5--outbound-priority-queue-with-class-aware-drop-23-days), ADR-0001

## Context

The current per-session outbound path is a single `mpsc(256)` channel
(`actix-api/src/webtransport/mod.rs:351`). Under burst (e.g. a 1.5 MB keyframe
≈ 1250 chunks) the queue tail-drops indiscriminately, sometimes evicting audio
or control packets behind video chunks and triggering a `KEYFRAME_REQUEST`
recovery cycle (~500 ms).

The proposal is a **5-class priority queue** keyed off `RoutingHeader` /
`PacketType`: control, audio, video-base-layer, video-enhancement, and
diagnostics. Each class has its own depth and drop policy; high-priority
classes preempt lower ones. The SFU emits `CONGESTION` with per-class
thresholds so clients can step down before the queue saturates.

This ADR captures the class taxonomy, per-class depths, drop policy
(tail-drop vs. head-drop), and how it interacts with the existing
`CongestionTracker` in `session_logic.rs`.

## Decision

_TBD — to be filled out under bead `vc-c4e.6`._

## Consequences

_TBD — to be filled out under bead `vc-c4e.6`._
