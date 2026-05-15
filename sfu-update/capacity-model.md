# Capacity Model — SFU Refactor (200-participant webinar)

> **Status:** Skeleton. To be filled out under bead `vc-c4e.8`.
>
> **Source of truth:** [`PLAN.md` §J — Capacity Model (200-participant webinar)](./PLAN.md#capacity-model-200-participant-webinar).
>
> This document expands the back-of-envelope numbers in PLAN.md §J into a
> reproducible capacity model: per-pod inbound/outbound, the binding constraint
> (egress), mpsc backlog memory, burst behavior, and the breaking points for
> webinar vs. conference shapes.

## 1. Inputs

_TBD — sender count, receiver count, per-track bitrates, top-N video selection,
audio mixdown assumptions._

## 2. Per-Pod Inbound

_TBD — cite PLAN.md §J (10 senders × 800 kbps video + 200 audio × 32 kbps ≈ 14.4 Mbps)._

## 3. Per-Pod Outbound (binding constraint)

_TBD — cite PLAN.md §J (per-receiver 8.8 Mbps; total 1.76 Gbps across 200 receivers)._

## 4. Mitigations

_TBD — multi-pod fanout vs. audio mixdown trade-off (latter breaks E2EE; see
PLAN.md Open Risk #1)._

## 5. mpsc Backlog Memory

_TBD — 5-class priority queue × ~256 slots × 1500B per session × N sessions._

## 6. Burst Behavior

_TBD — keyframe burst sizing vs. P2 queue depth; KEYFRAME_REQUEST recovery._

## 7. Breaking Points

_TBD — egress-bound (~250 receivers/pod) vs. inbound-bound (~30 senders/pod);
webinar shape vs. conference shape._

## 8. Validation Plan

_TBD — load-test methodology using `bot/` headless client; metrics to capture._
