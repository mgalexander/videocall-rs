# Capacity Model — SFU Refactor (200-participant webinar)

> **Status:** Filled out under bead `vc-c4e.8`.
>
> **Source of truth:** [`PLAN.md` §J — Capacity Model (200-participant webinar)](./PLAN.md#capacity-model-200-participant-webinar).
>
> This document expands the back-of-envelope numbers in PLAN.md §J into a
> reproducible capacity model: per-pod inbound/outbound, NATS bandwidth, mpsc
> backlog memory, burst behavior, and the breaking points for webinar vs.
> conference shape. It also enumerates the **audio forward-all vs. server
> mixdown** decision matrix that PLAN.md flags as Open Risk #1.

All numbers are upper-bound back-of-envelope. Validation belongs to the
200-bot load test (§8); the model exists so a polecat or reviewer can decide
*before* burning the load-test slot whether a knob change is plausible.

## 1. Inputs

Locked from PLAN.md (Phase 4 layer-selector defaults, Phase 5 priority-queue
class depths, and PLAN.md §J inputs):

| Symbol | Meaning | Webinar (v1 target) | Conference (future) |
|--------|---------|---------------------|---------------------|
| `N_total` | Participants in one room | 200 | 30–50 |
| `N_send_video` | Active video senders | 10 | = `N_total` |
| `N_send_audio` | Audio senders (all unmuted) | `N_total` = 200 | `N_total` |
| `R_audio` | Per-stream audio bitrate (Opus, mono) | 32 kbps | 32 kbps |
| `R_video_in` | Per-sender ingress video (camera or screen) | 800 kbps | 800 kbps |
| `R_video_out` | Per-receiver forwarded video tier (post-layer-drop) | 400 kbps | 400 kbps |
| `K_visible` | Max visible video tiles per receiver (`SubscriptionUpdate.slots`) | 6 | 6 |
| `chunk_size` | Avg WebCodecs encoded chunk on the wire | 1500 B | 1500 B |
| `KF_size` | Keyframe payload (period ~2 s on T0) | 1.5 MB | 1.5 MB |

`R_video_out = 400 kbps` is the SVC-dropped tier the layer-selector targets
for the non-active set (PLAN.md Phase 4). Under v1's L1T3 default, dropping
is temporal-only — the selector forwards T0 (and T1 if budget allows) of the
800 kbps stream, averaging ~400 kbps over a 2 s window. Under future L3T3_KEY
the selector picks the base spatial + T0/T1 to hit the same ~400 kbps
envelope. The active speaker may receive the full 800 kbps stream; that adds
at most one extra tier per receiver and is folded into the egress headroom
below.

## 2. Per-Pod Inbound

One pod owns the room and ingests every sender exactly once (the room-affinity
model — see [ADR-0005](adr/0005-room-affinity-routing.md)).

```
inbound = N_send_video × R_video_in  +  N_send_audio × R_audio
        = 10 × 800 kbps               +  200 × 32 kbps
        = 8.0 Mbps                    +  6.4 Mbps
        = 14.4 Mbps
```

Notes:

- **All audio is always ingested**, even from non-speakers. Active-speaker
  detection runs server-side on `RoutingHeader.audio_level` after ingest, so
  the SFU cannot skip ingest based on speaker state.
- Inbound queuing discipline is unchanged from today (PLAN.md decision #8).
  The 14.4 Mbps figure is well within a single tokio task's read budget and
  is **not** the binding constraint.

## 3. Per-Pod Outbound (binding constraint)

Each of the 200 receivers gets a per-receiver mix from the forwarder:

```
per_receiver = K_visible × R_video_out  +  N_send_audio × R_audio
             = 6 × 400 kbps              +  200 × 32 kbps
             = 2.4 Mbps                  +  6.4 Mbps
             = 8.8 Mbps
```

```
egress_total = N_total × per_receiver
             = 200 × 8.8 Mbps
             = 1.76 Gbps
```

**This is the binding constraint.** A single 1 Gbps pod cannot sustain the
webinar shape under forward-all audio without one of the mitigations in §4.

Audio dominates: `200 × 32 kbps × 200 = 1.28 Gbps` of per-receiver fanout is
audio. The video forwarding contribution is only `200 × 2.4 Mbps = 480 Mbps`.
This is why the audio-forward-all vs. mixdown decision (§9) is the highest-
leverage knob in the entire model.

## 4. Mitigations

Two viable paths to bring `egress_total` under a 1 Gbps pod NIC:

### 4a. Multi-pod fanout (v1 default)

Spill the room across N pods (PLAN.md §F-2 / ADR-0005). Each spill pod
subscribes to the same NATS subjects, runs its own forwarder, and serves a
slice of receivers. Receivers are pinned by consistent-hash to one of the
pods.

- Pods needed for webinar: `ceil(1.76 / 0.8)` ≈ **3 pods @ 800 Mbps headroom**.
- Preserves E2EE — no payload decryption anywhere.
- Cost: NATS fanout amplifies (§5). Speaker-detection consistency requires
  one owner pod publishing `SpeakerUpdate` on `room.{room}.system`
  (Open Risk #7).

### 4b. Audio mixdown (deferred, breaks E2EE)

Server decodes all 200 audio streams, mixes to one stream at 48 kbps, and
forwards that single stream to every receiver:

```
per_receiver_mixdown = 6 × 400 kbps + 1 × 48 kbps  ≈ 2.45 Mbps
egress_mixdown_total = 200 × 2.45 Mbps             ≈ 490 Mbps
```

Fits a single pod comfortably, but **the server must hold cleartext audio**.
Incompatible with strict E2EE. Deferred to a "town hall" mode under a
future `adr/00NN-audio-mixdown.md` (owned by bead `p2-10` in PLAN.md's
Convoy P2 table); not in v1. See §9 for the full decision matrix.

## 5. NATS Bandwidth

NATS publish-side is unchanged from today: `session_logic.rs` publishes each
inbound media packet to `room.{room}.{session}`. Subscribers are the
forwarders on every pod that owns at least one receiver in that room.

| Topology | NATS publish (one room) | NATS server fanout | Net NATS load |
|----------|-------------------------|--------------------|----------------|
| Single-pod room (≤195 participants) | 14.4 Mbps | 14.4 Mbps (1 subscriber pod) | ~30 Mbps |
| Spillover, 2 pods | 14.4 Mbps | 28.8 Mbps | ~43 Mbps |
| Spillover, 3 pods (webinar @ forward-all) | 14.4 Mbps | 43.2 Mbps | ~58 Mbps |

NATS is **not** the binding constraint at 200 participants — even 10 such
rooms in parallel are well under a single `nats-server` instance's published
~10 Gbps/core throughput. Operationally relevant only as a cost signal in
cross-region deployments (PLAN.md Open Risk #2: ~$200/hr cross-region at 30%
remote mix).

System subjects (`room.{room}.system` for `SpeakerUpdate`, health beacons,
`SubscriptionUpdate`) carry control traffic only — measured in tens of
kbps per room; ignored in egress totals.

## 6. mpsc Backlog Memory

The Phase 5 outbound priority queue replaces the single `mpsc(256)` with 5
class queues (ADR-0004):

| Class | Depth | Per-slot avg | Per-session bytes |
|-------|-------|--------------|-------------------|
| P0 Control | 32 | 256 B | 8 KB |
| P1 Audio | 128 | 200 B (Opus 20 ms frame) | 25 KB |
| P2 Keyframe + base T0 video | 128 | 1500 B | 192 KB |
| P3 Video P-frames base spatial | 256 | 1500 B | 384 KB |
| P4 Enhancement + screen | 256 | 1500 B | 384 KB |
| **Per-session total (worst case)** | **800** | — | **≈ 1.0 MB** |

```
backlog_room = per_session × N_total
             = 1.0 MB × 200
             = 200 MB
```

At three-pod spillover, each pod backs ~70 receivers ⇒ ~70 MB per pod.
Fine on the 8 GB node target. The figure is a **worst case** — typical
steady-state occupancy is single-digit slots per class; backlog only
saturates during burst recovery (§7).

## 7. Burst Behavior

The pathological case is a 1.5 MB keyframe arriving at a single receiver
whose downlink is below the burst rate.

```
KF_chunks = KF_size / chunk_size
          = 1_500_000 / 1500
          = 1000–1250 chunks  (allowing for framing overhead)
```

P2 depth = 128 slots ⇒ the keyframe overflows P2 by ~10×. Tail-drop is
*within the keyframe* — receiver decodes a corrupt frame, emits
`KEYFRAME_REQUEST`, and `packet_handler.rs:115`'s rate-limit gates the
re-request. Expected recovery ≈ **one RTT + one keyframe interval ≈ 500 ms**.

Audio is unaffected because:

- P1 (audio) sits above P2 in the strict-priority schedule.
- The fairness quantum (8 packets/class before peeking lower) bounds
  P0/P1 starvation, not P2 starvation.
- Worst-case audio scheduling latency = `P0 depth × wire time per packet`
  ≈ 4 ms (PLAN.md Phase 5 paragraph).

This is acceptable for webinar shape: at most one keyframe per active
sender per ~2 s, ~10 senders ⇒ ~5 keyframe events/sec at room scale, well
within the slow-receiver budget. **It is not acceptable for conference
shape**: see §8.

## 8. Breaking Points

The two independent failure axes:

| Axis | Limit per pod | Driving formula | Webinar (10 v, 200 a) | Conference (50 v, 50 a) |
|------|--------------|-----------------|----------------------|--------------------------|
| Egress | ~250 receivers @ forward-all audio | `egress = N_recv × (K_visible × R_video_out + N_send_audio × R_audio)` | 200 → **at edge** (1.76 Gbps; needs §4 mitigation) | 50 × (2.4 + 1.6) = 200 Mbps (comfortable) |
| Inbound | ~30 senders @ 800 kbps | `inbound = N_send_video × R_video_in + N_send_audio × R_audio` | 10 → 14.4 Mbps (10× headroom) | 50 → 41.6 Mbps (still fine on NIC, but keyframe-burst rate goes 5×) |
| Keyframe burst | ~10 senders before P2 saturation overlaps | `KF_rate × KF_chunks > schedule_budget` | 10 senders × 0.5 Hz = 5 events/s (handled) | 50 senders × 0.5 Hz = 25 events/s (P2 tail-drop becomes steady-state, not burst) |

**Plain English.** Webinar shape is egress-bound and we ride the edge of
the binding constraint (`§3` = `1.76 Gbps`). Multi-pod fanout (§4a) is the
v1 answer. Conference shape is *not* harder on the NIC — it is harder on the
**P2 keyframe queue**: at 25 keyframes/s the burst recovery model breaks
because the queue never drains between bursts. That is the real reason
the same SFU code path does not cover conference shape without follow-up
work (PLAN.md Open Risk #3, deferred).

The 250 / 30 / 50 numbers should be treated as **soft cliffs**, not
contract values: the load test in §8a is what we ship against.

## 9. Decision Matrix — Audio: Forward-All vs. Server Mixdown (Open Risk #1)

PLAN.md §J locks v1 on **forward-all audio + multi-pod fanout**. This
matrix captures the trade-off so the deferred audio-mixdown ADR (owned by
bead `p2-10` per PLAN.md's Convoy P2 table) has a place to land:

| Dimension | Forward-all (v1 default) | Server mixdown (deferred "town hall" mode) |
|-----------|--------------------------|---------------------------------------------|
| Per-receiver audio egress | `N_send_audio × R_audio` = 6.4 Mbps | `1 × 48 kbps` = 48 kbps |
| Total egress @ 200 receivers | 1.76 Gbps (binding) | ~490 Mbps (single pod) |
| Pods needed | 3 (with headroom) | 1 |
| **E2EE posture** | **Preserved** — server never sees plaintext audio | **Broken** — server must decode + mix |
| Active-speaker UX | Client can render full grid + per-speaker meter | Single mixed stream — no per-speaker UI without ducking metadata |
| Speaker-detection input | `RoutingHeader.audio_level` (in clear, server reads) | Server already has audio amplitude — speaker detection is "free" |
| Failure mode | One slow receiver gets per-class drops; others unaffected | Mixer hiccup is heard by everyone; jitter buffer is centralised |
| Cross-region cost | Audio dominates fanout — expensive at 30% remote mix | Audio collapses to one stream — ~13× cheaper |
| Codec churn | Opus end-to-end, unchanged | Server must transcode (or re-encode after mix) |
| Recording-bot impact | Bot ingests N audio streams (matches forward-all path) | Bot ingests one mixed stream — simpler but less flexible |
| Privacy / compliance | Strong: server is a cipher relay | Weak: server holds cleartext audio in memory |
| Migration cost | Zero new server crypto; existing `RoutingHeader` extension covers it | New "town hall" key escrow + relaxed-crypto policy |

**Decision rule for the deferred ADR.** Server mixdown is allowed only when
**all** of the following hold:

1. Meeting is marked `town_hall` (explicit shape flag, not a default).
2. All participants consent to the relaxed-crypto posture at join time.
3. The recording-bot pipeline is configured to ingest the mixed track.
4. The deployment is *not* in a region where relaxed-crypto would violate
   policy (US-EDU / EU-public-sector / etc. — out of scope for v1).

For v1 (`webinar` shape), Forward-All wins on all the dimensions that
matter (E2EE + per-speaker UX + uniform failure mode). The 3× pod cost is
the price of preserving the project's core invariant.

## 10. Validation Plan

The numbers above are paper. The load-test confirms them.

| Gate | Setup | Pass criterion | Source |
|------|-------|----------------|--------|
| 50-bot smoke (merge gate) | `bot/` × 50, 5 min, single pod | <0.5% audio loss; per-receiver egress within ±20% of model | PLAN.md §6 |
| 200-bot release gate | `bot/` × 200, webinar shape, 3-pod spillover, 10 min | <0.5% audio loss; `sfu_dropped_*` histogram dominated by P4 class; no P0/P1 drops | PLAN.md §7 |
| Slow-receiver pathological | 1 throttled receiver @ 1 Mbps + 9 healthy senders | Audio loss <0.1% on the slow receiver; healthy receivers unaffected | PLAN.md §6 |
| Spillover redirect | Kill owner pod mid-test | Surviving pod accepts redirected joiners within 15 s | PLAN.md §7 |
| Keyframe burst | 10 MB synthetic video burst into 1 Mbps receiver | Audio loss <0.1% during burst; KEYFRAME_REQUEST observed; recovery <1 s | PLAN.md §6 |

Metrics to capture for each run (PLAN.md Open Risk #5):

- `sfu_forwarded_total{class}` — sanity-check per-class shares vs. §6 model.
- `sfu_dropped_total{class,reason}` — must show P0/P1 = 0.
- `sfu_room_size`, `sfu_speaker_changes_per_min` — shape sanity.
- `sfu_decide_latency_us` histogram — must remain under 200 μs p99.
- Egress bytes/sec per pod (Prometheus on the host NIC) — must match
  `egress_total / pods` from §3+§4 within ±15%.

If a measured number diverges from the model by >25%, the model is wrong,
not the code — update this doc before tuning thresholds.
