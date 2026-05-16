# RFC-2: SFU Architecture for videocall-rs

# I Executive Summary

## A. Goal of the System

`videocall-rs` is an open, MIT-licensed video conferencing ecosystem with end-to-end encryption between peers. This RFC proposes turning the current NATS-fanout server into a true Selective Forwarding Unit (SFU) targeted at **webinar-shape meetings of up to 200 participants** (≤10 active video senders, ~190 listeners), while preserving WebTransport (QUIC) as the primary transport and WebSocket as fallback.

## B. Purpose of the Request for Comment (RFC)

Define the umbrella architecture for the SFU refactor on the `experimental-sfu` branch. Each major design choice is captured in a granular ADR under [`/sfu-update/adr/`](../sfu-update/adr/); planning artifacts (capacity model, packet diagrams, test matrix) live alongside under [`/sfu-update/`](../sfu-update/). The full implementation plan is [`/sfu-update/PLAN.md`](../sfu-update/PLAN.md) — this RFC is a curated summary of its Context, Locked Decisions, and Phased Implementation sections.

## C. Overview of the Organization

Griffin Obeid: griffobeid@securityunion.dev — Co-founder and developer

Ronen Barzel: ronen@barzel.org — Core Contributor

Dario Lencina: dario@securityunion.dev — Co-founder and developer

# II. Current System

## A. Description of the existing system

`videocall-rs` is hosted at [https://rustlemania.com](https://rustlemania.com); source at [https://github.com/security-union/videocall-rs](https://github.com/security-union/videocall-rs). The primary connection is WebTransport, with WebSocket as fallback. Everything is in Rust; the WASM UI is built on Dioxus. The backend scales horizontally over NATS pub/sub, and every encrypted media packet is republished to every peer in a room. `PERFORMANCE.md:95-100` already documents an aspirational SFU mode and `ARCHITECTURE.md:308-314` calls out simulcast and tiered quality as future work.

## B. Strengths and limitations of the current system

**Strengths**
- Highly-scalable pub/sub architecture
- Written in Rust end to end
- Uses WebCodecs API for low-latency media
- WebTransport and WebSocket both supported
- End-to-end encrypted media payload
- Open source & MIT licensed

**Limitations**
- The server is a pub/sub fanout, not a real SFU — every packet goes to every peer in the room.
- At 200 participants, blind fanout collapses on outbound bandwidth and on the `mpsc(256)` per-session buffer that today drops packets FIFO.
- Outbound queue is FIFO with silent drop: an audio packet can be discarded behind a burst of video.
- No layer awareness; no notion of active speaker; no per-receiver subscription.
- Only works on Chrome / Chromium-based browsers in practice.

# III. Proposed System

## A. Context and binding constraint

At 200 participants the binding constraint is **outbound fanout**, not ingestion. The plan preserves the project's inbound queuing discipline (bounded + drop) but changes the outbound queue from FIFO-with-silent-drop to **priority-aware-with-class-drop**. E2EE is preserved in evolved form: media payload remains encrypted, but `MediaPacket` gains an unencrypted `RoutingHeader` (SFrame-style) carrying layer ids, keyframe flag, and audio level — exactly enough for the SFU to forward intelligently without ever decrypting media.

Capacity sketch (full numbers in [`capacity-model.md`](../sfu-update/capacity-model.md)):
- Per-pod inbound (room owner): 10 senders × 800 kbps video + 200 audio × 32 kbps = **14.4 Mbps**.
- Per-pod outbound (top-6 video each + all audio): 200 × 8.8 Mbps = **~1.76 Gbps** — the binding constraint, requiring either 2+ pods per room or audio mixdown.
- Breaks at ~250 receivers (egress) or ~30 senders (inbound) per pod. Webinar shape is ~20× easier than conference shape.

## B. Locked Decisions

These came out of the design interview and are now fixed for v1:

1. **Meeting shape:** webinar first (≤10 active video, rest listeners). Hooks for conference shape later.
2. **E2EE posture:** evolve — encrypted payload + clear routing header on `MediaPacket`. See [ADR-0001](../sfu-update/adr/0001-routing-header-out-of-encryption.md).
3. **Selection model:** hybrid — server picks a default active-speaker set; client overrides via pins and visibility slots. See [ADR-0002](../sfu-update/adr/0002-active-speaker-detection.md) and [ADR-0003](../sfu-update/adr/0003-hybrid-subscription-model.md).
4. **Client contract:** coordinated client+server changes allowed; wire protocol can evolve.
5. **Sender encoder:** VP9 SVC via WebCodecs (`scalabilityMode: "L1T3"`), single bitstream with temporal/spatial layers; SFU drops layers per receiver.
6. **Room routing:** hybrid room-affinity; consistent-hash `room_id` → preferred pod; NATS handles spillover and cross-region. See [ADR-0005](../sfu-update/adr/0005-room-affinity-routing.md).
7. **Decision artifacts:** this umbrella RFC plus granular ADRs under [`/sfu-update/adr/`](../sfu-update/adr/); planning notes ([`capacity-model.md`](../sfu-update/capacity-model.md), [`packet-diagrams.md`](../sfu-update/packet-diagrams.md), [`test-matrix.md`](../sfu-update/test-matrix.md)) under [`/sfu-update/`](../sfu-update/).
8. **Inbound queuing:** keep current bounded-with-drop discipline; **change outbound** to a priority queue with class-aware drop. See [ADR-0004](../sfu-update/adr/0004-outbound-priority-queue.md).

Cross-cutting governance decisions, accepted alongside this RFC:
- [ADR-0006: Refinery push contract](../sfu-update/adr/0006-refinery-push-contract.md) — how polecat branches land on `experimental-sfu`.
- [ADR-0007: DAG source of truth](../sfu-update/adr/0007-dag-source-of-truth.md) — how the convoy DAG is authored and materialized.

## C. Phased Implementation Roadmap

The work lands on `experimental-sfu` in six independently-mergeable phases. Each phase has an exit criterion and a corresponding gastown convoy (`P0..P6`) whose beads track the actual work; full bead-level DAGs are in [`PLAN.md`](../sfu-update/PLAN.md).

| Phase | Title | Effort | Exit Criterion |
| ----- | ----- | ------ | -------------- |
| P0 | Decision substrate & feature flag | 0.5–1 day | Both binaries boot with `SFU_MODE=legacy` and `SFU_MODE=sfu`; unit test asserts flag plumbing; RFC and ADR scaffolds landed. |
| P1 | Wire protocol: routing header + new packet types | 1–2 days | Legacy + new clients coexist; new client emits `RoutingHeader`; server logs them; no routing change yet. |
| P2 | SFU forwarder module (pass-through) | 3–5 days | `SFU_MODE=sfu` reaches parity with legacy for 1:1 and 1:N rooms; integration test asserts every sent packet is received. |
| P3 | Active-speaker detection + subscription model | 3–5 days | 12-client demo (6 senders + 6 listeners); listeners receive only the speaker set; pin delivers within one RTT; speaker change propagates within 500ms. |
| P4 | VP9 SVC + per-receiver layer dropping | 4–7 days | Receiver throttled to 500kbps receives base+T0 only; throttle lifts → upgrade to top layer within 2s; no thrash with ±20% RTT noise. |
| P5 | Outbound priority queue with class-aware drop | 2–3 days | Synthetic burst test (10MB video burst into a 1Mbps receiver) — audio loss <0.1% while video loss rises smoothly; no HOL block on audio. |
| P6 | Room-affinity routing + capacity validation | 3–5 days | 200-bot load test against a 2-pod deployment pins the room to one pod; killing the owner causes redirect-to-survivor with <15s downtime. |

### P0 — Decision substrate & feature flag
Add `SFU_MODE` env (`legacy` | `sfu`) to both server binaries. Create `actix-api/src/sfu/mod.rs` and `actix-api/src/sfu/config.rs` as the new module root. `SFU_MODE=sfu` is a no-op shim today; it logs and falls through to legacy paths. Land RFC and ADR scaffolds.

### P1 — Wire protocol
Extend `MediaPacket` with an optional `RoutingHeader` (field 10). Add `PacketType` values: `SUBSCRIPTION_UPDATE`, `SPEAKER_UPDATE`, `LAYER_HINT`, `ADMISSION_DECISION`. New protos: `subscription_packet.proto`, `speaker_update_packet.proto`. Add `client_capabilities` bitmask to `CONNECTION`. Client populates `RoutingHeader` for video (WebCodecs chunk metadata) and audio (RMS pre-encode → `audio_level`). All new proto fields are optional; legacy clients remain compatible.

### P2 — SFU forwarder module (pass-through)
Introduce `actix-api/src/sfu/{forwarder.rs, room_state.rs, subscription.rs, speaker.rs, layer_selector.rs}`. The forwarder is **not** an actor — it's `Arc<RwLock<RoomState>>` consulted from each receiver's NATS callback, to avoid serializing the whole room behind one mailbox. Phase 2 selection logic is pass-through — observable parity with legacy, but the plumbing is in place. The phase also lands the `audio-mixdown-deferred` ADR (filed during P2 per bead `p2-10`; not yet on disk) capturing the disposition of Open Risk #1.

### P3 — Active-speaker detection + subscription model
Per-sender EWMA on `audio_level` (α=0.3) with entry/exit hysteresis (±0.05 over 200ms/800ms); top-N=4 every 200ms tick; generation counter on set change. `SubscriptionUpdate` is declarative — server replaces prior state. Forwarder consults the reconciled AllowSet. UI integration via existing `set_peer_visibility`. See [ADR-0002](../sfu-update/adr/0002-active-speaker-detection.md), [ADR-0003](../sfu-update/adr/0003-hybrid-subscription-model.md).

### P4 — VP9 SVC + per-receiver layer dropping
Client encoder switches to WebCodecs `scalabilityMode: "L1T3"`. Per-receiver layer selector with greedy two-pass scheduling: pass 1 ensures base layer for every allowed sender; pass 2 upgrades by priority while budget remains. Downgrades immediate on `CONGESTION`; upgrades require 20% headroom for ≥3s with a 5s cooldown. Keyframes at `T0/S0` are always forwarded; `REFERENCES_T0` frames are dropped only if their `T0` was also dropped. See [ADR-0001](../sfu-update/adr/0001-routing-header-out-of-encryption.md).

### P5 — Outbound priority queue with class-aware drop
Replace `mpsc::channel::<WtOutbound>(256)` (and the WS analog) with a `PrioritySender` over 5 inner channels:

| Class | Size | Drop policy | Examples |
| ----- | ---- | ----------- | -------- |
| P0 Control | 32 | never drop; log+stop session if full | RTT, heartbeat, SESSION_ASSIGNED, MEETING_*, CONGESTION, SPEAKER_UPDATE |
| P1 Audio | 128 | tail-drop oldest | all AUDIO packets |
| P2 Keyframe + base T0 video | 128 | tail-drop oldest | `is_keyframe=true` and `temporal=0 spatial=0` |
| P3 Video P-frames base spatial | 256 | tail-drop oldest | non-keyframe, `spatial=0` |
| P4 Enhancement + screen | 256 | head-drop oldest | `spatial>0` or `temporal>0 & spatial>0`; screen-share |

Consumer uses strict priority with an 8-packet fairness quantum to prevent starvation. `CongestionTracker` gains `record_drop_with_class` so P2 fires `CONGESTION` after 1 drop and P4 keeps the current 5-drops/1s threshold. See [ADR-0004](../sfu-update/adr/0004-outbound-priority-queue.md).

### P6 — Room-affinity routing + capacity validation
Consistent-hash `room_id` → pod ordinal via `jump_hash`. Migrate Deployment → StatefulSet (stable ordinals). On mismatch, server emits `ADMISSION_DECISION { redirect_to }` and closes; client reconnects. Each pod publishes 5s health beacons on `room.{room}.system`; spillover triggers at `count > 180` or `cpu > 80%`. Cross-region rooms have a "home region" set by first joiner; out-of-region clients redirect (accepting a ~250ms penalty for v1). See [ADR-0005](../sfu-update/adr/0005-room-affinity-routing.md).

## D. Expected outcomes and benefits

- Webinar-shape meetings of up to 200 participants on a single pod, with room for 2+ pod spillover beyond that.
- Audio quality preserved under contention (class-aware drop ensures audio is never starved by video bursts).
- Per-receiver bandwidth adaptation without server-side transcoding (SVC layer dropping in the SFU).
- E2EE posture preserved: media payload stays encrypted; only a small routing header travels in the clear.
- A clean feature flag (`SFU_MODE`) keeps the legacy path bootable throughout the refactor for fast rollback.

## E. Out of scope for v1

- Conference shape (30–50 active video senders) — capacity model breaks; needs publish-side filtering and possibly simulcast in addition to SVC.
- Server-side audio mixdown — incompatible with strict E2EE; needs a "town hall" mode with relaxed crypto.
- AV1 / H.264 alternatives — VP9 SVC only.
- Recording infrastructure — only capability bit and forwarder special-case; no recording bot built.
- Cross-region active-speaker consistency for spilled rooms — owner pod computes; spill pods consume.

## F. Open risks

Tracked in [`PLAN.md`](../sfu-update/PLAN.md) §"Open Risks"; the headline items are:

1. Audio: forward-all vs server mixdown — mixdown breaks E2EE; deferred to a separate "town hall" mode.
2. Cross-region cost — at 30% remote mix, ~$200/hour cross-region bandwidth; v1 pins rooms to home region.
3. Conference-shape upgrade path — capacity model breaks at ~30 senders; hooks present, explicit RFC follows v1.
4. Admission control at 200 — soft cap at 195 + 5-slot waiting room via existing observer mode (wired in P3).
5. Observability — Prometheus counters / gauges / histograms added in P2 via `actix-api/src/metrics.rs`.
6. Recording bots — capability bit `IS_RECORDER` bypasses layer dropping and `max_visible_video` cap.
7. Cross-region speaker detection consistency — owner pod computes; spill pods consume `SpeakerUpdate`.
8. VP9 SVC browser support — Chromium M111+ verified; Safari 18.2 dropped-layer rendering on the test matrix.

# IV. Specific Areas for Comment

## A. Feedback on the overall roadmap

The phased plan front-loads decision substrate (P0) and wire protocol (P1) so that all subsequent phases land on a stable surface. Feedback is welcome on phase ordering, exit criteria, and whether the P0→P6 sequence captures the right risk-reduction curve.

## B. Suggestions for improvements or alternatives to proposed phases

Each ADR has its own "Alternatives Considered" section. Suggestions are most actionable when filed against the specific ADR (e.g. a comment on [ADR-0004](../sfu-update/adr/0004-outbound-priority-queue.md) about the 5-class split versus a different class taxonomy).

## C. Comments on feasibility and implementation

The capacity model (§III.A and [`capacity-model.md`](../sfu-update/capacity-model.md)) sets the binding constraint at ~1.76 Gbps outbound per pod for the 200-participant webinar shape. Feedback on whether this matches operational experience — or on alternate constraints (memory, syscall rate, NATS bandwidth) — is welcome.

## D. Thoughts on integration with existing infrastructure

The SFU module slots into the existing actix-api process; NATS topology and Helm chart skeletons are reused (the Helm change in P6 is a Deployment→StatefulSet migration, not a new chart). The client lands as additive proto fields plus a new `sfu_client.rs` module; legacy clients continue to work via field defaults. The feature flag (`SFU_MODE`) keeps both paths live throughout the refactor.

# V. Feedback Submission

## A. Format and content for feedback

File a PR to this repo with your change proposal — either against this RFC, against a specific ADR under [`/sfu-update/adr/`](../sfu-update/adr/), or against the corresponding planning artifact in [`/sfu-update/`](../sfu-update/). The team will review and can set up a call to go over the changes.

# VI. After the RFC Process

## A. Review and incorporation of feedback

The Security Union team commits to reviewing all the feedback and working with the contributors to advocate for their initiatives. Once an ADR's Status moves from `Proposed` to `Accepted`, the matching phase becomes implementable; convoys are staged and launched per [`PLAN.md`](../sfu-update/PLAN.md) §"Convoy launch protocol".
