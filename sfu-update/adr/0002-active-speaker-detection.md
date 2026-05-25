# ADR-0002: Active Speaker Detection (EWMA on audio_level)

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** overseer (malexander)
- **Related:** [ADR-0001](0001-routing-header-out-of-encryption.md) (the `RoutingHeader.audio_level` / `is_speaking` fields this ADR consumes), [ADR-0003](0003-hybrid-subscription-model.md) (the AllowSet that consumes this ADR's output), [ADR-0005](0005-room-affinity-routing.md) (only the owner pod runs the scorer), [`PLAN.md` Phase 3](../PLAN.md#phase-3--active-speaker-detection--subscription-model-35-days), [`PLAN.md` New Wire Surface](../PLAN.md#new-wire-surface-consolidated), [`PLAN.md` Open Risk #7](../PLAN.md#open-risks-escalate-before-each-phase), [`packet-diagrams.md`](../packet-diagrams.md), [`rfc-2-sfu-architecture.md`](../../rfc/rfc-2-sfu-architecture.md), bead `vc-c4e.4`.

## Context

A 200-participant webinar cannot forward every sender's video to every receiver. Of the room's notional 10 active video senders, only a handful are interesting at any given moment — the current speaker, a few recently-active speakers held warm to absorb turn-taking, and whatever the receiver has explicitly pinned. The SFU's job is to compute, continuously and cheaply, the small set of sessions whose video is worth pushing to a given receiver by default. That set is what we call the **active-speaker set**, and it is the dominant driver of bandwidth shape in the [capacity model](../capacity-model.md) — the difference between forwarding 10 video streams per receiver and forwarding 4 is the difference between a workable webinar shape and one that melts the egress pipe.

[ADR-0001](0001-routing-header-out-of-encryption.md) settled the upstream question of how the server *sees* audio activity without payload access: every AUDIO `MediaPacket` now carries `RoutingHeader.audio_level` (pre-Opus RMS in `[0, 1]`) and `RoutingHeader.is_speaking` (the sender-local VAD/threshold bit previously only present on `HeartbeatMetadata`). The forwarder reads these on every inbound audio packet. The remaining question — the one this ADR answers — is *what to do* with the per-packet signal: how to debounce it into a stable speaker set, how often to recompute, how to avoid UI thrash, and how to publish the result to receivers and to spill-pod consumers.

The naive answer ("pick the loudest sender right now") fails for three reasons. First, raw audio level is noisy: a single high-energy frame from a key tap, mic bump, or breath spike would yank the speaker set every 20 ms. Second, natural speech contains many brief sub-200ms gaps that would cause a speaker to "drop out" of the set and immediately rejoin, producing visible UI churn at the receiver as tiles reorder and video streams stop and start. Third, in cross-region rooms where the scorer must run on a single authoritative pod ([ADR-0005](0005-room-affinity-routing.md)) and propagate to spill pods over NATS, a high update rate would multiply the cross-region message volume per [Open Risk #7](../PLAN.md#open-risks-escalate-before-each-phase). The detector design must be cheap per packet, smooth over noise, hysteretic on set membership, and parsimonious on publish.

The closest prior art is WebRTC's [RFC 6464](https://datatracker.ietf.org/doc/rfc6464/) (Client-to-Mixer Audio Level Indication via RTP header extension) which standardises both the wire format for per-packet audio levels and the typical SFU usage pattern (server-side aggregation, periodic dominant-speaker selection). RFC 6464's design conclusion is the one this ADR adopts in protocol-neutral form: the level lives on every packet, in the clear, outside the encrypted payload, and a smoothing/hysteresis pass on the server produces a stable selection. We are not implementing RFC 6464 itself — we already chose to carry the level in `RoutingHeader` rather than as an RTP header extension ([ADR-0001](0001-routing-header-out-of-encryption.md)) — but the signal-processing approach is borrowed.

## Decision

**The server-side speaker scorer maintains a per-sender exponentially-weighted moving average over `RoutingHeader.audio_level`, recomputes the top-N=4 speaker set on a 200ms tick with entry/exit hysteresis, and publishes `SpeakerUpdate` to `room.{room}.system` only when set membership changes.** The owner pod for a room is the sole scorer; spill pods consume `SpeakerUpdate` as receivers and do not run their own detector.

Concretely:

1. **Detector — EWMA per sender.** For each session `s` in the room, the scorer maintains `score[s] : f32`, updated on every inbound AUDIO `MediaPacket` for that sender as:

   ```
   score[s] ← α · audio_level + (1 − α) · score[s]
   ```

   with `α = 0.3`. Senders that have not produced an audio packet within a sliding 1s window decay toward zero on the next tick (`score[s] ← (1 − α) · score[s]`) so that a sender who falls silent leaves the speaker set on the same timescale as a quiet active sender. `α = 0.3` was chosen as a defensible default in the range used by similar implementations (Jitsi's dominant-speaker uses a comparable smoothing constant on a comparable signal); it is exposed as a tunable in `SfuConfig` (see Implementation) so we can sweep it under load without a code change. Lower `α` favours stability; higher `α` favours responsiveness. The 200ms tick and 0.05 entry/exit deltas (below) were tuned together with `α = 0.3` and should be adjusted as a set, not individually.

2. **Speaking gate.** A sender is *eligible* for the top-N set only if **both** of:
   - `score[s] > 0.05` — i.e., smoothed RMS energy above the noise floor, *and*
   - the most recent AUDIO packet from `s` with `RoutingHeader.is_speaking = true` arrived within the last 400 ms.

   The first clause uses the smoothed energy; the second clause uses the sender-local VAD/threshold bit ([ADR-0001](0001-routing-header-out-of-encryption.md) §3 audio) as a fast tie-breaker that distinguishes speech from sustained non-speech energy (typing, HVAC, a barking dog). Together they reject both kinds of false positives: continuous low-level noise (fails the energy gate) and brief high-energy transients (fails the EWMA's smoothing, and the VAD bit will likely be `false` for a key tap). Senders failing either clause have `is_speaking = false` reported in their `SpeakerEntry` if they remain in the published set; senders failing for an extended window are removed from the set by the exit hysteresis below.

3. **Top-N selection on a 200ms tick.** A periodic task runs every 200 ms (configurable, default `tick = Duration::from_millis(200)`). On each tick the scorer sorts eligible senders by `score[s]` descending, takes the top-N=4 (`max_speakers = 4`), and compares to the previously-published set. The 200ms cadence sets the worst-case detection latency for a *new* speaker who just started talking at `~200 ms + EWMA warm-up`; an `α = 0.3` EWMA reaches ~70% of a step input in roughly three samples, so for a sender producing audio packets at 50 packets/sec (Opus 20ms frame) the warm-up is ~60 ms, dominated by the tick.

4. **Hysteresis — slow exit, fast entry.** Naively reapplying top-N each tick would cause set churn whenever two senders' scores cross. Instead:
   - **Entry:** a candidate not currently in the set joins iff its score has exceeded the current N-th member's score by `+0.05` *and* has done so on every sample of a 200 ms entry window (i.e., the next tick after the candidate has unambiguously beaten the incumbent).
   - **Exit:** an incumbent leaves the set iff its score has fallen below the best non-member's score by `-0.05` *and* has done so for an 800 ms window — four consecutive ticks.

   The asymmetry (exit slower than entry, 800 ms vs 200 ms) is deliberate. We want speakers to *join* the set quickly so a new speaker's video appears with minimal latency, but to *leave* slowly so brief speech pauses (breaths, sentence boundaries) don't cause their tile to disappear and reappear. 800 ms is roughly the upper end of natural inter-utterance pauses in conversational speech; entries beyond that are unambiguously turn-taking, not pausing.

5. **Generation counter.** The scorer maintains a monotonic `uint64 generation` counter, incremented on every set-membership change (entry, exit, or reordering of `is_speaking` flags within the set). The published `SpeakerUpdate.generation` reflects the value at the moment of publish. Receivers and spill pods MUST treat `SpeakerUpdate` messages with `generation <= last_seen_generation` as duplicates and discard them. This makes client-side handling idempotent under NATS at-least-once delivery and trivially resolves out-of-order delivery across spill pods.

6. **Publish on change only, not on tick.** `SpeakerUpdate` is published to `room.{room}.system` (the existing room-wide control subject) exclusively when the scorer's set-membership computation produces a different result from the previously-published one. A steady-state room with a single dominant speaker generates *zero* `SpeakerUpdate` messages between speaker changes — only the per-packet EWMA accounting runs each tick, and that is in-process. This bounds publish rate to roughly one message per real speaker transition, which in normal webinar discourse is sub-Hz.

7. **Wire format.** As specified in [`PLAN.md` New Wire Surface](../PLAN.md#new-wire-surface-consolidated):

   ```protobuf
   message SpeakerUpdate {
     repeated SpeakerEntry top_speakers = 1;   // ordered by score descending
     uint64 generation = 2;
   }
   message SpeakerEntry {
     uint64 session_id = 1;
     float  score = 2;
     bool   is_speaking = 3;
   }
   ```

   `PacketType = SPEAKER_UPDATE = 11`. The `score` field is the EWMA at publish time, exposed so clients can render activity-level UI affordances (the dot-meter beside a speaker tile, etc.) without re-deriving it. `is_speaking` is the per-entry version of the VAD gate from §2 above — true iff the speaking-gate is currently passing for that sender. The score field is informational, not load-bearing: clients MUST sort by the wire order of `top_speakers`, not by `score`, to avoid divergence between server selection and client display.

8. **Cross-region authority.** Per [ADR-0005](0005-room-affinity-routing.md), a room has exactly one owner pod determined by consistent hash. The scorer runs only on the owner pod. Spill pods (those serving overflow joiners for the same room — see [`PLAN.md` Phase 6](../PLAN.md#phase-6--room-affinity-routing--capacity-validation-35-days)) subscribe to `room.{room}.system` as ordinary receivers and forward `SpeakerUpdate` to their local clients without recomputation. This addresses [Open Risk #7](../PLAN.md#open-risks-escalate-before-each-phase). The cost is a small NATS-fanout latency (typically <20 ms within a region; up to 250 ms cross-region per [Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase)) between owner-pod detection and spill-pod-client display — accepted for v1.

9. **Downstream contract.** The speaker set is one input to the per-receiver AllowSet computed by [ADR-0003](0003-hybrid-subscription-model.md). The reconciliation rule there is `forward_set(receiver) = pinned ∪ default_speaker_set ∪ slot_sessions`, capped at `max_visible_video = 6`. A receiver who has pinned someone outside the speaker set still sees them; a receiver with no pins sees exactly the speaker set. This ADR does not own the AllowSet semantics — it owns only the `default_speaker_set` term.

10. **Implementation locus.** `actix-api/src/sfu/speaker.rs`, stubbed in Phase 2 (bead `p2-1`) and filled in Phase 3 (beads `p3-1`, `p3-2`, `p3-3` — see [`PLAN.md` Convoy P3](../PLAN.md#convoy-p3--active-speaker--subscription-model)). The scorer holds `Arc<RwLock<RoomState>>` from [ADR-0003](0003-hybrid-subscription-model.md)'s room model; per-sender `score` lives on the room's member table, not in a separate map, so scorer ticks and forwarder lookups share a cache line.

## Consequences

**Pro:**

- **Cheap per packet, cheap per tick.** The hot path is one float multiply-add per inbound audio packet, plus a sort-of-N=4 over <30 active speakers every 200 ms. There is no allocation in steady state. This fits inside the per-room read-lock window the forwarder already takes.
- **No payload access required.** The scorer reads only `RoutingHeader.audio_level` and `RoutingHeader.is_speaking`, both of which travel in the clear by [ADR-0001](0001-routing-header-out-of-encryption.md). The decision is fully compatible with E2EE: the server never decrypts audio to make a speaker selection.
- **Hysteresis kills UI thrash.** The 200 ms entry + 800 ms exit windows have been deliberately tuned against natural conversational speech patterns. A speaker who pauses mid-sentence keeps their tile; a speaker who clears their throat for half a second doesn't briefly hijack the layout.
- **Idempotent receiver handling.** The generation counter lets receivers dedupe `SpeakerUpdate` messages trivially, and lets spill-pod consumers tolerate NATS reordering without bespoke logic. Client code becomes `if msg.generation > self.last_speaker_gen { apply(msg); self.last_speaker_gen = msg.generation }`.
- **Publish rate is bounded by real-world conversation cadence.** Because we publish only on set-membership change (not on every tick), a quiet room costs zero `SpeakerUpdate` traffic and a normal conversational room costs roughly one publish per turn-take — a fraction of a Hz. The cross-region message volume hinted at in [Open Risk #7](../PLAN.md#open-risks-escalate-before-each-phase) is correspondingly bounded.
- **Trivially observable and tunable.** Exposing `α`, `tick`, `max_speakers`, entry/exit deltas, and exit window as `SfuConfig` fields lets operators sweep parameters against the bot harness ([`PLAN.md` Phase 6](../PLAN.md#phase-6--room-affinity-routing--capacity-validation-35-days)) without a redeploy. The 200ms histogram of `sfu_decide_latency_us` from [`PLAN.md` Open Risk #5](../PLAN.md#open-risks-escalate-before-each-phase) covers the scorer-inclusive forwarder path.

**Con:**

- **Detection latency floor ≈ 200 ms.** A brand-new speaker who has been silent and now starts talking will not enter the published set until the next tick at the earliest, plus the 200ms entry window confirmation, plus the EWMA warm-up. In the worst case (just-missed a tick boundary) this is closer to ~400 ms. For webinar shape this is well inside the perceptual window for "the right person's video is on screen", but for ultra-low-latency conversation use cases it would be visible. The conference-shape variant ([`PLAN.md` Out of Scope](../PLAN.md#out-of-scope-for-v1)) would likely need a shorter tick and looser hysteresis.
- **Very brief utterances may not register.** A 100ms "uh-huh" or "mm-hmm" will not move the EWMA enough to clear the entry threshold against an active speaker. The speaker set will continue to show the previous speaker until a more substantial utterance occurs. This is generally desirable in moderated webinars (acks are not turn-takes), but worth noting.
- **Trust in sender-reported levels.** A malicious sender could lie about `audio_level` and try to capture a speaker slot while silent. The EWMA smoothing means a single spoofed packet cannot dominate — they would need to sustain the lie at the room's audio packet rate (50 Hz) for the warm-up period and then continue sustaining to stay in the set. The mitigation is exactly the one stated in [ADR-0001 §Sender can lie](0001-routing-header-out-of-encryption.md): the speaker selection is *advisory*; a receiver who notices that the "speaker" has no audible audio (no decryptable Opus frames or only silence frames) can downgrade them client-side and surface a diagnostic. The server does not police this.
- **Fixed N = 4 is shape-specific.** Four works for webinar shape (≤10 senders, audience mostly listens). It will be wrong for conference shape (everyone potentially speaks), for "town hall" shape (one panellist + many askers), and for very small rooms (N=4 in a 3-person room is a no-op). The parameter is tunable per-room in `SfuConfig` but the v1 default targets webinar only; varying it per-room dynamically is [out of scope for v1](../PLAN.md#out-of-scope-for-v1).
- **Cross-region detection-display skew.** When a room has spilled to multiple pods, a receiver on a spill pod sees a `SpeakerUpdate` ~NATS-fanout-latency after a receiver on the owner pod sees it. Same-region this is negligible (<20 ms). Cross-region (per [Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase), 250 ms ceiling) it is visible: a receiver in EU sees the speaker change a quarter-second after a US-co-located receiver. Acceptable for v1; documented.
- **The 200 ms tick adds a periodic timer per active room.** At 1000 simultaneous rooms (well beyond v1 targets) this is 5000 timer fires per second. Tokio handles this trivially, but it is non-zero work that grows linearly with active-room count, not with participant count.
- **`SpeakerUpdate` is in the P0 Control class.** Per [ADR-0004](0004-outbound-priority-queue.md) it sits with RTT/heartbeat/SESSION_ASSIGNED in the never-drop queue. A pathologically-flapping speaker selection in a misbehaving room would consume P0 budget. The hysteresis design is what prevents this; if it ever fails we have an immediate observability signal in `sfu_speaker_changes_per_min` ([`PLAN.md` Open Risk #5](../PLAN.md#open-risks-escalate-before-each-phase)) and a circuit-breaker would be a follow-on ADR.

**Mitigations / things this ADR explicitly does NOT do:**

- Does not run a learned VAD or ML model on the server — see Rejected alternative D.
- Does not redistribute the scorer to spill pods — single authority on owner pod, by design (Rejected alternative F).
- Does not vary N adaptively. Constant N=4 is the v1 contract; conference shape will revisit.
- Does not attempt to detect simultaneous speakers ("two people talking at once"). The selection is rank-based on smoothed energy; both will appear in the top-N if both clear the gate, but there is no special "collision" UI signal.
- Does not weight by historical speaker time (e.g., to give floor time to under-speakers). The detector is reactive, not policy-driven.

## Implementation

- [ ] `actix-api/src/sfu/speaker.rs` — `SpeakerScorer` struct with per-sender EWMA (`α = 0.3`), `update(session_id, RoutingHeader)` called from the forwarder's inbound path (`p3-1` / `vc-c4e.4`-child).
- [ ] `actix-api/src/sfu/speaker.rs` — 200ms periodic `tick()` driving top-N=4 selection with entry/exit hysteresis (+0.05 / -0.05 over 200ms / 800ms windows) and monotonic `generation` counter (`p3-2`).
- [ ] `actix-api/src/sfu/speaker.rs` — `publish_speaker_update` emitter to `room.{room}.system` on set-membership change only (`p3-3`).
- [ ] `actix-api/src/sfu/config.rs` — expose `SpeakerScorerConfig { alpha, tick, max_speakers, entry_delta, exit_delta, entry_window, exit_window, speaking_floor, vad_recency }` so the parameters above are configurable without code changes.
- [ ] `actix-api/src/metrics.rs` — `sfu_speaker_changes_per_min` gauge, `sfu_speaker_active_count` gauge, `sfu_speaker_publish_total` counter (per [`PLAN.md` Open Risk #5](../PLAN.md#open-risks-escalate-before-each-phase)).
- [ ] `videocall-client/src/decode/peer_decode_manager.rs` — consume inbound `SPEAKER_UPDATE`, dedupe by `generation`, drive speaker-tile UI (`p3-6`).
- [ ] `actix-api/src/sfu/tests/speaker_tests.rs` — unit tests for EWMA convergence, hysteresis (no thrash on bouncing input), generation monotonicity under concurrent updates, exit-on-silence decay (`p3-9`).
- [ ] `e2e/tests/sfu-speaker-rotation.spec.ts` — 12-client rotation demo asserts speaker change visible within 500 ms (`p3-12`).

Phase 2 (`p2-1`) lands the empty `speaker.rs` module behind `SFU_MODE`; Phase 3 (`p3-1`, `p3-2`, `p3-3`) fills it in. The downstream consumer in [ADR-0003](0003-hybrid-subscription-model.md)'s subscription reconciliation (beads `p3-4`, `p3-5`) is what makes the speaker set bind on the receiver side; without that, this ADR is observable-only.

## Rejected alternatives

**Alternative A — Per-packet instantaneous top-N (no smoothing).** Recompute the speaker set on every inbound audio packet, sorted by raw `audio_level`. **Rejected** because every key tap, breath, mic-bump, or cross-talk burst would yank the set, causing receiver-side video tiles to reorder and video subscriptions to thrash at ~50 Hz. The downstream cost (KEYFRAME_REQUEST storms, NATS publish load) would dominate. Smoothing is the entire point of the EWMA.

**Alternative B — RTP-style ssrc-audio-level header extension ([RFC 6464](https://datatracker.ietf.org/doc/rfc6464/)).** Adopt the WebRTC-standard wire format for per-packet audio levels and reuse the standard server-side aggregation patterns. **Rejected** because we don't speak RTP — we speak protobuf-wrapped media packets, and per [ADR-0001](0001-routing-header-out-of-encryption.md) the level already travels in `RoutingHeader.audio_level`. Reproducing RFC 6464 inside the protobuf envelope would add an indirection without changing the signal. The aggregation approach here (EWMA + hysteresis + generation counter) is functionally equivalent to what an RFC-6464-conformant SFU would do internally; we acknowledge RFC 6464 as the closest prior art and borrow its design conclusions without its wire format.

**Alternative C — Client-side speaker selection (each receiver scores its own inbound audio).** Have every client run an EWMA over inbound audio levels and pick its own top-N. **Rejected** because the entire purpose of the SFU is to *avoid* sending every audio stream to every receiver. Client-side scoring requires receiving every sender's audio to score them, which is the precise bandwidth blowup the [capacity model](../capacity-model.md) shows is fatal at 200 participants. Server-side scoring is a precondition for "forward only the speaker set's video." If we kept forwarding all audio to all clients we *could* additionally let clients re-score, but at that point the server's selection is the authoritative one anyway and client-side recomputation is redundant.

**Alternative D — Server-side ML/VAD model (WebRTC VAD, Silero, etc.).** Run a learned voice-activity detector on the server, either on the audio payload or on derived spectral features. **Rejected** for two reasons. First, payload access is incompatible with the E2EE posture set by [ADR-0001](0001-routing-header-out-of-encryption.md) — see that ADR's Rejected alternative A. Second, the `is_speaking` VAD bit is *already* available pre-encryption on the sender side, computed by hardware or by a sender-local VAD with full audio access; promoting it to a routing-header bit (already decided in [ADR-0001](0001-routing-header-out-of-encryption.md)) gives us the VAD signal for free. The server has no information advantage over the sender on this question.

**Alternative E — Aggressive smoothing (lower `α`, e.g., 0.1).** Use a much smoother EWMA to further reduce sensitivity. **Rejected** as a default because it pushes detection latency for new speakers above the perceptual threshold (~500 ms before the EWMA crosses the gate against a quiet competitor). `α = 0.3` is a deliberate compromise; we expose it as a tunable so we can sweep it under bot load and revisit. Conversely, `α = 0.5+` is rejected because it negates the smoothing benefit relative to alternative A.

**Alternative F — Distributed scorers (each pod runs its own scorer over the audio it sees).** In a spilled room, let each pod independently compute its local speaker set. **Rejected** because pods don't see the same audio in a spill scenario — the room owner sees senders directly, spill pods see the NATS rebroadcast. The two would converge on the same answer eventually, but with skew, and the inconsistency would be visible to receivers on different pods. Single-authority owner-pod scoring with `SpeakerUpdate` fanout is strictly simpler and provides bounded-skew consistency by construction.

**Alternative G — Receive-all-audio relaxed to top-N audio too.** Apply the speaker set to audio forwarding as well, not just video. **Rejected for v1** because audio bandwidth is small (32 kbps × 200 = 6.4 Mbps per receiver, easily affordable) and because cutting audio to non-top-N speakers would break "I can hear someone interrupt before they're promoted to the speaker set" — a quality conversational behaviour. Audio stays room-wide ([`PLAN.md` New Wire Surface](../PLAN.md#new-wire-surface-consolidated): `SubscriptionUpdate.receive_all_audio = true` is the v1 default); the speaker set affects video only.

## Status

**Accepted** 2026-05-17. Detector parameters (`α = 0.3`, tick = 200 ms, N = 4, entry/exit ±0.05 over 200 ms / 800 ms windows, speaking floor = 0.05, VAD recency = 400 ms) are the v1 defaults and are tunable via `SfuConfig` without an ADR change. Algorithmic changes (different detector family, distributed scorer, ML/VAD on server, applying the set to audio forwarding) require a new ADR. Supersedes nothing. Superseded by: none.
