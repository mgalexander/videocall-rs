# ADR-0009: Audio Mixdown Deferred (Open Risk #1)

- **Status:** Accepted (deferred). v1 SFU does not implement audio mixdown.
- **Date:** 2026-05-17
- **Deciders:** overseer (malexander)
- **Related:** [ADR-0001](0001-routing-header-out-of-encryption.md) (E2EE posture — server reads the unencrypted routing header but never the encrypted payload), [ADR-0002](0002-active-speaker-detection.md) (speaker scoring runs server-side on `RoutingHeader.audio_level`, not on decoded audio), [ADR-0004](0004-outbound-priority-queue.md) (audio rides priority class P1, separately from video P2/P3/P4), [ADR-0005](0005-room-affinity-routing.md) (per-pod soft cap is sized for forward-all audio), [`PLAN.md` Open Risk #1](../PLAN.md#open-risks-escalate-before-each-phase), [`PLAN.md` Capacity Model](../PLAN.md#capacity-model-200-participant-webinar), [`capacity-model.md` §4a](../capacity-model.md#4a-multi-pod-fanout-v1-default), [`capacity-model.md` §4b](../capacity-model.md#4b-audio-mixdown-deferred-breaks-e2ee), [`capacity-model.md` §9](../capacity-model.md#9-decision-matrix--audio-forward-all-vs-server-mixdown-open-risk-1), bead `vc-68d` (`p2-10`).

> **ADR numbering note.** [`PLAN.md`'s Convoy P2 table](../PLAN.md#open-risks-escalate-before-each-phase) and [`capacity-model.md` §4b](../capacity-model.md#4b-audio-mixdown-deferred-breaks-e2ee) both reference this decision as "ADR-0006". By the time this ADR landed, the 0006 slot was already taken by [ADR-0006 Refinery Push Contract](0006-refinery-push-contract.md) and 0007/0008 by the polecat/cluster ADRs. The decision is the one the plan called out; only the file number drifted. No PLAN.md updates are made here per the bead's non-scope.

## Context

The 200-participant webinar capacity model ([`capacity-model.md` §4a](../capacity-model.md#4a-multi-pod-fanout-v1-default), reflected in [`PLAN.md`'s Capacity Model section](../PLAN.md#capacity-model-200-participant-webinar)) shows that per-pod outbound is the binding constraint at the v1 target shape:

```
per_receiver = K_visible × R_video_out  +  N_send_audio × R_audio
             = 6 × 400 kbps             +  200 × 32 kbps
             = 2.4 Mbps                 +  6.4 Mbps              =  8.8 Mbps
egress_total = 200 × 8.8 Mbps                                    =  1.76 Gbps
```

Audio is `200 × 32 kbps = 6.4 Mbps` per receiver — `6.4 / 8.8 ≈ 73 %` of the outbound mix, before any video layer dropping kicks in. At 200 receivers that is `200 × 6.4 Mbps = 1.28 Gbps` of audio fanout per room. Video-only (`200 × 2.4 Mbps = 480 Mbps`) would fit one pod with headroom; it is the audio plane that forces the multi-pod arithmetic in [ADR-0005](0005-room-affinity-routing.md) (`ceil(1.76 / 0.8) ≈ 3` pods at 800 Mbps each).

A **server-side audio mixdown** — decode all 200 inbound audio streams, mix to a single 48 kbps stream, forward the one mixed stream to every receiver — would collapse the audio plane from `N_send_audio × R_audio` to `1 × R_mix`:

```
per_receiver_mixdown = 6 × 400 kbps + 1 × 48 kbps  ≈ 2.45 Mbps
egress_mixdown_total = 200 × 2.45 Mbps             ≈ 490 Mbps
```

That is the 200×-on-audio (1.28 Gbps → 6.4 Mbps room-wide audio fanout) and `~3.6×`-on-total (1.76 Gbps → 490 Mbps) reduction the capacity model calls out. It would fit one pod comfortably and would also collapse the cross-region audio bill ([`PLAN.md` Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase)) by the same factor.

**The cost is the project's core security invariant.** Mixing requires the SFU to decrypt every inbound audio packet so it can combine PCM samples. That makes the server a member of the E2EE group for audio — exactly the property [ADR-0001](0001-routing-header-out-of-encryption.md) was written to preserve. The routing header is unencrypted by design (it carries `audio_level`, `is_speaking`, layer ids) but the *payload* stays end-to-end encrypted; that is what lets the README's "No SFUs" / E2EE marketing claim survive the SFU refactor in evolved form. Mixdown breaks that.

The webinar shape — `≤10` active video senders, `~190` listeners, [`PLAN.md` Capacity Model](../PLAN.md#capacity-model-200-participant-webinar) — also makes audio fanout less painful in practice than the headline 1.28 Gbps figure suggests: most participants are listeners, and most of the 200 audio streams carry near-silence. Speaker detection ([ADR-0002](0002-active-speaker-detection.md)) operates on the unencrypted `RoutingHeader.audio_level` precisely so the SFU can reason about who is speaking without holding plaintext audio.

This ADR records the v1 decision so the rationale does not get lost the next time someone reopens the capacity arithmetic and notices that audio is the largest term in the egress equation.

## Decision

**For v1, audio is FORWARDED unmixed. Each receiver gets all subscribed senders' encrypted audio packets. The SFU never decrypts media.**

A future "town hall" mode — with an explicit relaxed-E2EE shape flag, possibly negotiated using the existing `client_capabilities` bitmask from [ADR-0001](0001-routing-header-out-of-encryption.md) §4 (the [`p1-3`](../PLAN.md#convoy-p1--wire-protocol--routing-header) capability-bit pattern) — may add server mixdown, but it is **out of scope for v1**. The decision rule for that future ADR is already enumerated in [`capacity-model.md` §9](../capacity-model.md#9-decision-matrix--audio-forward-all-vs-server-mixdown-open-risk-1): explicit `town_hall` room flag, participant consent at join time, recording-bot pipeline configured for the mixed track, and deployment region permits relaxed crypto.

## Consequences

**Pro:**

- **E2EE preserved end-to-end.** No payload decryption on the server. The README's "No SFUs" / E2EE invariant survives the SFU refactor in its evolved form ([ADR-0001](0001-routing-header-out-of-encryption.md)): metadata in the clear, content opaque.
- **Forwarder logic stays simple.** No per-room audio mixer state, no codec on the server, no PCM buffer management, no transcode pipeline. The forwarder is a packet relay with priority-class gates; the priority queue ([ADR-0004](0004-outbound-priority-queue.md)) already gives audio its own P1 class for scheduling.
- **Uniform failure model.** One slow receiver gets per-class drops on its own connection; others are unaffected. A mixer hiccup would be heard by *everyone* and would centralise the jitter buffer; forward-all avoids that single point of failure.
- **Per-speaker UX is free.** Clients receive each speaker's audio separately and can render per-speaker meters, ducking, and individual mute / volume controls without server cooperation. A mixed stream would force any per-speaker UI to be reconstructed from out-of-band metadata.
- **Layer-aware video forwarding (Phase 4) plus room affinity (Phase 6) is sufficient.** The 1.76 Gbps egress total is met by [ADR-0005](0005-room-affinity-routing.md)'s 3-pod-per-room sizing at 800 Mbps each. The capacity model balances on this; nothing else in v1 needs mixdown to close.
- **Recording bots stay in the same code path.** A recording bot receives the same per-speaker streams as a human participant ([`capacity-model.md` §9](../capacity-model.md#9-decision-matrix--audio-forward-all-vs-server-mixdown-open-risk-1)). No special-case ingest pipeline.

**Con:**

- **Audio outbound bandwidth scales linearly with audio fanout.** `N_subscribed_audio × 32 kbps` per receiver. For the v1 webinar shape (`N_subscribed_audio = 200`) this is the 6.4 Mbps per receiver figure that drives the multi-pod sizing. The priority-queue classes ([ADR-0004](0004-outbound-priority-queue.md)) keep it manageable up to `~200` participants per pod; beyond that, room-affinity routing ([ADR-0005](0005-room-affinity-routing.md)) splits the load across pods within a region.
- **Conference shape (30+ simultaneous active speakers) remains out of scope.** That is the binding-constraint case where forward-all audio genuinely does not scale on a single pod, and where mixdown's 200×-on-audio reduction would matter most. It is the body of work behind [`PLAN.md` Open Risk #3](../PLAN.md#open-risks-escalate-before-each-phase) and is deferred to v2 with its own RFC.
- **Cross-region audio bill is not collapsed.** [`PLAN.md` Open Risk #2](../PLAN.md#open-risks-escalate-before-each-phase) calls out `~$200/hr` cross-region bandwidth at a 30 % remote mix. The audio plane is the dominant term in that figure, and forward-all keeps it dominant. v1 mitigates by pinning rooms to a home region ([ADR-0005](0005-room-affinity-routing.md) §7), not by collapsing the audio plane.
- **Decision is locked to v1 only.** Adopting a `town_hall` mode later means an explicit crypto-relaxation policy, capability negotiation, participant consent UX, and a recording-bot pipeline that ingests a mixed track. None of that is built. This ADR records *that* it is deferred, not *how* it will eventually land.

**Mitigations / things this ADR explicitly does NOT do (per the bead's non-scope):**

- Does **not** change any code.
- Does **not** update [`PLAN.md`](../PLAN.md) or [`capacity-model.md`](../capacity-model.md). Both already describe forward-all as the v1 default and reference this decision; the cross-references here are pointers, not edits.
- Does **not** specify the eventual mixdown design. Codec choice (single Opus re-encode vs. PCM mix + re-encode), mixer placement (owner pod vs. dedicated mixer pod), key-escrow scheme for the relaxed-crypto group, and the `town_hall` consent UX are all left for the future ADR.
- Does **not** propose a new capability bit. The `client_capabilities` pattern ([ADR-0001](0001-routing-header-out-of-encryption.md) §4, bead [`p1-3`](../PLAN.md#convoy-p1--wire-protocol--routing-header)) is *the obvious mechanism* for negotiating a future `TOWN_HALL_MODE` capability, but the bit is not reserved here. It will be allocated when the town-hall ADR lands.

## Status

**Accepted (deferred)** 2026-05-17. v1 SFU does not implement audio mixdown. The forward-all audio plane is the v1 default; multi-pod fanout per [ADR-0005](0005-room-affinity-routing.md) is the v1 capacity story. A future ADR may introduce a `town_hall` room shape with relaxed E2EE and server-side mixdown; the decision rule for that ADR is enumerated in [`capacity-model.md` §9](../capacity-model.md#9-decision-matrix--audio-forward-all-vs-server-mixdown-open-risk-1). Supersedes nothing. Superseded by: none.
