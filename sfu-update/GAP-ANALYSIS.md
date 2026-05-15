# SFU Refactor — Gap Analysis & Adversarial Security Audit

**Scope.** Audit of `sfu-update/PLAN.md` (commit `183fc53`) and the surrounding plan-mode artifacts, against (a) the actual `mgalexander/videocall-rs` codebase at HEAD `c01a773` and (b) an adversarial threat model assuming a motivated peer/observer/operator attacker. Performed 2026-05-15, before any execution beyond P0 Wave 1 (vc-c4e.1).

**Method.** Three passes:
1. **Internal consistency** — read PLAN.md end-to-end, then re-read each cross-reference (DAG ↔ wire surface ↔ phasing ↔ capacity model). Flag contradictions, magic-number mismatches, and decisions referenced but never made.
2. **Codebase reality** — spot-check load-bearing claims in PLAN.md against actual files (`actix-api/src/actors/packet_handler.rs`, `token_validator.rs`, `session_manager.rs`, `helm/`, `bot/`). Flag claims that won't survive contact.
3. **Adversarial threat model** — assume an attacker who can be (a) a peer in the meeting with a valid JWT, (b) a passive eavesdropper on the wire, (c) a future compromised SFU pod, (d) a hostile recording bot, (e) a malicious operator with rig-level access. For each new wire-format element and forwarding decision, ask: *what's the worst thing I can do with this?*

**Priority levels.** Concerns are filed P0–P3 plus C (consistency, non-security). The "owning phase" column says **when** the concern must be addressed — typically the phase that introduces the attack surface. **Do not** wait until end-of-plan; each item should land as part of (or immediately after) the phase that creates it.

| Pri | Meaning | Gate |
| --- | --- | --- |
| **P0** | Exploitable from day-one of the phase. Blocks merge of the phase that introduces the surface. | Hard gate. |
| **P1** | Realistic attack; mitigations must land in the same phase before the phase closes. | Phase-close gate. |
| **P2** | Defence-in-depth, multi-step exploits, or known-trustworthy assumptions to revisit. | Pre-release / pre-public-launch. |
| **P3** | Long-tail hardening, observability, supply-chain. | Bug-bash backlog. |
| **C** | Inconsistency / clarification only — no immediate security impact. | Resolve before the affected phase starts. |

For each issue: **ID** (G-1, G-2, … for consistency; S-P0-1, S-P1-1, … for security), **summary**, **PLAN.md anchor**, **owning phase**, **suggested mitigation**, **suggested bead** (key to add to `convoy-manifest.yaml`).

---

## 1. Internal Consistency (C-class)

### C-1. Capacity model assumes "200 audio senders" but plan is webinar-shape

> **PLAN.md anchor:** "Capacity Model (200-participant webinar)" — *"10 senders × 800 kbps video + 200 audio × 32 kbps"*.

The locked decision (#1) is webinar-first: ≤10 active video + ~190 listeners. Listeners are typically muted. Yet the capacity model bills audio for 200 senders. Either (a) the meeting allows every listener to be a continuous audio sender (which is a different shape — closer to "open mesh" than webinar), or (b) the capacity model is computing a worst case that's larger than the design target. The forwarding logic and admission control should reflect whichever is true.

- **Owning phase:** P0 (clarify before P3 active-speaker work).
- **Mitigation:** Decide explicitly: webinar = listeners are audio-muted by default (server enforces; client can request to be unmuted via `MeetingPacket` / admission). Re-do the capacity numbers with the actual expected audio-sender count (say 30–50 transients, not 200). Update `sfu-update/capacity-model.md` once written.
- **Suggested bead:** `gap-c1-clarify-audio-sender-shape` (chore, blocks p0-8 capacity-model task).

### C-2. VP9 SVC `scalabilityMode: "L1T3"` conflicts with the layer-selection algorithm

> **PLAN.md anchor:** Phase 4 *"WebCodecs `scalabilityMode: \"L1T3\"`"* vs. layer_selector pseudocode that picks `(spatial, temporal)` and references `spatial_layer_id` in the routing header.

`L1T3` = **1 spatial layer**, 3 temporal layers. There is no spatial dimension to select on. The pseudocode and ADR-0004 both speak as if spatial selection is in scope from Phase 4. If you really want spatial drop, you need `L3T3_KEY` (3 spatial × 3 temporal) — which is heavier on the sender and not universally supported.

- **Owning phase:** P4.
- **Mitigation:** Pick one: (a) Start with `L1T3` for P4, drop the spatial-selection code path entirely (the routing header's `spatial_layer_id` stays unused — keep the field for forward compat but document it as "always 0 for now"), or (b) Start with `L3T3_KEY` and accept the sender CPU cost. (a) is the lower-risk first cut.
- **Suggested bead:** `gap-c2-decide-spatial-vs-temporal-only` (decision; blocks p4-1, p4-5).

### C-3. CongestionTracker class-awareness ordering

> **PLAN.md anchor:** Phase 4 KFR routing references "drop signal from `CongestionTracker`" and per-receiver bandwidth estimate; Phase 5 introduces `record_drop_with_class`. Phase 4 lands before Phase 5.

Phase 4's keyframe-aware logic ("don't blast a 1.5Mbps keyframe to a 200kbps receiver") wants to know whether a drop was on the **keyframe class** specifically. But the priority queue + per-class CongestionTracker doesn't ship until Phase 5. So Phase 4 either (a) ships against an older CongestionTracker that only sees session-id, or (b) silently depends on Phase 5 work.

- **Owning phase:** P4 (sequence shift) **or** P5 (move first).
- **Mitigation:** Move `record_drop_with_class` and the class-aware threshold work into early Phase 4 (or split: a minimal `class` enum lands in P4; full PrioritySender in P5). Update the DAG so p4-7 (forwarder layer-drop) blocks p4-10 (KFR routing) blocks p5-6 (CongestionTracker class).
- **Suggested bead:** `gap-c3-resequence-congestion-class` (chore; rewrites DAG edges).

### C-4. ADR-0006 audio-mixdown numbered before the decisions it depends on

> **PLAN.md anchor:** ADR-0006 is materialised inside Phase 2 (p2-10), but the audio-forward-all-vs-mixdown choice has direct implications on P3 (subscription model) and P5 (priority queue audio class size).

P3's `SubscriptionUpdate.receive_all_audio = true` default and P5's P1 audio queue size both assume forward-all. If the eventual ADR says "mixdown for >100 participants", those values need to change. The ADR landing inside P2 is fine for ordering, but its absence-of-decision should not block P3/P5 design.

- **Owning phase:** P2.
- **Mitigation:** Annotate p3-* and p5-* beads with "assumes ADR-0006 = forward-all". If ADR-0006 flips to "mixdown", reopen those beads.
- **Suggested bead:** none — annotation in `convoy-manifest.yaml` summary fields.

### C-5. JWT validation and `room_join` claim not mentioned in PLAN.md

> **PLAN.md anchor:** absent. Codebase reality: `actix-api/src/token_validator.rs:138-189` already validates room JWTs (HMAC-SHA256 signature, `exp`, `iss`, `room_join: true`, plus a NATS-safe regex on `room`).

The plan never references existing auth. A reader could mistakenly think auth is unsolved or up-for-design. It's neither. Also, the plan introduces multiple new packet types (`SubscriptionUpdate`, `SpeakerUpdate`, …) without saying what role-bearing tokens may produce them.

- **Owning phase:** P0 (documentation), P1 (when wire surface lands).
- **Mitigation:** Add a "Trust model & auth" section to PLAN.md citing `token_validator.rs:138-189`. Specify which new packets are observer-allowed vs `room_join`-only. **Important:** the existing `observer: true` flag (see `token_validator.rs:177-184`) is a defense-in-depth: observers connect but don't participate in media exchange — the SFU should reject `SubscriptionUpdate` from observer sessions (they have nothing to subscribe *for*).
- **Suggested bead:** `gap-c5-trust-model-section` (task; blocks p0-2 RFC).

### C-6. Plan doesn't anchor `max_visible_video = 6` to any UI reality

> **PLAN.md anchor:** §F *"Cap by `max_visible_video = 6` for webinar shape (the UI can render at most 6 tiles meaningfully)"*.

Where does 6 come from? Current videocall-client UI may render more or fewer tiles. If the UI renders 12 and the server caps at 6, half the tiles are dark. If the UI renders 4 and the server caps at 6, the extra two video streams are wasted bandwidth.

- **Owning phase:** P3.
- **Mitigation:** Either (a) measure the actual current/intended max-tiles in `videocall-client` and use that, or (b) make it a `SubscriptionUpdate` field (`max_video_slots`) the client supplies. (b) is more flexible.
- **Suggested bead:** `gap-c6-make-video-slot-cap-client-driven` (feature; modify the SubscriptionUpdate proto).

### C-7. Forwarder lock-contention claim is unsupported

> **PLAN.md anchor:** §D *"Forwarder is **not** an Actix actor — it's `Arc<RwLock<RoomState>>` accessed from each receiver's NATS-callback task. This avoids serializing the entire room behind one mailbox (which would cap throughput at ~50k msgs/s on a single actor)."*

The "50k msgs/s" number is asserted with no source. More importantly, a single `RwLock<RoomState>` at 200 receivers each consulting it on every inbound packet (8 audio + ~50 video = ~12k reads/sec aggregate) plus 1Hz writes from the speaker tick + occasional writes from subscription updates is fine for read-mostly access **only if** the lock is short-held. If `decide()` does any non-trivial work while holding the read lock, contention will grow. The plan doesn't bound the lock-hold time.

- **Owning phase:** P2.
- **Mitigation:** Make `Forwarder::decide` lock-acquire, snapshot the minimal needed state (~32 bytes), release, then run the actual decision on the snapshot. Bench under simulated load before P3.
- **Suggested bead:** `gap-c7-bound-forwarder-lock-time` (task; part of p2-3).

### C-8. Refinery push contract assumes upstream isn't a working clone

> **PLAN.md anchor:** absent; discovered during bootstrap.

The plan never specifies how the Refinery's merged result reaches `/mnt/llms/videocall` (the user's working clone, which is also the rig's upstream `file:///mnt/llms/videocall`). In practice the Refinery wants to push back to the upstream, but Git refuses pushes to a non-bare repo's checked-out branch by default.

- **Owning phase:** P0.
- **Mitigation:** Pick one explicitly and document in `SCALE-UP.md`: (a) `git config receive.denyCurrentBranch updateInstead` in `/mnt/llms/videocall` and keep working tree clean before each merge; (b) configure convoys with `--merge=local` so Refinery never pushes back, user fetches from `rig` remote manually; (c) point the rig's upstream to a separate bare repo (`/mnt/llms/videocall.git`) used only as an exchange surface. Lean (b) — best matches the "local-only, manually approved" contract.
- **Suggested bead:** `gap-c8-define-refinery-push-contract` (task).

### C-9. `bot/` harness scaling claim is unverified

> **PLAN.md anchor:** §K *"`bot/` is the right driver — it's already Rust + WebTransport + can run headless without WebCodecs. Extend `bot/src/main.rs` with: `--room R --senders 10 --listeners 190 --duration 300s`."*

The `bot/` directory exists but its scaling profile is asserted without measurement. Running 200 bots on one machine = 200 WebTransport sessions, 200 QUIC connection states, audio/video synthesis loops. That likely needs a multi-host harness, not single-machine.

- **Owning phase:** P6.
- **Mitigation:** First-run benchmark: how many bots fit on one host? Plan a multi-host harness (e.g., k3s job with N replicas) if the answer is <200. Cite real numbers in `test-matrix.md`.
- **Suggested bead:** `gap-c9-benchmark-bot-host-density` (task; precedes p6-10).

### C-10. PLAN.md and convoy-manifest.yaml drift risk

> **PLAN.md anchor:** "Convoy launch protocol" *"If a phase's DAG changes mid-execution, re-run `gt convoy stage`."* The PLAN.md and `convoy-manifest.yaml` separately describe the DAG.

Two sources of truth for the DAG, with no enforced consistency. Drift between PLAN.md prose and `convoy-manifest.yaml` will land silently — polecats execute from the manifest, humans review against PLAN.md.

- **Owning phase:** P0.
- **Mitigation:** Pick one as the canonical source; the other is generated/derived. Recommend YAML as canonical (machine-parsable), and a generator that emits the prose section of PLAN.md from it. Or at minimum: add a CI check that fails if the bead-counts/keys disagree.
- **Suggested bead:** `gap-c10-single-source-of-dag-truth` (task; tooling).

### C-11. ADR-0001 description glosses over AAD

> **PLAN.md anchor:** ADR-0001 description (in `convoy-manifest.yaml`) just says "SFrame-style." Real SFrame is a [draft RFC with a specific AAD construction](https://datatracker.ietf.org/doc/draft-ietf-sframe-enc/). The plan doesn't say which fields are AAD'd and which are mutable-in-flight.

If the routing header is in AAD, the SFU cannot rewrite layer ids (e.g., to renumber temporal layers). If the routing header is NOT in AAD, attackers can swap headers (see S-P0-1 below). This is a load-bearing cryptographic decision and ADR-0001 must address it explicitly.

- **Owning phase:** P0 (when ADR-0001 is authored as p0-3).
- **Mitigation:** ADR-0001 must specify: AAD = `session_id | sequence | media_type` (immutable, server can't rewrite). Routing header (`is_keyframe`, `temporal_layer_id`, `spatial_layer_id`, `audio_level`, etc.) is OUTSIDE AAD — server reads, possibly clamps, but does not forge. Specifically: server must NEVER produce a forged RoutingHeader for a sender's data (only the original sender signs/encrypts).
- **Suggested bead:** none — ADR-0001 itself addresses it (raise the bar on the ADR template).

### C-12. Phasing mixes "scaffold" with "lock down"

> **PLAN.md anchor:** Phase 1 lands new proto fields *as optional with proto3 defaults*. Phase 2's forwarder consumes them.

Optional proto fields are great for compat but mean Phase 2's forwarder has to defensively handle "header missing or partially populated." The plan describes this scenario implicitly ("capability_announce" bitmask gates header use) but never bottoms out: what does the forwarder do when an SFU-capable client sends a `MediaPacket` with `routing` absent? Treat as opaque, drop, or assume defaults? Phase 2 needs to decide.

- **Owning phase:** P2.
- **Mitigation:** Document the precedence: if `capability_announce.SFU_ROUTING_HEADER == 0`, forwarder uses legacy path for that session. If `== 1` and header absent on a `MEDIA` packet, log + treat as `temporal=0 spatial=0 is_keyframe=false`. After 5 such occurrences on the same session, drop the connection (signals client bug).
- **Suggested bead:** `gap-c12-document-header-absent-policy` (chore; part of p2-3).

---

## 2. Security findings — adversarial

### P0 (block-the-phase, fix in-phase)

#### S-P0-1. Routing header is forge-able by any peer, breaking SFU selection

> **PLAN.md anchor:** §C "New Wire Surface" — `RoutingHeader { is_keyframe, temporal_layer_id, spatial_layer_id, audio_level, is_speaking, frame_marker, picture_id }`.

**Attack.** A peer with a valid JWT sends every audio packet with `audio_level = 1.0, is_speaking = true`. The server's speaker scorer (P3) sees them as the loudest in the room, pegs them at the top of the speaker set, and forwards their video to all 200 receivers — independent of whether they're actually speaking. They get the spotlight 100% of the time and starve real speakers. Cost to attacker: 1 audio session.

**Variant.** Same attacker sets `is_keyframe = true` on every video frame. The forwarder's keyframe-always-forward invariant (§G) means the server forwards every one of their frames, even to receivers whose budget should drop them. Bandwidth amplification × N receivers.

**Why this isn't already mitigated.** Existing `packet_handler.rs:60-72` drops client-originated **CONGESTION** packets (with a great comment about the analogous attack). The plan introduces a *new* trustworthy-looking field — `audio_level` — that the server uses for routing decisions, without saying the field is untrusted. By default, code that consumes a field trusts it.

**Mitigation (must land with P1 or P3, no later).**
- Server-side bounds + sanity: clamp `audio_level` to [0, 1], reject `is_speaking = true` when `audio_level < 0.05`, ignore `is_keyframe` for sender-claimed flag — the server should detect keyframes from the encrypted-payload preamble or from VP9 chunk metadata the sender includes elsewhere. The keyframe flag in the header is for *parsing assist*, not for *trust*.
- Rate-limit speaker-set rank changes per session: a session cannot enter the top-N more than once per 5 seconds.
- Server computes an **EWMA-of-EWMA**: tracking how much the claimed `audio_level` correlates with the *observed bitrate variance* of the sender's audio stream (real speech has bursts; constant-1.0 doesn't). Diverge → flag the session, suppress its `audio_level`.
- Add server-side metric `sfu_routing_header_anomaly_total{sid,kind}` and alert above threshold.
- **Owning phase:** P1 (header lands) for parse-clamp; P3 (speaker logic lands) for rank-limiter; P4 (layer drop lands) for `is_keyframe` trust shift.
- **Suggested beads:** `s-p0-1a-clamp-routing-header` (feature, P1), `s-p0-1b-speaker-rank-rate-limit` (feature, P3), `s-p0-1c-keyframe-trust-server-side` (feature, P4).

#### S-P0-2. New PacketTypes have no origin discipline

> **PLAN.md anchor:** §C — `SUBSCRIPTION_UPDATE`, `SPEAKER_UPDATE`, `LAYER_HINT`, `ADMISSION_DECISION`, `CAPABILITY_ANNOUNCE`.

**Attack.** A client crafts an `ADMISSION_DECISION { redirect_to: "evil.example:443" }` packet, addressed by a chosen victim's `session_id`, and sends it through the WebTransport stream. The forwarder, having no origin discipline on this new packet type, fans it out to the victim. Victim's client (which is coded to honor server `ADMISSION_DECISION`) reconnects to `evil.example`. Phishing meeting in one shot.

Same shape for `SPEAKER_UPDATE` (client forges the speaker list — UI displays it; receiver thinks attacker is in the speaker set without server agreement) and `LAYER_HINT` (client tells server "I selected your top layer" out-of-band, confusing layer selector).

**Why this isn't already mitigated.** The plan never says which packet types are client→server vs. server→client. Today the codebase has an example *correctly handled*: `packet_handler.rs:70-72` drops client-originated CONGESTION because the comment shows someone thought about the direction. The new packet types need the same treatment.

**Mitigation (must land with the phase that introduces each packet).**
- Define a `PacketType` direction matrix as part of ADR-0001 or a new ADR:
  - **Client→server only:** `CAPABILITY_ANNOUNCE`, `SUBSCRIPTION_UPDATE`, `MEDIA` (with RoutingHeader as part of MediaPacket), `KEYFRAME_REQUEST`.
  - **Server→client only:** `SPEAKER_UPDATE`, `LAYER_HINT`, `ADMISSION_DECISION`, `CONGESTION`, `SESSION_ASSIGNED`, `MEETING_*`.
  - **Either:** none.
- Extend `packet_handler.rs::classify_packet` to drop client-originated server-only PacketTypes (mirroring the CONGESTION pattern at line 70).
- Server-only outbound packets must be signed/authenticated when they cross pods (S-P1-3 below covers this for `ADMISSION_DECISION` specifically).
- **Owning phase:** P1 (proto), with the classifier-drop in same phase as a hard requirement.
- **Suggested bead:** `s-p0-2-packet-direction-discipline` (feature; lands in P1 before p1-13 clippy gate).

#### S-P0-3. Admission control deferred to P3 leaves P0–P2 wide open at 200 joins

> **PLAN.md anchor:** Open Risk #4 *"Admission control at 200. Need soft cap at 195 + 5-slot waiting room … Wire into phase 3."*

**Attack.** Between P0 and P3 closing (~7–13 days per the plan), there is no participant cap. A scripted attacker can use a valid JWT (or a leaked JWT from a real meeting) to spawn 1000 sessions in a single room. Each session opens a WebTransport connection (256-slot outbound mpsc, ~1MB buffer minimum, plus QUIC connection state per session). At 1000 sessions on a single pod with current 8GB profile = roughly 1GB session state + the broadcast amplification means every join is fanned out to 999 peers (`PARTICIPANT_JOINED`), so the OOB control plane (NATS) is also blown up.

**Why this isn't already mitigated.** Today, with full-mesh broadcast and no cap, the system *might* survive 200 (PERFORMANCE.md does claim 250 viewer scaling). The plan introduces an SFU that *handles* 200 efficiently — but doesn't introduce admission control until after the architecture invites larger meetings. There is a window of vulnerability.

**Mitigation (lift to P0 or P1, no later).**
- Land a hard cap (`MAX_PARTICIPANTS_PER_ROOM = 200`) in `chat_server.rs::JoinRoom` as a **P0 task**. Reject the 201st joiner with `ADMISSION_DECISION { rejected: true, reason: "room full" }`. This is one-day work and removes the worst-case OOM during P1–P2 dev.
- The waiting-room logic (existing observer mode) stays in P3 as planned. The cap is the gate.
- **Owning phase:** P0 — add as `s-p0-3-hard-admission-cap` blocking p0-14 (the test-plumbing gate).
- **Suggested bead:** `s-p0-3-hard-admission-cap` (feature, P0).

#### S-P0-4. NATS subject pattern allows cross-room subscription if NATS is exposed

> **PLAN.md anchor:** §D *"Keep the publish side unchanged (per-sender subject `room.{room_id}.{session}`)."*

**Attack.** If NATS is reachable from a process other than the SFU pods (e.g., a leaked NATS credential, a misconfigured ingress, a sidecar in the same pod that's compromised), an attacker can subscribe to `room.>` and receive every packet of every meeting in the cluster.

**Why this isn't already mitigated.** Existing JWT validation only protects the SFU's HTTP path. NATS auth is opaque from the plan. `token_validator.rs:170-179` correctly forbids `.` and `>` in room IDs in JWTs (defense in depth against NATS subject injection on the producer side), but says nothing about NATS auth on the consumer side.

**Mitigation.**
- Configure NATS with per-credential subject ACLs: a videocall-rs pod can publish to `room.>` and `room.>.system` but not subscribe to `>` (or only to `room.{room_id}.*` after a JWT-validated client joins — this requires more dynamic subject management than the current static config).
- At minimum, ensure NATS is on a private network (Kubernetes service of type `ClusterIP`, no `Ingress`).
- Add a P6 audit: enumerate every component that holds NATS credentials and confirm credential scope.
- **Owning phase:** P0 (audit existing NATS config), then P6 if changes needed.
- **Suggested bead:** `s-p0-4-audit-nats-acls` (chore, P0; produces a report).

---

### P1 (phase-close gate)

#### S-P1-1. PacketWrapper.session_id is client-supplied and trusted in places

> **Codebase reality:** `actix-api/src/actors/transports/wt_chat_session.rs:333-338` parses the inbound `PacketWrapper` and uses the `session_id` field for outbound-drop accounting (`session_logic.rs:426 on_outbound_drop`). NATS subject routing on the *publish* side uses the *server-tracked* session_id, but several places downstream consume the client-claimed value.

**Attack.** A peer claims another peer's `session_id` in its `PacketWrapper.session_id`. Effects:
- Receivers' self-skip logic (which compares peer-claimed session_id) drops their own legit packets, or mis-identifies the speaker.
- `CongestionTracker` mis-attributes drops to the wrong sender, causing the wrong session to receive `CONGESTION` notifications and step down.
- Per-receiver speaker scoring (P3) mis-attributes `audio_level` to a stable session_id that belongs to a victim → spotlight inversion attack.

**Why this isn't introduced by the plan but is amplified by it.** Pre-SFU, the attack was already possible. Post-SFU, the consequences widen (speaker selection, layer selection).

**Mitigation.**
- Server overwrites `PacketWrapper.session_id` to the connection's server-tracked session_id on every inbound media path. This is one line in `packet_handler.rs` / `wt_chat_session.rs::handle_inbound`. The original sender's intended session_id is irrelevant — only the *server-known* one is authoritative.
- For client→server packets where the session_id is metadata (e.g., `SubscriptionUpdate` referencing other senders' session_ids), validate that all referenced session_ids belong to the same room.
- **Owning phase:** P2 (forwarder lands; this is where the trust boundary moves).
- **Suggested bead:** `s-p1-1-overwrite-claimed-session-id` (feature, P2).

#### S-P1-2. SubscriptionUpdate amplification — unbounded fan-in

> **PLAN.md anchor:** §F *"`pinned_sessions`: always forward, regardless of speaker rank."*

**Attack.** Receiver sends `SubscriptionUpdate { pinned_sessions: [200 session_ids] }` every 10ms. Per-session subscription reconciliation runs on the SFU on every receipt. At 1k SubscriptionUpdates/sec × 200 list-size = 200k operations/sec just for one malicious receiver. Multiplied across N malicious receivers, this is a control-plane DoS.

The plan caps `max_visible_video = 6` so the *effective* allow-set is bounded. But that's enforced on output. The *processing* of the giant pin list happens before the cap.

**Mitigation.**
- Rate-limit `SubscriptionUpdate` per receiver: max 4 per second (subscription is meant for visibility / pin changes, not 1kHz updates). Excess silently dropped.
- Hard cap `pinned_sessions.len()` at 16 (8 frontal pins + 8 history); reject with `LAYER_HINT { error: "subscription too large" }`.
- Reconciliation cost is bounded by the cap; metric `sfu_subscription_update_rate{sid}` reports violations.
- **Owning phase:** P3.
- **Suggested bead:** `s-p1-2-rate-limit-subscription` (feature, P3; modifies p3-4).

#### S-P1-3. ADMISSION_DECISION redirects need to be signed

> **PLAN.md anchor:** Phase 6 *"server responds with `ADMISSION_DECISION { redirect_to: \"webtransport-{owner}.webtransport-headless.svc:443\" }`"*.

**Attack.** Even if S-P0-2 catches client-originated `ADMISSION_DECISION` packets, a compromised SFU pod (or a pod with a credential leak) can emit a redirect to *any* host. The client's `ConnectionManager` (videocall-client/src/connection/connection_manager.rs) honors it. One bad pod + open redirect = wholesale meeting hijack.

**Mitigation.**
- `redirect_to` is signed by the *meeting-api* (the trust root that also issues JWTs), not by the SFU pod itself. The SFU pod inserts the meeting-api-signed redirect into `ADMISSION_DECISION`. Client validates the signature with the meeting-api's public key.
- Alternative cheaper option: maintain a client-side allowlist (the `region.svc:443` patterns issued in JWTs at meeting start) and refuse redirects outside the allowlist.
- **Owning phase:** P6.
- **Suggested bead:** `s-p1-3-sign-admission-redirect` (feature, P6).

#### S-P1-4. KEYFRAME_REQUEST cost asymmetry

> **PLAN.md anchor:** §G *"Keyframe requests funnel through the layer-aware path"*, and existing rate-limit at `packet_handler.rs:115-143` is `KEYFRAME_REQUEST_MAX_PER_SEC = 2` per **sender session**.

**Attack.** Attacker has 100 receiver sessions in one room (cheap — JWTs may be re-usable depending on auth config). Each session sends 2 KFRs/sec to victim sender. Result: victim's encoder produces ~200 keyframes/sec (CPU spike + 200× bitrate amplification → bandwidth blast on victim's uplink). Existing per-sender rate-limit applies on the *requester* side, not the *recipient* side.

**Mitigation.**
- Rate-limit on the *recipient*: each sender can be asked for at most 2 keyframes/sec *regardless of how many receivers request them*. Excess KFRs are deduplicated by the SFU into a single forwarded KFR.
- Layer-aware path already plans to "not blast a 1.5Mbps keyframe to a 200kbps receiver" — extend it to coalesce concurrent KFRs.
- **Owning phase:** P4 (where layer-aware KFR routing lands).
- **Suggested bead:** `s-p1-4-keyframe-coalesce` (feature, P4; expands p4-10).

#### S-P1-5. Capability bitmask is client-claimed, not certificate-bound

> **PLAN.md anchor:** §C *"`client_capabilities` (bits: `SFU_ROUTING_HEADER=1`, `SVC=2`, `SUBSCRIPTION=4`)."*

**Attack.** A malicious client claims capabilities it doesn't actually have (`SVC=2`) to receive higher-quality forwards, then drops the enhancement layers. Cost to attacker: receives more bandwidth than entitled. Cost to system: wasted upstream/downstream.

This is low-severity but worth noting; capabilities aren't trust-bearing so the worst case is bandwidth waste, not security breach.

**Mitigation.**
- Server probes: if a client claims `SVC` but its observed bandwidth-of-receipt is below the budget for the highest layer, downgrade automatically. Existing `DiagnosticsPacket` provides the input.
- Document that capability claims are advisory, not load-bearing for security.
- **Owning phase:** P4.
- **Suggested bead:** `s-p1-5-capability-claim-is-advisory` (chore; doc-only).

---

### P2 (pre-public-launch)

#### S-P2-1. Recording bot has unconstrained capability

> **PLAN.md anchor:** Open Risk #6 *"`IS_RECORDER` bit → forwarder bypasses layer dropping and `max_visible_video` cap."*

**Attack.** A peer with a `room_join: true` JWT claims `IS_RECORDER`. Server bypasses all caps for that peer → it receives every video at top layer + every audio. Meeting privacy bypassed.

**Mitigation.**
- `IS_RECORDER` requires a separate JWT claim (e.g., `recorder: true`) signed by meeting-api, not a free-claim capability bit.
- Recording bot's identity is announced to all participants via `PARTICIPANT_JOINED` with a visible "recording" flag (UI shows a red dot).
- **Owning phase:** P3 or wherever IS_RECORDER lands (currently no phase owns it explicitly).
- **Suggested bead:** `s-p2-1-recorder-jwt-claim` (feature; create new phase or bolt onto P3).

#### S-P2-2. Cross-region NATS message tampering

> **PLAN.md anchor:** Open Risk #7 *"only the owner pod computes the speaker set; spill pods consume `SpeakerUpdate` from `room.{room}.system`."*

**Attack.** A compromised spill pod publishes a forged `SpeakerUpdate` to `room.{room}.system`. All non-owner pods consume it; their forwarders push the attacker's chosen "speaker set" to clients.

**Mitigation.**
- Within-cluster NATS uses mTLS + per-pod credentials with subject ACLs (`room.>.system` is publishable only by the owner-pod credential, which rotates with leader election).
- Owner pod signs `SpeakerUpdate` payloads with a per-room ephemeral key; spill pods verify before consuming.
- **Owning phase:** P6.
- **Suggested bead:** `s-p2-2-signed-speaker-updates` (feature, P6).

#### S-P2-3. Sub-optimal cleartext leaks via RoutingHeader

> **PLAN.md anchor:** ADR-0001 design *"encrypted payload + clear routing header."*

**Threat.** Even with the payload encrypted, the cleartext routing header leaks:
- `audio_level` over time = a fingerprint of who's speaking, when, for how long. An external observer with packet captures can infer meeting dynamics ("Alice spoke for 5 min", "everyone went quiet at the question").
- `temporal_layer_id` patterns reveal the encoder's GOP structure (predictable: every Nth frame is a keyframe → camera-on/off transitions).
- `is_speaking` directly leaks who's talking. Sequential analysis is more powerful than per-packet inspection.

Acceptable for typical use but a regression vs. the previous full-payload encryption (where the SFU couldn't read anything).

**Mitigation.**
- Bucket `audio_level` to coarse classes (e.g., 4 buckets) rather than float-precision.
- Document the leak explicitly in ADR-0001 as a deliberate trade-off.
- Optional: mode for "audio quiescent mask" — if `is_speaking == false`, omit audio packets entirely (current behavior is to send silence frames). Reduces signal.
- **Owning phase:** P1 (when the header lands) for the bucketing decision; ADR-0001 (P0) for the documentation.
- **Suggested bead:** `s-p2-3-routing-header-leak-doc` (decision; bolt onto ADR-0001).

#### S-P2-4. Spillover pod accepts cross-pod forwards without re-validation

> **PLAN.md anchor:** §I *"Spill pods federate transparently because they already subscribe to `room.{room}.*`."*

**Attack.** A misbehaving (or compromised) sender pod publishes media for `room.X.session=Y` even though session Y is not actually in room X. Spill pods will forward to their receivers because they trust the NATS subject.

**Mitigation.**
- Each SFU pod verifies the sender's `session_id` is registered in `RoomState.members` (per the owner-pod-published membership list). Unregistered → drop.
- This is a per-packet check; could be expensive at 200 sessions × ~50 packets/sec each → 10k membership checks/sec. Implement as a Bloom filter for cheap rejection of obviously-bogus session_ids; full hash-map check on hit.
- **Owning phase:** P6.
- **Suggested bead:** `s-p2-4-cross-pod-membership-check` (feature, P6).

#### S-P2-5. Room ID entropy

> **PLAN.md anchor:** absent; codebase reality: room IDs come from JWT claims, but the plan never says how they're generated.

**Attack.** If room IDs are guessable (sequential, dictionary words, short random strings), an attacker who learns the room ID joins the room (assuming they can also forge a JWT for that room, which depends on JWT secret rotation — see S-P3-1). At minimum, learning the room ID enables targeted social engineering.

**Mitigation.**
- Room IDs should be ≥128 bits of entropy (e.g., UUIDv4 or base64-encoded 16-byte random).
- Audit meeting-api's room-ID generation (look at `meeting-api/src/`).
- **Owning phase:** P0 audit.
- **Suggested bead:** `s-p2-5-audit-room-id-entropy` (chore, P0).

#### S-P2-6. Worktree disk-fill DoS

> **PLAN.md anchor:** §B0 standing guardrail *"abandoned worktrees over 1GB warrant cleanup."*

**Attack.** A malicious or buggy polecat creates large files in its worktree (e.g., `dd if=/dev/zero of=blob bs=1M count=10000`). Worktree balloons. Server disk fills. Other rigs/services on the host crash.

**Mitigation.**
- Worktree quotas via Linux quota or filesystem cgroup.
- Daemon-side polecat heartbeat reports worktree size; Witness nukes offenders over a soft cap (e.g., 5GB).
- **Owning phase:** P6 (when production-ish loads start).
- **Suggested bead:** `s-p2-6-polecat-worktree-quota` (feature; ops-side).

---

### P3 (long-tail hardening)

#### S-P3-1. JWT secret rotation

> Codebase reality: `token_validator.rs` reads a single shared secret. The plan adds new packet types that may want to be authenticated by the same secret.

**Attack.** JWT secret leaks (logs, misconfigured env var, compromised pod). All historical and future tokens are forge-able until secret rotated. Plan doesn't address rotation.

**Mitigation.** JWT issuer supports key rotation (kid header, parallel verification of N keys, rotate N-1 → N every X days). Out of plan's current scope but worth filing.
- **Owning phase:** Bug-bash backlog.
- **Suggested bead:** `s-p3-1-jwt-rotation-design` (decision).

#### S-P3-2. Metric cardinality

> **PLAN.md anchor:** §B Phase 2 metrics list.

If counters are labeled `{from_sid, to_sid}` at 200×200 = 40k label combinations, Prometheus scrape cost balloons. Aggregate metrics first; per-pair only behind a debug flag.
- **Owning phase:** P2.
- **Suggested bead:** `s-p3-2-cap-metric-cardinality` (chore).

#### S-P3-3. Mayor mail privacy

> Bootstrap reality: I sent the full PLAN.md as a pinned, permanent mayor mail. Permanent = persists in Dolt forever.

The plan isn't secret in this case but the *pattern* is: durable mayor mail is forever-readable to anyone with Dolt access. Mail with sensitive content (credentials, customer PII, incident details) is a leak channel.
- **Owning phase:** N/A (gastown-level policy, not SFU plan).
- **Suggested bead:** none — policy memo, not a bead.

#### S-P3-4. VP9 SVC malformed-bitstream parsing

> **PLAN.md anchor:** §C `RoutingHeader.frame_marker = 6;`.

The server reads SVC layer flags from the routing header. The original SVC bitstream is still in the encrypted payload. If a malicious sender claims `temporal=2 spatial=0` in the header but the actual encrypted payload has `temporal=0 spatial=0`, the forwarder routes wrong. Worst case: confusion (degraded experience for some receivers), not security breach.
- **Owning phase:** P4.
- **Suggested bead:** `s-p3-4-routing-header-vs-payload-divergence` (chore; observability — count mismatches).

#### S-P3-5. Bot harness used as attack tool

> **PLAN.md anchor:** §K *"`bot/` is the right driver."*

If `bot/` is open-source (likely — `videocall-rs` is public), anyone can run 200 bots against the public deployment and load-test it adversarially.

Mitigation: rate-limit per source IP at the WebTransport listener; require JWT for each session (the existing JWT validation does this — the cost is "attacker needs JWTs", which depends on meeting creation rate-limits).
- **Owning phase:** P6.
- **Suggested bead:** `s-p3-5-source-ip-rate-limit` (feature; ops).

---

## 3. Quick-wins available right now (before any new phase opens)

These can land in P0 as additional beads, or as a single "S-quickwins" convoy. Each is small, well-bounded, and removes one of the most acute risks.

1. **Hard admission cap** (S-P0-3). One-line addition to `chat_server.rs::JoinRoom`. ~30 min.
2. **NATS subject ACL audit** (S-P0-4). Read `helm/global/nats/` (if it exists) + Helm values. Document findings. ~1 hour.
3. **Add unit test asserting `packet_handler.rs` drops client-originated CONGESTION** (existing behavior, no test). Lock-in the property so it can't regress when new packet types are added. ~30 min.
4. **Refinery push contract pick** (G-C8). Decide and document. ~15 min.
5. **DAG single-source-of-truth pick** (C-10). Decide and document. ~30 min.

If all five land before P1 opens, the experiment ships on much firmer ground.

---

## 4. Suggested bead bundle: convoy `S0` (security pre-flight)

Add the following to `convoy-manifest.yaml` as a sibling of P0, blocking P1:

```yaml
convoys:
  - key: S0
    title: "SFU Security Pre-flight (P0-class risks)"
    parent_epic: sfu-epic
    summary: |
      Quick-wins and P0-class security findings from GAP-ANALYSIS.md.
      Must close before P1 opens.
    tracks:
      - s-p0-3-hard-admission-cap
      - s-p0-4-audit-nats-acls
      - gap-c8-refinery-push-contract
      - gap-c10-dag-source-of-truth
      - s-p0-2-packet-direction-discipline  # (lands as part of P1 itself, but referenced here)
```

For each non-quickwin P0/P1 finding, add a `s-px-x-...` bead to the phase where it lives. Reference this audit document in each bead's description.

---

## 5. What I deliberately did NOT cover

- **Codec-level fuzzing** of VP9 SVC parsers (`videocall-codecs/`). Out of scope; would be a separate audit.
- **Browser-side WebCodecs attack surface.** Out of scope.
- **Supply chain.** `Cargo.lock` audit, dependency review, container image provenance. Out of scope; mention because the SFU work expands the dependency graph (no new top-level crates were mandated, but check at each phase exit).
- **Cryptographic review of the existing AES/RSA E2EE.** Out of scope; the plan inherits it and the audit assumes it's correctly implemented.

If you want any of these added, file a new convoy.

---

## 6. Reviewer notes

This audit was performed by a single reviewer in one pass. Replicate with at least one independent reviewer before treating it as authoritative. Specifically:
- S-P0-1 (routing header forgery) is the highest-confidence finding; it follows directly from the SFU's design and is hard to mitigate cleanly. Independent confirmation is cheap and worth it.
- S-P1-1 (PacketWrapper.session_id trust) needs verification by reading actual call sites — I confirmed two (CongestionTracker, packet_handler classify) but there may be others.
- The cryptographic mitigations in S-P2-2 (signed SpeakerUpdate) need a real protocol design, not the one-paragraph treatment given here.

End of audit.
