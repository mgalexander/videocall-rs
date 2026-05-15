# SFU Refactor of videocall-rs — Implementation Plan

Target branch: `experimental-sfu` (local-only in phase 1)
Planning artifacts: `/mnt/llms/videocall/sfu-update/`
Umbrella RFC: `/mnt/llms/videocall/rfc/rfc-2-sfu-architecture.md`

---

## Context

`/mnt/llms/videocall` is a fresh clone of `mgalexander/videocall-rs`. Today the server is a pub/sub fanout over NATS, not a true SFU: every encrypted media packet is republished to every peer in a room. The README markets "No SFUs," but `PERFORMANCE.md:95-100` already documents SFU mode and `ARCHITECTURE.md:308-314` calls out simulcast and tiered quality as future work. At 200 participants, blind fanout collapses on outbound bandwidth and on the `mpsc(256)` per-session buffer that today drops packets FIFO.

This plan turns the server into a real SFU on an experimental branch, targeted at **webinar-shape meetings of up to 200 participants** (≤10 active video senders, ~190 listeners). It keeps WebTransport (QUIC) as the primary transport and WebSocket as the fallback. It preserves the project's *inbound queuing* discipline but changes the outbound queue from FIFO-with-silent-drop to priority-aware-with-class-drop, because the bottleneck is fanout, not ingestion.

E2EE is preserved in evolved form: media payload remains encrypted, but `MediaPacket` gains an unencrypted `RoutingHeader` (SFrame-style) carrying layer ids, keyframe flag, and audio level — exactly enough for the SFU to forward intelligently without ever decrypting media.

## Locked Decisions (from interview)

1. **Meeting shape:** webinar first (≤10 active video, rest listeners). Hooks for conference shape later.
2. **E2EE posture:** evolve — encrypted payload + clear routing header on `MediaPacket`.
3. **Selection model:** hybrid — server picks default active-speaker set; client overrides via pins/visibility slots.
4. **Client contract:** coordinated client+server changes allowed; wire protocol can evolve.
5. **Sender encoder:** VP9 SVC via WebCodecs (`scalabilityMode: "L1T3"`), single bitstream with temporal/spatial layers; SFU drops layers per receiver.
6. **Room routing:** hybrid room-affinity; consistent-hash room_id → preferred pod; NATS handles spillover and cross-region.
7. **Decision artifacts:** umbrella RFC at `/rfc/rfc-2-sfu-architecture.md`; granular ADRs under `/sfu-update/adr/`; planning notes (capacity model, packet diagrams, test matrix) under `/sfu-update/`.
8. **Inbound queuing:** keep current bounded-with-drop discipline; **change outbound** to priority queue with class-aware drop policy.

## Bootstrap into Gastown (executed by this session before any SFU work)

The full DAG is meant to run under gastown's daemon, not be driven step-by-step from this chat. This section is the hand-off ritual: I bring the sandbox up, register the rig, prime the Mayor, and watch a first bead actually execute end-to-end. Once that loop closes once, the system governs the rest and I downgrade to author/reviewer.

### Standing guardrails (in force for every bootstrap step and the SFU phases)
- **Disk:** before any container restart or bead launch, run `df -h /` and `docker system df`. Soft alert at 80%, halt at 85% — escalate to user, don't continue.
- **Local-only:** no `git push` without explicit user approval (one approval per push). Polecat work happens in **git worktrees** rooted under `/mnt/llms/videocall/.worktrees/` (or rig convention). Merges land on `experimental-sfu` locally first; promotion to remote `main` is gated on user say-so.
- **Container caution:** `gastown-sandbox` has been up ~2 days hosting `lps-` and `imap-` rigs. Don't restart it unless the mount situation requires it; prefer hot-mount or already-present paths.

### B0 — Verify sandbox + mount situation (read-only)
- `docker ps` confirms `gastown-sandbox` running.
- `docker inspect gastown-sandbox` to enumerate current bind mounts.
- If `/mnt/llms` (or `/mnt/llms/videocall`) is already mounted into the container, skip B1.
- Read `/mnt/llms/gas-town/docker-compose.yml` and `docker-compose.override.yml` to understand the canonical mount config.
- Capture baseline `df -h /` and `docker system df` to a note in `sfu-update/ops-log.md`.

### B1 — Map `/mnt/llms/videocall` into the sandbox (only if not already visible)
- Preferred: append a bind mount to `docker-compose.override.yml` (read-write) targeting the same path inside the container, then `docker compose up -d gastown-sandbox` to apply with minimal disruption to running rigs.
- Confirm by `docker exec gastown-sandbox ls /mnt/llms/videocall` returning the repo tree.
- If the user prefers I not touch the override file, alternative is `docker run --mount` for a sidecar, but that breaks the `gt` shared workspace assumption — flag and ask before going that way.

### B2 — Enrol `videocall` as a rig
- Inside the container: `docker exec gastown-sandbox gt rig add videocall --path /mnt/llms/videocall --prefix vc-` (prefix proposal; user can override).
- This writes a new line to `/mnt/llms/gas-town/town/.beads/routes.jsonl` (`{"prefix":"vc-","path":"videocall"}`) and creates `/mnt/llms/videocall/.beads/{metadata.json,config.yaml,audit.log,locks/}`.
- Verify: `docker exec gastown-sandbox gt rig list` shows videocall; `cat /mnt/llms/videocall/.beads/metadata.json` shows the prefix.
- If `gt rig add` is not the actual subcommand, fall back to creating `.beads/` directly with the metadata pattern documented in `/mnt/llms/gas-town/town/CLAUDE.md` ("metadata.json + config.yaml pattern" referenced as the recovery procedure for stub rigs) and then `gt repair` from inside the container.

### B3 — Prime the Mayor with rig context
- `docker exec -it gastown-sandbox gt prime --as mayor` (or equivalent — confirm against the town's STARTUP.md) so the Mayor's session reloads `routes.jsonl` and sees `vc-`.
- Send a structured handoff to the Mayor: `docker exec gastown-sandbox gt mail send --to mayor --subject "videocall rig enrolled" --body-file /mnt/llms/videocall/sfu-update/PLAN.md`.
- The mail is durable (creates a bead + Dolt commit per `town/CLAUDE.md`), so the Mayor sees this even if its session restarts.

### B4 — Author the planning artifacts (still authored by me, *consumed* by the Mayor)
- Create on disk under `/mnt/llms/videocall/sfu-update/` (this Claude session, in worktree-aware mode):
  - `PLAN.md` — copy of this plan file, this becomes the source of truth inside the rig.
  - `convoy-manifest.yaml` — machine-readable representation of the DAG (one entry per bead with `id, type, title, summary, deps:[{kind:blocks|parent-child, target}], wave, parent_convoy`). The Mayor (or `scripts/materialize.sh`) parses this to invoke `bd create` / `bd update --add-dep`.
  - `scripts/materialize.sh` — idempotent shell script that walks the manifest, runs `bd create` for missing beads, runs `bd update --add-dep blocks:<id>` for missing edges, and emits a summary. Re-runnable without producing duplicates.
  - `ops-log.md` — running log of bootstrap actions, disk readings, container restarts, mayor responses. Mirrored back to me on Mayor escalation.
  - `worktrees/.gitkeep` — and add `.worktrees/` to `.gitignore`.
- Branch `experimental-sfu` is created locally now (not by a polecat) so `sfu-update/` lives on it from commit one.

### B5 — Materialise the umbrella + P0 only
**Don't** materialise all six convoys at once. Start small to validate the loop.
- Manifest at this point contains: `sfu-epic` (epic), `P0` (convoy), and beads p0-1 through p0-14.
- Either `docker exec gastown-sandbox bash sfu-update/scripts/materialize.sh` runs the bd commands, or the Mayor (on receipt of the B3 mail) runs its own materialise formula. Pick based on Mayor capability — fallback is the script.
- Verify: `docker exec gastown-sandbox bd list --prefix vc- --status open` shows ~15 entries.

### B6 — Stage and launch P0
- `docker exec gastown-sandbox gt convoy stage P0` — must report `staged:ready` with Wave 1 = `[p0-1]`. If `staged:warnings`, resolve before launch.
- `docker exec gastown-sandbox gt convoy launch P0` — transitions to `open`, slings p0-1 to a Rust-capable polecat (or pool dispatch if no preferred polecat).

### B7 — Watch the first bead execute end-to-end
This is the proof loop. Things to observe and log to `ops-log.md`:
- Polecat session spawned (visible via `gt polecat list` or in container logs).
- Polecat creates worktree under `/mnt/llms/videocall/.worktrees/p0-1-<polecat-id>/`.
- Polecat performs p0-1's scope: scaffold `sfu-update/` README + adr/ subtree + capacity-model.md/packet-diagrams.md/test-matrix.md skeletons. (Note: the polecat is *expanding* on what I already wrote — placeholder files become real files.)
- Polecat commits to the worktree, opens a merge request through the Refinery.
- Refinery merges into `experimental-sfu` locally; **no push**.
- Polecat closes p0-1 via `bd update p0-1 --status done`.
- Daemon's event-driven feeder detects the close and slings Wave 2 (p0-2, p0-11). Stop here — confirm Wave 2 dispatch is observed, then **pause**.

### B8 — Document scale-up for the container's consumption
At this point I write the scale-up dossier into the rig in a form the Mayor and Witness can act on without further chat-driven instructions:
- `sfu-update/SCALE-UP.md` — the operational ramp narrative: when to materialise P1..P6, the per-phase wave count, expected polecat capability mix per phase (Rust backend, frontend WebCodecs, Helm/K8s for P6), CI gate thresholds (50-bot smoke = merge gate; 200-bot = release gate).
- `sfu-update/FANOUT.md` — how the convoy daemon should distribute work across polecats: pool dispatch acceptable for P0/P1; reserve a frontend polecat for P3/P4 (client-side encoder + UI work); reserve a Helm/K8s polecat for P6.
- `sfu-update/ops-log.md` — populated continuously through B0..B7; becomes the historical record.
- Send a second `gt mail send --to mayor` referencing `SCALE-UP.md` and `FANOUT.md` and authorising the Mayor to materialise P1 only after I (the user) confirm P0 close.

### B9 — Monitoring loop (runs through every following bead)
- Disk: every 10 min, `df -h /` + `docker system df`. Append to `ops-log.md`. Alert at 80%, halt at 85%.
- Dolt: per `town/CLAUDE.md`, if `bd` commands hang, run `gt dolt dump` and `gt dolt status` BEFORE any restart; never `rm -rf .dolt-data/`.
- Worktree size: `du -sh /mnt/llms/videocall/.worktrees/*` — abandoned worktrees over 1GB warrant cleanup.
- Mayor escalations: any `gt escalate` reaching me triggers a chat re-engagement.

### B10 — Hand-off complete; cede control
Once Wave 2 of P0 is observed and P0 itself reaches `open` with multiple waves in flight, this session steps back. From here:
- Daemon drives feed; Witness watches polecat health; Refinery handles merges into `experimental-sfu`.
- I re-engage on (a) Mayor escalations, (b) end-of-phase reviews, (c) drift between `sfu-update/PLAN.md` and reality, (d) the manual-approval gate before each `git push`.

### Bootstrap checklist (concrete, in execution order)
1. `docker ps` + `docker inspect gastown-sandbox` — confirm container, capture mount config.
2. `df -h /` + `docker system df` — baseline disk.
3. Read `/mnt/llms/gas-town/docker-compose.yml`, `docker-compose.override.yml` — understand current state.
4. Decide: mount already present? If yes → step 6.
5. Edit `docker-compose.override.yml` to add `/mnt/llms/videocall:/mnt/llms/videocall:rw` mount; `docker compose up -d gastown-sandbox`; reverify `docker exec gastown-sandbox ls /mnt/llms/videocall`.
6. `git checkout -b experimental-sfu` (local).
7. Author `sfu-update/{PLAN.md, convoy-manifest.yaml, scripts/materialize.sh, ops-log.md}` and update `.gitignore` to exclude `.worktrees/`.
8. `docker exec gastown-sandbox gt rig add videocall --path /mnt/llms/videocall --prefix vc-` (or fallback metadata.json + `gt repair`).
9. `docker exec -it gastown-sandbox gt prime --as mayor` then `gt mail send --to mayor` with `PLAN.md`.
10. Run materialize (script or Mayor formula); verify with `bd list --prefix vc-`.
11. `gt convoy stage P0` — must be `staged:ready`.
12. `gt convoy launch P0`.
13. Watch p0-1 dispatch → polecat worktree → Refinery merge into `experimental-sfu` → close → Wave 2 dispatch.
14. Author `SCALE-UP.md` + `FANOUT.md` + final mayor mail.
15. **Pause for user confirmation** before allowing P1 to materialise.

---

## Workspace Setup (Phase 0a, executed first)

These are deterministic prep steps, separate from the design phases.

1. `git checkout -b experimental-sfu` from current HEAD (`c01a773`).
2. Create directory tree:
   - `/mnt/llms/videocall/sfu-update/`
     - `README.md` — index of artifacts in this directory
     - `capacity-model.md` — back-of-envelope from §J below
     - `packet-diagrams.md` — sequence diagrams of new packet types
     - `test-matrix.md` — codec / browser / shape coverage
     - `adr/` — granular ADRs (template: Context / Decision / Consequences / Status)
       - `0001-routing-header-out-of-encryption.md`
       - `0002-active-speaker-detection.md`
       - `0003-hybrid-subscription-model.md`
       - `0004-outbound-priority-queue.md`
       - `0005-room-affinity-routing.md`
3. Create `/rfc/rfc-2-sfu-architecture.md` — the umbrella proposal that links each ADR.
4. Note: branch stays local in phase 1. Recommend pushing to a personal fork for backup; not required.

## Phased Implementation (6 phases, each independently mergeable on `experimental-sfu`)

### Phase 0 — Decision substrate & feature flag (0.5–1 day)
- Add `SFU_MODE` env (`legacy` | `sfu`) read in both server binaries.
- Create `actix-api/src/sfu/mod.rs` and `actix-api/src/sfu/config.rs` as the new module root.
- `SFU_MODE=sfu` is a no-op shim today; it logs and falls through to legacy paths.
- Land RFC + ADR scaffolds.
- **Exit:** both binaries boot with either flag value; unit test asserts flag plumbing.

**Files:** new `/rfc/rfc-2-sfu-architecture.md`, `/sfu-update/**`, `actix-api/src/sfu/mod.rs`, `actix-api/src/sfu/config.rs`; modify `actix-api/src/bin/webtransport_server.rs`, `actix-api/src/bin/websocket_server.rs`, `actix-api/src/lib.rs`.

### Phase 1 — Wire protocol: routing header + new packet types (1–2 days)
- Extend `MediaPacket` with an optional `RoutingHeader` (field 10).
- Add `PacketType` values: `SUBSCRIPTION_UPDATE`, `SPEAKER_UPDATE`, `LAYER_HINT`, `ADMISSION_DECISION`.
- New protos: `subscription_packet.proto`, `speaker_update_packet.proto`.
- Add `client_capabilities` bitmask to `CONNECTION` packet (bits: `SFU_ROUTING_HEADER`, `SVC`, `SUBSCRIPTION`).
- Client populates `RoutingHeader` for video (from WebCodecs chunk metadata `svc.temporalLayerId`) and audio (RMS pre-encode → `audio_level`).
- All new proto fields are optional; legacy clients stay compatible via field defaults.
- **Exit:** legacy + new clients coexist; new client emits headers; server logs them; no routing change yet.

**Files:** modify `protobuf/types/{media_packet.proto, packet_wrapper.proto, connection_packet.proto}`; new `protobuf/types/{subscription_packet.proto, speaker_update_packet.proto}`; modify `videocall-client/src/encode/{camera_encoder.rs, microphone_encoder.rs, screen_encoder.rs}`; modify `videocall-client/src/connection/connection_manager.rs` (capabilities advertisement); modify `actix-api/src/actors/packet_handler.rs` (parse-and-pass-through).

### Phase 2 — SFU forwarder module (3–5 days)
- Introduce `actix-api/src/sfu/{forwarder.rs, room_state.rs, subscription.rs, speaker.rs, layer_selector.rs}`.
- Forwarder is **not** an actor — it's `Arc<RwLock<RoomState>>` consulted from each receiver's NATS callback. (Avoids serializing the whole room behind one mailbox.)
- NATS publish side unchanged (`room.{room}.{session}`). Filter moves consumer-side: in `actix-api/src/actors/chat_server.rs::handle_msg` (around chat_server.rs:784), each receiver's subscription task calls `Forwarder::decide(receiver_sid, packet_wrapper, routing_header) → Forward(bytes) | Drop`.
- Phase 2 selection logic is pass-through — observable, parity with legacy, but the plumbing is now in place.
- **Exit:** `SFU_MODE=sfu` reaches parity with legacy for 1:1 and 1:N rooms; integration test asserts every sent packet is received.

**Files:** new `actix-api/src/sfu/{forwarder.rs, room_state.rs, subscription.rs, speaker.rs, layer_selector.rs}` (stubs ok for non-forwarder modules); modify `actix-api/src/actors/chat_server.rs` (handle_msg + JoinRoom subscription wiring at chat_server.rs:560-765); modify `actix-api/src/actors/session_logic.rs` to expose `RoutingHeader` on the outbound path.

### Phase 3 — Active-speaker detection + subscription model (3–5 days)
- **Speaker scoring** (`sfu/speaker.rs`): per-sender EWMA on `audio_level` (α=0.3); "speaking" if `score > 0.05` and `is_speaking=true` arrived within 400ms; top-N=4 every 200ms tick; entry/exit hysteresis at ±0.05 over 200ms/800ms windows; generation counter on set change. Publish `SpeakerUpdate` to `room.{room}.system` on change.
- **Subscription model** (`sfu/subscription.rs`): `SubscriptionUpdate` is declarative — server replaces prior state. Reconciliation = `pinned ∪ default_speaker_set ∪ slot_sessions`, capped at `max_visible_video=6` for video, room-wide for audio. Stale entries silently dropped; pre-join entries held as pending (cap 50).
- **Forwarder** consults reconciled AllowSet: forward iff sender in receiver's AllowSet. Layer dropping still off — receiver gets whatever the sender sent.
- Client UI: existing `set_peer_visibility` in `videocall-client/src/client/video_call_client.rs:849` becomes the trigger to emit `SubscriptionUpdate`. New `videocall-client/src/sfu_client.rs` for the emit path.
- **Exit:** 12-client demo (6 senders + 6 listeners); listeners receive only the speaker set; pinning a non-speaker delivers their video within one RTT; speaker change propagates within 500ms.

**Files:** modify `actix-api/src/sfu/{speaker.rs, subscription.rs, forwarder.rs}`; modify `videocall-client/src/decode/peer_decode_manager.rs` (consume `SPEAKER_UPDATE`); new `videocall-client/src/sfu_client.rs` (emit `SubscriptionUpdate`); new `actix-api/src/sfu/tests/{speaker_tests.rs, subscription_tests.rs}`.

### Phase 4 — VP9 SVC + per-receiver layer dropping (4–7 days)
- Client encoder: WebCodecs `scalabilityMode: "L1T3"` (1 spatial, 3 temporal) initially; option for `"L3T3_KEY"` later. Parse encoded-chunk metadata to populate `RoutingHeader.temporal_layer_id` etc.
- **Layer selector** (`sfu/layer_selector.rs`): per-receiver budget = `min(estimated_downlink * 0.85, max_video_kbps)`. Greedy two-pass — pass 1 ensures base layer (T0, spatial 0) for every allowed sender that fits; pass 2 upgrades by priority while budget remains. Downgrades immediate on `CONGESTION` or drop signal; upgrades require 20% headroom for ≥3s; 5s cooldown after a downgrade-upgrade cycle.
- Invariants: keyframes with `temporal=0 spatial=0` are always forwarded; frames with `REFERENCES_T0=true` are dropped only if their T0 of the same `picture_id` was also dropped.
- KEYFRAME_REQUEST routing becomes layer-aware: don't blast a 1.5Mbps keyframe to a 200kbps receiver.
- **Exit:** receiver throttled to 500kbps via `network_throttle.py` receives base+T0 only; throttle lifts → upgrade to top layer within 2s; no thrash with ±20% RTT noise.

**Files:** modify `videocall-client/src/encode/camera_encoder.rs` (encoder config + chunk metadata extraction); modify `actix-api/src/sfu/{layer_selector.rs, forwarder.rs}`; modify `videocall-client/src/decode/peer_decode_manager.rs` (multi-rate decode; smarter KFR); modify `actix-api/src/client_diagnostics.rs` (bandwidth estimate exposure to forwarder).

### Phase 5 — Outbound priority queue with class-aware drop (2–3 days)
Replace `mpsc::channel::<WtOutbound>(256)` at `actix-api/src/webtransport/mod.rs:351` (and the WS analog) with a `PrioritySender` over 5 inner channels:

| Class | Size | Drop policy | Examples |
| --- | --- | --- | --- |
| P0 Control | 32 | never drop; log+stop session if full | RTT, heartbeat, SESSION_ASSIGNED, MEETING_*, CONGESTION, SPEAKER_UPDATE |
| P1 Audio | 128 | tail-drop oldest | all AUDIO packets |
| P2 Keyframe + base T0 video | 128 | tail-drop oldest | `is_keyframe=true` and `temporal=0 spatial=0` |
| P3 Video P-frames base spatial | 256 | tail-drop oldest | non-keyframe, `spatial=0` |
| P4 Enhancement + screen | 256 | head-drop oldest | `spatial>0` or `temporal>0 & spatial>0`; screen-share |

Consumer in `webtransport/bridge.rs`: strict priority order with **fairness quantum** — after 8 packets from a higher class, peek the next class to prevent starvation. Classification uses the already-parsed `PacketWrapper` at `wt_chat_session.rs:333-338` plus the `RoutingHeader`; no second parse.

`CongestionTracker` (`actix-api/src/actors/session_logic.rs:77-155`) gains `record_drop_with_class`: P2 triggers `CONGESTION` after 1 drop (urgent); P4 keeps current 5-drops/1s threshold.

Worst-case audio scheduling latency = P0 queue × per-packet wire time ≈ 4ms.

**Exit:** synthetic test — 1 sender bursting 10MB video to a 1Mbps receiver — audio loss <0.1% while video loss rises smoothly. No HOL block on audio.

**Files:** new `actix-api/src/sfu/priority_queue.rs`; modify `actix-api/src/webtransport/{mod.rs, bridge.rs}`, `actix-api/src/actors/transports/{wt_chat_session.rs, ws_chat_session.rs}`, `actix-api/src/actors/session_logic.rs`.

### Phase 6 — Room-affinity routing + capacity validation (3–5 days)
- Consistent-hash `room_id` → pod ordinal via `jump_hash`. Inputs: `STATEFULSET_REPLICAS` and `POD_NAME` from K8s downward API. Migrate Deployment → StatefulSet so ordinals are stable.
- Routing flow: client connects to lowest-RTT pod (existing RTT election unchanged). If `pod_ordinal != owner(room_id)`, server responds with `ADMISSION_DECISION { redirect_to: "webtransport-{owner}.webtransport-headless.svc:443" }` and closes. Client reconnects.
- Spillover: each pod publishes 5s health beacons on `room.{room}.system` with `(participant_count, cpu_load)`. If owner reports `count > 180` or `cpu > 80%`, ring marks room `SpilledOver`; new joiners admitted to spill pods. Spill pods federate over NATS as today (transparent because they already subscribe to `room.{room}.*`).
- Cross-region: each region's StatefulSet hashes locally. Rooms have a "home region" set by first joiner. Out-of-region clients get redirected (250ms RTT penalty — accepted for v1; revisit per §K risk #2).
- Failover: pod death → K8s restart. Clients reconnect via existing connection-closed handlers; 5–15s receiver downtime.
- **Exit:** 200-bot load test against 2-pod deployment shows the room pinned to one pod; killing the owner causes redirect-to-survivor with <15s downtime.

**Files:** new `actix-api/src/sfu/affinity.rs`; modify `actix-api/src/bin/{webtransport,websocket}_server.rs`; new `helm/rustlemania-webtransport/templates/statefulset.yaml` (replaces Deployment); modify `helm/rustlemania-webtransport/{values.yaml,templates/service.yaml}` and the WebSocket equivalents.

---

## New Wire Surface (consolidated)

`MediaPacket` (additive, all proto3 optional):
```
message RoutingHeader {
  bool is_keyframe = 1;
  uint32 temporal_layer_id = 2;       // 0=base, 1..N=enhancement
  uint32 spatial_layer_id = 3;
  float audio_level = 4;              // 0..1 RMS, AUDIO only
  bool is_speaking = 5;               // VAD/threshold hint
  uint32 frame_marker = 6;            // bitfield: START_OF_FRAME=1, END_OF_FRAME=2, REFERENCES_T0=4
  uint64 picture_id = 7;              // for SVC dependency tracking
}
```

`PacketWrapper.PacketType` additions:
```
SUBSCRIPTION_UPDATE = 10;
SPEAKER_UPDATE = 11;
LAYER_HINT = 12;
ADMISSION_DECISION = 13;
CAPABILITY_ANNOUNCE = 14;
```

New control packets:
```
message SubscriptionUpdate {
  repeated uint64 pinned_sessions = 1;
  repeated VisibilitySlot slots = 2;
  uint32 max_video_kbps = 3;
  bool receive_all_audio = 4;            // v1 default true
}
message VisibilitySlot {
  uint64 session_id = 1;
  uint32 preferred_spatial = 2;
  uint32 preferred_temporal = 3;
}
message SpeakerUpdate {
  repeated SpeakerEntry top_speakers = 1;
  uint64 generation = 2;
}
message SpeakerEntry { uint64 session_id = 1; float score = 2; bool is_speaking = 3; }
```

`CONNECTION` packet gains `client_capabilities` (bits: `SFU_ROUTING_HEADER=1`, `SVC=2`, `SUBSCRIPTION=4`). Forwarder consults capabilities per-receiver: legacy clients keep getting the full fanout for that receiver only.

---

## Critical Files (single-list reference)

Existing files that anchor the work:
- `actix-api/src/actors/chat_server.rs` (`handle_msg` at chat_server.rs:784; `JoinRoom` at chat_server.rs:560-765)
- `actix-api/src/actors/session_logic.rs` (`CongestionTracker` at session_logic.rs:77-155)
- `actix-api/src/actors/transports/wt_chat_session.rs` (`Handler<Message>` at wt_chat_session.rs:324; classification surface at wt_chat_session.rs:333-352)
- `actix-api/src/webtransport/mod.rs` (line 351 — the mpsc(256) being replaced)
- `actix-api/src/actors/packet_handler.rs` (`classify_packet`; rate limits at packet_handler.rs:115-143)
- `protobuf/types/media_packet.proto`, `packet_wrapper.proto`, `connection_packet.proto`
- `videocall-client/src/encode/camera_encoder.rs` (encoder config + header populate)
- `videocall-client/src/encode/microphone_encoder.rs` (audio level + RED framing)
- `videocall-client/src/decode/peer_decode_manager.rs` (subscription emission, layer-aware decode)
- `videocall-client/src/client/video_call_client.rs` (set_peer_visibility at video_call_client.rs:849; congestion handling at 1364-1381)
- `neteq/src/neteq.rs` (jitter buffer — verify graceful handling of layer-dropped reordered streams)
- `helm/rustlemania-webtransport/`, `helm/rustlemania-websocket/` (StatefulSet migration for affinity)

Existing utilities to reuse rather than reinvent:
- `HeartbeatMetadata.is_speaking` — already an active-speaker hint; promote it from heartbeat-only to per-AUDIO-packet (via `RoutingHeader.is_speaking`)
- `PacketWrapper::CONGESTION` and the client's `congestion_step_down` flag — existing mechanism; the SFU just emits CONGESTION more intelligently (per-class thresholds)
- `KEYFRAME_REQUEST` rate-limit at packet_handler.rs:115 — keep; the SFU adds layer-aware routing, not a new mechanism
- `DiagnosticsPacket` (`videocall-client/src/diagnostics/`) — existing per-receiver feedback; extend `video_metrics` with a `BandwidthEstimate` field in phase 4
- ConnectionManager's RTT election (`videocall-client/src/connection/connection_manager.rs:182-219`) — unchanged; affinity redirects happen *after* RTT election picks a pod
- `bot/` — already a headless Rust WebTransport client; extend for load testing rather than building a new harness

---

## Capacity Model (200-participant webinar)

Per-pod inbound (room owner): 10 senders × 800 kbps video + 200 audio × 32 kbps = **14.4 Mbps**.

Per-pod outbound (forwarding to all 200 receivers, top-6 video each + all audio):
- Per receiver = 6 × 400 kbps + 200 × 32 kbps = 2.4 + 6.4 = **8.8 Mbps**
- Total = 200 × 8.8 Mbps = **1.76 Gbps** — the binding constraint.

This requires either 2+ pods per room *or* audio mixdown (200 → 1 stream, dropping per-receiver out to ~2.5 Mbps, total ~500 Mbps on one pod). Mixdown breaks E2EE; flagged as Open Risk #1.

mpsc backlog: 5-class priority queue × ~256 slots × 1500B ≈ 1 MB per session × 200 sessions = 200 MB RAM. Fine on 8GB nodes.

Burst behavior: a 1.5MB keyframe = ~1250 chunks blows P2's 128 slots → tail-drop within the frame → KEYFRAME_REQUEST + ~500ms recovery. Acceptable for webinar; not for conference shape.

Breaks at: ~250 receivers (egress) or ~30 senders (inbound) per pod. Webinar shape is ~20× easier than conference shape.

Full numbers in `/sfu-update/capacity-model.md`.

---

## Open Risks (escalate before each phase)

1. **Audio: forward-all vs server mixdown.** Mixdown breaks E2EE. Plan assumes forward-all for v1; mixdown deferred to a separate "town hall" mode (Open Risk → ADR `0006-audio-mixdown.md` in phase 3).
2. **Cross-region cost.** At 30% remote mix, ~$200/hour cross-region bandwidth. v1 pins rooms to home region; revisit at scale.
3. **Conference-shape (30–50 senders) upgrade path.** Capacity model breaks at ~30 senders. Hooks are present (`layer_selector`, `SubscriptionUpdate.slots`); explicit conference-shape RFC follows v1.
4. **Admission control at 200.** Need soft cap at 195 + 5-slot waiting room using existing observer mode. Wire into phase 3.
5. **Observability.** No phase explicitly defines metrics. Add Prometheus counters (`sfu_forwarded`, `sfu_dropped_{budget,unsubscribed,layer}`), gauges (`sfu_room_size`, `sfu_speaker_changes_per_min`), histogram (`sfu_decide_latency_us`) in phase 2 via `actix-api/src/metrics.rs`.
6. **Recording bots.** Add capability bit `IS_RECORDER` → forwarder bypasses layer dropping and `max_visible_video` cap.
7. **Cross-region speaker detection consistency.** When a room spills across pods, only the owner pod computes the speaker set; spill pods consume `SpeakerUpdate` from `room.{room}.system`.
8. **VP9 SVC browser support.** Chromium M111+ ok; verify Safari 18.2 (WebTransport ships) can render dropped-layer SVC bitstreams. Add to test matrix.

---

## Verification

End-to-end:
1. After Phase 0: `SFU_MODE=legacy` and `SFU_MODE=sfu` both boot; existing `start_dev.sh` flow works for both.
2. After Phase 1: legacy client + new client coexist in same room; new client emits `RoutingHeader`; visible in server logs; `cargo clippy -D warnings` clean.
3. After Phase 2: forwarder pass-through parity — golden trace test asserts every legacy-path packet is also delivered by the SFU path; integration test in `actix-api/src/sfu/tests/`.
4. After Phase 3: 12-client demo. Listeners receive only speaker-set + pins. Speaker rotation visible within 500ms of `is_speaking` change. `e2e/tests/sfu-speaker-rotation.spec.ts` covers UI assertion.
5. After Phase 4: throttle a receiver to 500kbps with `network_throttle.py`; verify base+T0 only; lift throttle and verify upgrade within 2s without thrash.
6. After Phase 5: synthetic burst test (10MB video burst into a 1Mbps receiver) — audio loss <0.1%.
7. After Phase 6: 200-bot load test (extend `bot/` to drive shape). Kill the owner pod; surviving pod accepts redirected joiners within 15s.

Per-phase gates:
- Unit: `cargo test -p videocall-actix-api --features sfu`
- Lint: `cargo fmt --check && cargo clippy -- -D warnings` (matches existing precommit/postcommit hooks)
- Smoke: 50-bot, 5-minute test must complete with <0.5% audio loss. Wire as CI nightly with the 200-bot test as a release gate.
- Conventional Commits format (project convention); one commit per phase exit.

Inbound queuing — sanity check: confirm with profiling on phase 2 that inbound mailboxes don't grow unbounded under 200-client load. If they do, escalate via `0007-inbound-queue-bounds.md`. Current plan intentionally leaves inbound discipline unchanged.

---

## Gastown DAG per Phase

Each phase becomes a convoy that `tracks` its leaf beads. Dependencies use `blocks` for hard sequencing and `parent-child` only for organizational grouping (parent-child is non-blocking — the convoy daemon dispatches children of an open epic just fine). Bead **types** follow the gastown slingable set: `task`, `bug`, `feature`, `chore` are dispatchable; `decision` and `epic`/`sub-epic` are not. Convoy beads themselves are the trackers.

**Cross-phase edges:** `P{N+1}` convoy `waits-for` `P{N}` convoy close. ADRs from earlier phases are `parent-child` linked to relevant tasks in later phases (non-blocking, just traceability).

**Bootstrap before P0:** create the umbrella epic `sfu-epic [epic]` and the six convoys `P0..P6 [convoy]`. The umbrella epic is `parent-child` of every leaf. Run `gt convoy stage sfu-epic` to materialize waves; `gt convoy stage <P_N>` to refresh a phase if its DAG changes.

Legend below: `[type] id "summary" → deps: blocks list`. Where dependency lists get long, deps are summarized as wave membership. "Wave N" means everything in Wave N can be slung in parallel once Wave N-1 is closed.

---

### Convoy P0 — Decision substrate & feature flag

| Bead | Type | Summary |
| --- | --- | --- |
| p0-1 | chore | Create `/sfu-update/` tree (README, capacity-model.md, packet-diagrams.md, test-matrix.md, adr/) and copy this plan into `sfu-update/PLAN.md` |
| p0-2 | task | Author `/rfc/rfc-2-sfu-architecture.md` umbrella RFC |
| p0-3 | decision | ADR-0001 routing-header-out-of-encryption |
| p0-4 | decision | ADR-0002 active-speaker-detection |
| p0-5 | decision | ADR-0003 hybrid-subscription-model |
| p0-6 | decision | ADR-0004 outbound-priority-queue |
| p0-7 | decision | ADR-0005 room-affinity-routing |
| p0-8 | task | Fill out `capacity-model.md` (§J) |
| p0-9 | task | Fill out `packet-diagrams.md` (sequence diagrams for the new packet types) |
| p0-10 | task | Fill out `test-matrix.md` (codec × browser × shape) |
| p0-11 | feature | Add `actix-api/src/sfu/{mod.rs, config.rs}` and `SFU_MODE` env parsing |
| p0-12 | feature | Wire `SFU_MODE` into `bin/webtransport_server.rs` |
| p0-13 | feature | Wire `SFU_MODE` into `bin/websocket_server.rs` |
| p0-14 | task | Unit test asserting flag plumbing on both binaries |

Edges:
- p0-1 blocks: everything else in P0
- p0-2 blocks: p0-3..p0-7 (RFC frames the ADRs)
- p0-11 blocks: p0-12, p0-13, p0-14
- p0-12, p0-13 both block: p0-14

Waves:
- W1: p0-1
- W2: p0-2, p0-11
- W3: p0-3, p0-4, p0-5, p0-6, p0-7, p0-8, p0-9, p0-10, p0-12, p0-13
- W4: p0-14 ← convoy close gate

---

### Convoy P1 — Wire protocol: routing header + new packet types

`waits-for`: P0.

| Bead | Type | Summary |
| --- | --- | --- |
| p1-1 | feature | `RoutingHeader` submessage + field 10 on `media_packet.proto` |
| p1-2 | feature | New `PacketType` enum values on `packet_wrapper.proto` |
| p1-3 | feature | `client_capabilities` bitmask on `connection_packet.proto` |
| p1-4 | feature | New `subscription_packet.proto` |
| p1-5 | feature | New `speaker_update_packet.proto` |
| p1-6 | chore | Regenerate prost bindings; rebuild `videocall-types` |
| p1-7 | feature | Client: populate `RoutingHeader` from VideoEncoder chunk metadata in `camera_encoder.rs` |
| p1-8 | feature | Client: compute `audio_level` RMS pre-Opus in `microphone_encoder.rs` |
| p1-9 | feature | Client: passthrough `RoutingHeader` in `screen_encoder.rs` |
| p1-10 | feature | Client: emit `client_capabilities` on CONNECTION in `connection_manager.rs` |
| p1-11 | feature | Server: parse-and-log `RoutingHeader` in `packet_handler.rs` |
| p1-12 | task | Integration test: legacy + new client coexist in one room |
| p1-13 | task | `cargo fmt --check && cargo clippy -- -D warnings` clean |

Edges:
- p1-1..p1-5 all block p1-6
- p1-6 blocks p1-7..p1-11
- p1-7..p1-11 all block p1-12
- p1-12 blocks p1-13

Waves:
- W1: p1-1, p1-2, p1-3, p1-4, p1-5
- W2: p1-6
- W3: p1-7, p1-8, p1-9, p1-10, p1-11
- W4: p1-12
- W5: p1-13 ← convoy close gate

---

### Convoy P2 — SFU forwarder module (pass-through)

`waits-for`: P1.

| Bead | Type | Summary |
| --- | --- | --- |
| p2-1 | feature | Module skeleton `actix-api/src/sfu/{forwarder.rs, room_state.rs, subscription.rs, speaker.rs, layer_selector.rs}` (stubs) |
| p2-2 | feature | `RoomState` data model with member table, capabilities cache |
| p2-3 | feature | `Forwarder::decide` pass-through implementation |
| p2-4 | feature | Expose `RoutingHeader` to forwarder via `session_logic.rs::OutboundDecision` |
| p2-5 | feature | Hook forwarder into `chat_server.rs::handle_msg` behind `SFU_MODE` |
| p2-6 | feature | Wire `JoinRoom`/`LeaveRoom` to maintain `RoomState.members` |
| p2-7 | task | Add Prometheus counters (`sfu_forwarded`, `sfu_dropped_*`), gauge (`sfu_room_size`), histogram (`sfu_decide_latency_us`) in `actix-api/src/metrics.rs` |
| p2-8 | task | Golden trace parity test: legacy vs sfu paths produce identical fan-out |
| p2-9 | task | Integration test: SFU_MODE=sfu on 1:1 and 1:N rooms, matches legacy delivery |
| p2-10 | decision | ADR-0006 audio-mixdown-deferred (Open Risk #1) |

Edges:
- p2-1 blocks p2-2, p2-3
- p2-2 blocks p2-3, p2-4
- p2-3 blocks p2-5, p2-7, p2-8
- p2-4 blocks p2-5
- p2-5 blocks p2-6, p2-8, p2-9
- p2-7 blocks p2-9
- p2-8 blocks p2-9

Waves:
- W1: p2-1, p2-10
- W2: p2-2
- W3: p2-3, p2-4
- W4: p2-5, p2-7
- W5: p2-6, p2-8
- W6: p2-9 ← convoy close gate

---

### Convoy P3 — Active speaker + subscription model

`waits-for`: P2.

| Bead | Type | Summary |
| --- | --- | --- |
| p3-1 | feature | `SpeakerScorer` EWMA (α=0.3) per-sender scoring |
| p3-2 | feature | 200ms tick; entry/exit hysteresis (0.05/0.05 over 200/800ms); top-N=4; generation counter |
| p3-3 | feature | Publish `SpeakerUpdate` to `room.{room}.system` on generation change |
| p3-4 | feature | `Subscription` table + `resolve(receiver) → AllowSet { audio, video: layer_pref }` |
| p3-5 | feature | Forwarder consults AllowSet (per-receiver) for forward/drop |
| p3-6 | feature | Client: handle inbound `SpeakerUpdate` → speaker tile UI in `peer_decode_manager.rs` |
| p3-7 | feature | New `videocall-client/src/sfu_client.rs` to emit `SubscriptionUpdate` |
| p3-8 | feature | Wire `set_peer_visibility` and tile-pin UI to `sfu_client` emit |
| p3-9 | task | Unit: speaker scoring, hysteresis, generation idempotency |
| p3-10 | task | Unit: subscription reconciliation matrix (pin, slot, stale, pre-join, oversize) |
| p3-11 | task | Integration: 12-client demo with rotating speaker |
| p3-12 | task | `e2e/tests/sfu-speaker-rotation.spec.ts` |
| p3-13 | feature | Admission control soft cap (195 + 5-slot waiting) using existing observer mode (Open Risk #4) |

Edges:
- p3-1 blocks p3-2
- p3-2 blocks p3-3, p3-9
- p3-3 blocks p3-5, p3-6, p3-11
- p3-4 blocks p3-5, p3-10
- p3-5 blocks p3-11
- p3-7 blocks p3-8
- p3-8 blocks p3-11
- p3-11 blocks p3-12

Waves:
- W1: p3-1, p3-4, p3-7, p3-13
- W2: p3-2, p3-8
- W3: p3-3, p3-5, p3-9, p3-10
- W4: p3-6, p3-11
- W5: p3-12 ← convoy close gate

---

### Convoy P4 — VP9 SVC + per-receiver layer dropping

`waits-for`: P3.

| Bead | Type | Summary |
| --- | --- | --- |
| p4-1 | feature | Client encoder: WebCodecs `scalabilityMode: "L1T3"` |
| p4-2 | feature | Client encoder: extract temporal/spatial ids + frame_marker from chunk metadata |
| p4-3 | feature | Add `BandwidthEstimate` field to `DiagnosticsPacket` |
| p4-4 | feature | Server: expose receiver bandwidth estimate to forwarder (`client_diagnostics.rs`) |
| p4-5 | feature | `LayerSelector::pick_layers` greedy two-pass |
| p4-6 | feature | Hysteresis: upgrade watchdog (20% headroom × 3s) + downgrade cooldown (5s) |
| p4-7 | feature | Forwarder layer-drop using `RoutingHeader.temporal_layer_id`, `spatial_layer_id` |
| p4-8 | feature | Invariant: always forward `is_keyframe && T0 && S0` |
| p4-9 | feature | Invariant: REFERENCES_T0 frames only dropped if their T0 picture_id was also dropped |
| p4-10 | feature | Layer-aware `KEYFRAME_REQUEST` routing (don't blast a 1.5Mbps keyframe to a 200kbps receiver) |
| p4-11 | feature | Client decoder: accept variable-rate streams; smarter KFR emission |
| p4-12 | task | Unit: layer selector + hysteresis under bouncing bandwidth (no thrash) |
| p4-13 | task | Integration: throttled receiver scenario via `network_throttle.py` |
| p4-14 | task | Browser matrix: Chromium SVC verified; Safari 18.2 dropped-layer rendering verified |

Edges:
- p4-1 blocks p4-2
- p4-3 blocks p4-4
- p4-4 blocks p4-5
- p4-2 blocks p4-7
- p4-5 blocks p4-6, p4-7
- p4-7 blocks p4-8, p4-9, p4-10, p4-11
- p4-6 blocks p4-12
- p4-8, p4-9, p4-10, p4-11 all block p4-13
- p4-13 blocks p4-14

Waves:
- W1: p4-1, p4-3
- W2: p4-2, p4-4
- W3: p4-5
- W4: p4-6, p4-7
- W5: p4-8, p4-9, p4-10, p4-11, p4-12
- W6: p4-13
- W7: p4-14 ← convoy close gate

---

### Convoy P5 — Outbound priority queue + class-aware drop

`waits-for`: P4.

| Bead | Type | Summary |
| --- | --- | --- |
| p5-1 | feature | `PrioritySender` 5-class wrapper around inner bounded `mpsc`s |
| p5-2 | feature | Consumer: strict priority order with 8-packet fairness quantum |
| p5-3 | feature | Classification fn from `PacketWrapper` + `RoutingHeader` → class |
| p5-4 | feature | Replace `mpsc(256)` at `webtransport/mod.rs:351` with `PrioritySender` |
| p5-5 | feature | Replace WS analog channel with `PrioritySender` |
| p5-6 | feature | `CongestionTracker::record_drop_with_class` + per-class thresholds (P2:1, P4:5) |
| p5-7 | feature | Wire `Full` returns from both transports into class-aware `CongestionTracker` |
| p5-8 | task | Unit: priority ordering, fairness quantum (no starvation), drop policies per class |
| p5-9 | task | Synthetic burst test: 10MB video burst into 1Mbps receiver → audio loss <0.1% |
| p5-10 | task | Prometheus per-class drop counters added to `metrics.rs` |

Edges:
- p5-1 blocks p5-2, p5-4, p5-5
- p5-2 blocks p5-4, p5-5, p5-8
- p5-3 blocks p5-4, p5-5
- p5-6 blocks p5-7
- p5-4, p5-5 both block p5-7
- p5-7 blocks p5-9, p5-10

Waves:
- W1: p5-1, p5-3, p5-6
- W2: p5-2
- W3: p5-4, p5-5, p5-8
- W4: p5-7
- W5: p5-9, p5-10 ← convoy close gate

---

### Convoy P6 — Room affinity + capacity validation

`waits-for`: P5.

| Bead | Type | Summary |
| --- | --- | --- |
| p6-1 | feature | `jump_hash(room_id) → pod_ordinal` in `actix-api/src/sfu/affinity.rs` |
| p6-2 | feature | Helm: migrate webtransport Deployment → StatefulSet (stable hostnames) |
| p6-3 | feature | Helm: migrate websocket Deployment → StatefulSet |
| p6-4 | feature | Wire `POD_NAME` + `STATEFULSET_REPLICAS` env via K8s downward API |
| p6-5 | feature | `ADMISSION_DECISION` redirect emission on owner mismatch |
| p6-6 | feature | Client: handle redirect in `ConnectionManager` (reconnect to named pod) |
| p6-7 | feature | Owner pod health beacon every 5s on `room.{room}.system` |
| p6-8 | feature | Spillover acceptance logic (soft cap 180 participants / 80% cpu) |
| p6-9 | feature | Cross-region: home-region pinning + `region_hint` URL param |
| p6-10 | task | Extend `bot/` to drive 200-bot load test (`--room R --senders 10 --listeners 190 --duration 300s`) |
| p6-11 | task | Pod-kill failover test (assert <15s receiver downtime) |
| p6-12 | task | Update `sfu-update/capacity-model.md` with measured numbers |
| p6-13 | task | CI: nightly 200-bot test as release gate; 50-bot 5-min smoke as merge gate |

Edges:
- p6-2, p6-3 both block p6-4
- p6-1 blocks p6-5, p6-7
- p6-4 blocks p6-5
- p6-5 blocks p6-6
- p6-6 blocks p6-9
- p6-7 blocks p6-8
- p6-5, p6-6, p6-8 all block p6-10
- p6-10 blocks p6-11, p6-12, p6-13

Waves:
- W1: p6-1, p6-2, p6-3
- W2: p6-4, p6-7
- W3: p6-5, p6-8
- W4: p6-6
- W5: p6-9, p6-10
- W6: p6-11, p6-12, p6-13 ← convoy close gate

---

### Cross-cutting decision beads (linked across phases)

These decision beads exist independently and are `parent-child` linked to the tasks that depend on them. They don't block dispatch (parent-child is non-blocking in gastown) but provide traceability:

- ADR-0001 (p0-3) → parent of p1-1 (RoutingHeader proto), p1-7..p1-9 (client populate), p4-7..p4-11 (layer drop)
- ADR-0002 (p0-4) → parent of p3-1, p3-2, p3-3 (speaker detection)
- ADR-0003 (p0-5) → parent of p3-4, p3-5, p3-7, p3-8 (subscription)
- ADR-0004 (p0-6) → parent of p5-1..p5-7 (priority queue)
- ADR-0005 (p0-7) → parent of p6-1, p6-5, p6-7..p6-9 (affinity)
- ADR-0006 (p2-10) → parent of any future audio-mixdown work (not in v1)

### Convoy launch protocol

For each phase, when the prior phase closes:
1. `gt convoy stage P{N}` — validates DAG, computes waves, reports staged:warnings or staged:ready.
2. Review wave output; resolve any warnings (stale deps, missing parents).
3. `gt convoy launch P{N}` — transitions to open, Wave 1 slung.
4. Daemon's event-driven feeder picks up close events and dispatches next-ready beads automatically. Stranded scan catches any missed dispatches every 30s.
5. Convoy auto-closes when all tracked beads close (final wave is the close gate).

If a phase's DAG changes mid-execution (a bead is added/removed), re-run `gt convoy stage P{N}` to re-validate; the daemon handles the re-staged convoy idempotently.

---

## Out of Scope for v1

- Conference shape (30–50 active video senders) — capacity model breaks; needs publish-side filtering and possibly simulcast in addition to SVC.
- Server-side audio mixdown — incompatible with strict E2EE; needs a "town hall" mode with relaxed crypto.
- AV1 / H.264 alternatives — VP9 SVC only.
- Recording infrastructure — only capability bit and forwarder special-case; no recording bot built.
- Cross-region active-speaker consistency for spilled rooms — owner pod computes; spill pods consume.
