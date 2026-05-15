# SFU Refactor — Polecat Fan-out Strategy

**Audience:** Mayor (and Witness, when it decides where to dispatch).
**Purpose:** Per-phase polecat allocation so the convoy daemon's `feedNextReadyIssue` lands work on a polecat with the right skill profile.

---

## Capability profiles

The videocall rig polecats are typed by capability label. The Mayor sets the `--account` and `--var capability=...` on the rig config so `gt sling --formula mol-polecat-work --var capability=...` resolves the right polecat. For phase 0 we used the default `mol-polecat-work` formula; later phases may use specialised formulas in `/gt/videocall/.beads/formulas/`.

| Label | Skills | Used in phases |
| --- | --- | --- |
| `rust-backend` | cargo/clippy/protoc, tokio/actix, serde, prost | P0–P6 (everywhere on server) |
| `frontend-web` | TypeScript, WebCodecs, Rust→Wasm (videocall-client), browser testing | P1, P3, P4 (encoder + decoder + UI) |
| `playwright-e2e` | Playwright, Chromium/Safari driving | P3, P6 |
| `helm-k8s` | Helm v3, kubectl, K8s downward API, StatefulSet semantics | P6 |
| `load-test` | extend `bot/` for headless load, network shaping | P4, P5, P6 |
| `generalist` | Markdown/RFC authoring, capable across the above for small tasks | P0 (ADRs), every phase for chore beads |

For each phase, declare the **pool size** and **required labels** below. `gt polecat pool-init` runs at the start of each phase to grow the pool to that size. Existing idle polecats are preserved.

---

## Per-phase fan-out

### P0 — Decision substrate (currently running)

- Pool size: 2 (furiosa, nux — already created).
- Profile: 1 `generalist` (ADR drafting), 1 `rust-backend` (the SfuMode env + binary wiring at p0-11..p0-14).
- Slingable work: 9 beads. Decisions (5) authored by-hand or `--review-only` after task beads done.
- Sequencing recommendation: **let Wave 1 land before further pool growth** — the scaffold (p0-1) is small and serial.

### P1 — Wire protocol

- Pool size: 3.
- Profile: 1 `rust-backend` (proto edits + server log-and-pass-through), 2 `frontend-web` (client encoder header populate is the long pole: camera_encoder.rs, microphone_encoder.rs, screen_encoder.rs).
- Sequencing: Wave 1 = parallel proto edits (5 beads can fan out across 1–2 polecats — protos are mostly disjoint files; merges may need ordering, see "Conflict-prone areas" below).
- Special: introduce `frontend-web` polecats explicitly before launching this phase: `gt polecat identity add videocall frontend-web-1`.

### P2 — SFU forwarder module (pass-through)

- Pool size: 3.
- Profile: 2 `rust-backend`, 1 `generalist` (metrics + integration tests).
- Sequencing: All beads land in `actix-api/src/sfu/*` and `actix-api/src/actors/chat_server.rs`. **High conflict risk** — sling sequentially, not parallel. Tag the convoy with `--max-concurrent 1` so the daemon serialises within waves.

### P3 — Active speaker + subscription model

- Pool size: 4.
- Profile: 2 `rust-backend` (speaker.rs, subscription.rs, forwarder.rs), 1 `frontend-web` (sfu_client.rs + peer_decode_manager.rs visibility wiring), 1 `playwright-e2e` (e2e/tests/sfu-speaker-rotation.spec.ts).
- Watch: p3-3 (publish SpeakerUpdate) and p3-5 (forwarder consults AllowSet) both touch forwarder.rs. Sling these to the same polecat or serialise.

### P4 — VP9 SVC + per-receiver layer dropping

- Pool size: 5.
- Profile: 2 `frontend-web` (encoder config + chunk metadata + decoder multi-rate), 2 `rust-backend` (layer_selector.rs + forwarder layer drop + bandwidth estimate exposure), 1 `load-test` (throttle scenario).
- Watch: encoder behaviour is the highest-risk surface. Reserve a senior `frontend-web` polecat for camera_encoder.rs changes.

### P5 — Outbound priority queue

- Pool size: 3.
- Profile: 2 `rust-backend` (priority_queue.rs + webtransport bridge + CongestionTracker), 1 `load-test` (synthetic burst test).
- Watch: webtransport/mod.rs and wt_chat_session.rs both edited. **Serialise within waves** (`--max-concurrent 1`).

### P6 — Room affinity + capacity validation

- Pool size: 4.
- Profile: 1 `rust-backend` (affinity.rs + binary wiring), 1 `helm-k8s` (StatefulSet migration in helm/), 1 `frontend-web` (ConnectionManager redirect handling), 1 `load-test` (200-bot harness).
- Watch: Helm migration is the highest-risk operational surface. Recommend the `helm-k8s` polecat be a senior one with rollback discipline.

---

## Conflict-prone areas (sequence within wave, not parallel)

| Files | Phases | Strategy |
| --- | --- | --- |
| `actix-api/src/actors/chat_server.rs` | P2, P3 | One polecat at a time. Convoy `--max-concurrent 1`. |
| `actix-api/src/actors/session_logic.rs` | P2, P5 | One polecat at a time. |
| `videocall-client/src/encode/camera_encoder.rs` | P1, P4 | One polecat at a time across phases (P1 then P4). |
| `actix-api/src/sfu/forwarder.rs` | P2, P3, P4 | One polecat at a time. |
| `helm/rustlemania-{webtransport,websocket}/*` | P6 | One `helm-k8s` polecat owns this end-to-end. |

---

## Dispatch overrides (per-bead)

For beads that need a specific polecat (e.g., the `helm-k8s` polecat for p6-2), use:

```bash
gt sling <bead-id> videocall/<polecat-name>
```

For beads that need a specific formula (e.g., review-only for decision beads):

```bash
gt sling <decision-bead-id> videocall/<generalist> --review-only --formula mol-adr-author
```

For pool-mode (let the daemon pick any idle polecat in the right pool):

```bash
gt sling <bead-id> videocall
```

For deferred dispatch (let scheduler hold until capacity exists):

```bash
gt config set scheduler.max_polecats 5
# then sling normally; scheduler delays dispatch until pool has slack
```

---

## What the Mayor should NOT do without overseer ack

- Push to `origin` (any branch). Local-only is the contract.
- Force-merge a polecat branch that the Refinery rejected for conflicts. Conflicts mean PLAN.md drift; escalate.
- Materialise P1+ before the overseer confirms P0 close (see [SCALE-UP.md](./SCALE-UP.md) "Standing rules" #1).
- Grow the polecat pool beyond profile sizes above without an explicit reason — extra polecats burn disk via worktrees and compete for capacity-controlled scheduler slots.
