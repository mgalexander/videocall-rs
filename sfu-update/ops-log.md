# sfu-update Operations Log

Chronological log of bootstrap and convoy-execution events. Append-only.

Format: `YYYY-MM-DD HH:MM TZ  ACTOR  EVENT  DETAIL`

---

## 2026-05-15 Bootstrap

- baseline disk: `/dev/mapper/data-root 908G / 596G used / 266G avail / 70% used` — well under 80% soft alert and 85% halt threshold.
- baseline docker: `Images 107.7GB (73.43GB reclaimable), Build cache 4.77GB, Local volumes 1.738GB`.
- B0 verified `gastown-sandbox` running (up ~2 days hosting `lps-`, `imap-` rigs). Existing bind mounts: labs-pim-server, labs-imap-server, /gt → /mnt/llms/gas-town/town.
- B1 added `/mnt/llms/videocall:/mnt/llms/videocall` bind mount to `/mnt/llms/gas-town/docker-compose.override.yml`; `docker compose up -d gastown` recreated the container cleanly.
- Verified: `docker exec gastown-sandbox ls /mnt/llms/videocall` returns repo tree; `gt --version` returns `gt version dev`; `bd` found at `/usr/local/bin/bd`.
- Created branch `experimental-sfu` from `c01a773 (helm: fix webtransport version URL to use LB service (#827))`.
- Bootstrap commit `183fc53 chore(sfu): bootstrap sfu-update planning tree and convoy manifest` on experimental-sfu.
- Enrolled rig with `gt rig add videocall file:///mnt/llms/videocall --prefix vc --branch experimental-sfu`. Rig at `/gt/videocall/` with mayor/rig and refinery/rig clones on experimental-sfu. routes.jsonl gets `{"prefix":"vc-","path":"videocall"}`. rigs.json adds videocall entry with git_url `file:///mnt/llms/videocall`.
- Ran `bd bootstrap` (rig db needed manual init after `gt rig add`'s prefix-set warning).
- Mayor primed and started; durable handoff mail sent with full PLAN.md body (pinned, permanent, priority 1).
- Materialised: epic `vc-c4e`, 14 beads `vc-c4e.{1..12, 14, 16}` (3 dupes `.13/.15/.17` closed as the regex bug iteration), 22 blocking edges, convoy `hq-cv-upyye` (original) plus staged convoy `hq-cv-i4w2x` (4 waves, 9 dispatchable tasks — 5 decision-type ADRs are non-slingable, by design).
- Launched `hq-cv-i4w2x`. Wave 1 = vc-c4e.1.
- Provisioned 2-polecat pool via `gt polecat pool-init videocall --size 2` → furiosa + nux (Mad Max theme).
- `gt scheduler run` dispatched vc-c4e.1 to furiosa. Session started; daemon started (PID 27401).
- Furiosa working in `/gt/videocall/polecats/furiosa/videocall/` on branch `polecat/furiosa/vc-c4e.1@mp6w5df2` (forked from 183fc53).

## Known issues / one-off corrections during bootstrap

- `gt rig add` printed: `Warning: Could not set issue_prefix on rig database`. Resolved by `bd bootstrap` from `/gt/videocall`.
- `gt rig add` printed: `Warning: Town root is on branch 'experimental-sfu'` — false alarm; gt was reading the cwd's branch, not the actual town root at `/gt`.
- `gt init` from `/mnt/llms/videocall` created mayor/, refinery/, polecats/, witness/, crew/ — orphaned because the rig actually lives under `/gt/videocall/`. Removed; `.git/info/exclude` cleaned of the agent-directory entries.
- materialize.py regex initially matched any `vc-*` substring, picking up auto-import log noise. Rewrote to anchor on `Created issue:` / `Created convoy:`. 3 duplicate beads `vc-c4e.{13,15,17}` were closed via `bd close --reason "duplicate from regex bug"`.
- `bd` reports `Warning: auto-export: git add failed` on every command because `.beads/` is gitignored — non-fatal, expected (bead state stays out of code commits).
- `SetAgentState attempt N failed, retrying ... issue not found` warnings during `gt polecat pool-init` and session start. Non-fatal so far (furiosa is `working`), but worth watching.
- Furiosa's first session ran `gt prime --hook`, saw an empty hook (the scheduler sling produced a wisp but never landed the bead on the polecat's hook), and self-deferred with `gt done --status DEFERRED`. Root cause: identity beads weren't created (the `SetAgentState` warnings during pool-init were not "non-fatal" — they meant the polecat identities never landed).
- Recovery: `gt polecat identity add videocall furiosa` + `gt polecat identity add videocall nux` → created identity beads `vc-videocall-polecat-furiosa` and `vc-videocall-polecat-nux`. Then `gt hook vc-c4e.1 videocall/polecats/furiosa` attached the bead directly. Then `gt session restart videocall/furiosa` cycled furiosa's tmux session. Furiosa primed again, saw the hook, and started actual work (thinking → executing).
- **Lesson for future phases:** after `gt polecat pool-init`, verify identity beads exist via `gt polecat identity list videocall`. If empty, create them by hand BEFORE slinging.

## 2026-05-15 Pre-execution audit

- Pre-execution gap analysis + adversarial security review filed at `sfu-update/GAP-ANALYSIS.md`. Findings: 4 P0-class security issues, 5 P1, 6 P2, 5 P3, 12 consistency items.
- The most consequential findings (must address): S-P0-1 (routing header forgery enables active-speaker hijack), S-P0-2 (new packet types lack origin discipline — enables redirect/spoof attacks), S-P0-3 (no admission cap until P3 leaves a 7–13 day window where 1000-session DoS is possible), S-P0-4 (NATS subject ACLs not audited).
- Recommends a new convoy `S0` (security pre-flight) materialised in parallel with P0; the quick-wins section lists five items each ≤1 hour that would substantially harden the experiment before P1 opens.
- Scheduler remains paused. No new beads materialised from this audit yet — that step waits for overseer review.

## 2026-05-15 S0 convoy (security pre-flight quick-wins)

Five quick-wins from GAP-ANALYSIS.md authored + materialised as convoy `S0`.

- `sfu-update/adr/0006-refinery-push-contract.md` — convoys default to `--merge=local`; Refinery does not push to the file:// upstream. User manually fetches from the `rig` remote.
- `sfu-update/adr/0007-dag-source-of-truth.md` — `convoy-manifest.yaml` is canonical; PLAN.md "Gastown DAG per Phase" section is long-form documentation.
- `sfu-update/audits/nats-acl-audit.md` — NATS has `auth.enabled: false` in both regions, no client TLS, ClusterIP service, NodePort cross-region gateway also unauthenticated, no subject ACLs. Remediation order: enable basic auth + TLS before P1 closes; private LB for cross-region before P6 closes; subject ACLs before public launch.
- `actix-api/src/constants.rs` — added `MAX_PARTICIPANTS_PER_ROOM = 200` + `MAX_PARTICIPANTS_ENV` env-override constant.
- `actix-api/src/actors/chat_server.rs` — `JoinRoom` handler rejects 201st non-observer joiner with `Err("Room ... is at capacity")`; observers and reconnections bypass the cap correctly. Test `test_join_room_rejects_past_capacity` exercises success at cap-1, rejection at cap, and observer-bypass.
- `actix-api/src/actors/packet_handler.rs` — added explicit `SERVER_ONLY_PACKET_TYPES` list (currently `[CONGESTION]`; SPEAKER_UPDATE/LAYER_HINT/ADMISSION_DECISION will land in P1) plus property test `test_classify_all_server_only_packet_types_as_dropped`. Each future addition must extend the list, which forces the test to enrol it.

Materialise quirks captured in `[[reference-gastown-quirks]]`:
- `create_bead` was calling `extract_id(..., is_convoy=True)` (swapped) — fixed.
- Convoy create output uses `Created convoy 🚚 hq-cv-…` (emoji separator, no colon) — regex generalised.

Convoys after S0:
- `hq-cv-i4w2x` (P0, status: open, blocking S0+P1 via waits-for)
- `hq-cv-k4cb1` (S0, status: open, original from `gt convoy create`)
- `hq-cv-dkhtr` (S0, status: staged_ready, from `gt convoy stage`; will be launched once overseer ack's)

Compile + clippy: `cargo check -p videocall-api --tests` clean. `cargo clippy -- -D warnings` fails on pre-existing `videocall-types/src/validation.rs:79,93` (uninlined-format-args lint), not introduced by S0 changes.

## 2026-05-15 bd state permanence + NATS auth/TLS code (S0 follow-ups)

Two GAP-ANALYSIS-driven fixes landed:

### bd state inconsistency (root-cause fix)

`/gt/videocall/.gitignore` blanket-excluded `.beads/`, which (a) made the auto-export's `git add` step fail silently, leaving the JSONL stale, and (b) caused the next bd command's auto-import to revert any uncommitted writes in Dolt. Combined with `dolt.auto-commit=off` (the default), state mutations didn't stick.

Layered fix:
1. **Removed the blanket `.beads/` line** from `/gt/videocall/.gitignore`. The town repo's intent is that `.beads/` IS tracked at /gt level so JSONL is durable; the inner `/gt/videocall/.beads/.gitignore` already handles the Dolt internals exclusion correctly.
2. **Set `dolt.auto-commit on`** for the videocall rig (was already inherited from defaults; verified).
3. **Set `export.interval=1s`** (was 60s) so the timer-based auto-export catches up fast even when synchronous flush is skipped.
4. **Authored `sfu-update/scripts/bd-sync.sh`** — wraps any bd state-mutating command and runs `bd export --all -o /gt/videocall/.beads/issues.jsonl` synchronously after. Forces `cd /gt/videocall` first so bd's auto-discovery can't resolve a parent `.beads/` and cross-contaminate (we hit exactly that in one iteration — `bd export` from the wrong cwd wrote HQ data into the rig's JSONL; recovered via `bd export` from the correct cwd).
5. **Updated `sfu-update/scripts/materialize.py`** to do a synchronous export at the end of every run.

Verified: closing the four sticky S0 beads (vc-c4e.20/21/22/23) now lands consistently across Dolt + JSONL + `bd list`.

### NATS auth + TLS (avoid the P1 regression)

The audit's recommended remediation order said NATS basic auth + TLS must land before P1 closes (P1 introduces `audio_level`/`is_speaking` on the same unauthenticated bus). Code side is done; ops side is documented for operator action.

- New `actix-api/src/nats_connect.rs`: a single helper reading `NATS_USER`, `NATS_PASSWORD`, `NATS_TLS`, `NATS_TLS_CA` from env, building `async_nats::ConnectOptions` accordingly. Falls back to no-auth + plaintext when env unset (back-compat). Logs `auth=on/off tls=on/off` at INFO for ops visibility. Unit tests for partial-credentials and CA-missing-file failure paths.
- Refactored four production NATS call sites: `bin/webtransport_server.rs:47`, `bin/websocket_server.rs:276`, `bin/metrics_server.rs:667`, `bin/metrics_server_snapshot.rs:431`. Also routed two test-side call sites through the helper for consistency.
- Helm: `helm/rustlemania-{webtransport,websocket}/values.yaml` add `NATS_USER`/`NATS_PASSWORD` env from optional `nats-credentials` Secret + `NATS_TLS`/`NATS_TLS_CA` placeholders.
- Helm (NATS server): `helm/global/{us-east,singapore}/nats/values.yaml` annotated with the rollout sequence and a commented-out `users:` block. `auth.enabled` still `false` — operator flips it after Secret + pod redeploy.
- Authored `sfu-update/audits/nats-auth-rollout.md`: six-phase playbook (A: create Secret. B: redeploy SFU pods. C: enable nats auth. D: verify. E: optional TLS. F: subject ACLs post-P1). With rollback steps.
- Updated `sfu-update/audits/nats-acl-audit.md` §5 to mark the code side done and point at the rollout doc.

Verification: `cargo check -p videocall-api --tests` clean. `cargo test -p videocall-api --lib nats_connect::` passes both new tests.

## 2026-05-15 NATS auth rollout — pre-flight done, ops scripts authored

Followed up the previous bd-state + NATS-code commits with everything that can be done locally before kubectl/helm action against the real clusters. The end state of the local work:

### Integration matrix verified end-to-end

Stood up two local NATS containers — `sfu-nats-auth` on host port 24222 (basic auth: `sfu-cluster` / `testpass123`) and `sfu-nats-noauth` on 24223 — via `sfu-update/audits/nats-sandbox-up.sh`. Wrote `actix-api/tests/nats_auth_integration.rs` exercising all four cells of (client-creds × server-auth) through the `sec_api::nats_connect` helper:

| Cell | Client | Server | Result |
| --- | --- | --- | --- |
| A | no creds | auth | ✓ Authorization Violation (refused) |
| B | creds | auth | ✓ accepted |
| **C** | **creds** | **no-auth** | **✓ accepted — Phase B is provably safe before Phase C** |
| D | no creds | no-auth | ✓ accepted (baseline) |

All four passed (`cargo test -p videocall-api --test nats_auth_integration -- --ignored --test-threads=1`). Tests are `#[ignore]`'d by default; they tcp-probe the sandbox URLs and self-skip if unreachable, so CI without docker is unaffected. Tore the sandbox down after; spin it up again any time before rerolls.

### Runnable phase scripts

`sfu-update/audits/` gained five scripts. The operator runs them once per K8s context (`KUBECTX=do-us-east-cluster` then `KUBECTX=do-singapore-cluster`), no editing required:

- `nats-sandbox-up.sh` — local NATS-with-auth dev loop.
- `nats-auth-phase-a-create-secret.sh` — creates the `nats-credentials` Secret; supports `DRY_RUN=1` with the password redacted in the preview output.
- `nats-auth-phase-b-redeploy-sfu.sh` — `helm upgrade` the two SFU charts; waits for rollout and tails pod logs to surface the `auth=on` line from `nats_connect`.
- `nats-auth-phase-c-enable-nats-auth.sh` — `helm upgrade nats` injecting `auth.enabled=true` + the user/password via `--set` (password stays out of values.yaml); picks the right region chart from `KUBECTX`.
- `nats-auth-phase-d-validate.sh` — runs two `kubectl run --rm` probes via `natsio/nats-box`: connect without creds (expect refused), connect with creds (expect success); exits non-zero on either disagreement.

### Why this is the right stopping point

`kubectl` and `helm` aren't available from this session, and even if they were, executing Phases A–D against the production clusters falls under the "manually approved" gate per [[feedback-local-only-push]]. The pre-flight work proves the code path works against a real NATS-with-auth, and the scripts make each phase a one-liner per cluster for the operator. The audit doc + rollout doc + scripts together are the handoff package.

**Operator action to actually close S-P0-4:**
```bash
openssl rand -base64 32 | tr -d '=+/' | head -c 32 ; echo
# (record password; both clusters use the same value)
KUBECTX=do-us-east-cluster NATS_USER=sfu-cluster NATS_PASSWORD='<paste>' \
    bash sfu-update/audits/nats-auth-phase-a-create-secret.sh
# repeat per cluster, advance through phases b/c/d
```
