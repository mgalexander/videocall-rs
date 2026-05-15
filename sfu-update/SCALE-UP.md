# SFU Refactor — Scale-up Plan

**Audience:** Mayor (and humans reviewing what the Mayor will do next).
**Status:** authored 2026-05-15, after the P0 first-bead proof loop.
**Source of truth:** [PLAN.md](./PLAN.md). This file is the *operational ramp* — when to materialise each phase, what polecats to allocate, and the gates between phases.

---

## Standing rules

1. **One convoy at a time per phase.** Do not materialise `Pn+1` while `Pn` is still open. The DAG between phases is `P{N+1}.waits-for(P{N})`, but on this rig we enforce it manually too — the user (overseer) confirms each phase close before the next materialises.
2. **No remote push without overseer approval.** Refinery merge strategy is `local` (kept on `experimental-sfu`). `git push origin` requires explicit human go-ahead per push.
3. **Disk floor:** halt at 85% root usage; soft alert at 80%. See `ops-log.md` for current readings. Worktree cleanup is `gt polecat nuke` on completed work.
4. **Drift gate:** when reality diverges from `PLAN.md`, **update PLAN.md first**, then re-stage the affected phase via `gt convoy stage` so the wave plan matches the new DAG.

---

## Phase materialisation timeline

| Phase | Materialise after | Slingable beads | Decision beads | Approx duration | Polecat profile |
| --- | --- | --- | --- | --- | --- |
| **P0** | (already done) | 9 (chore/task/feature) | 5 (decisions, non-slingable) | 0.5–1 day | 1 generalist Rust polecat |
| **P1** | P0 closes + overseer ack | 12 (proto + client) | 0 | 1–2 days | 1 generalist + 1 frontend (WebCodecs) |
| **P2** | P1 closes | 9 (server forwarder skeleton) | 1 (ADR-0006 audio-mixdown-deferred) | 3–5 days | 1 Rust-backend + 1 generalist for tests/metrics |
| **P3** | P2 closes | ~13 (speaker + subscription + client UI) | 0 | 3–5 days | 1 Rust-backend + 1 frontend + 1 Playwright/e2e |
| **P4** | P3 closes | ~14 (encoder/decoder + layer selection) | 0 | 4–7 days | 1 frontend (WebCodecs SVC), 1 Rust-backend, 1 perf-test |
| **P5** | P4 closes | 10 (priority queue + congestion) | 0 | 2–3 days | 1 Rust-backend, 1 perf-test |
| **P6** | P5 closes | ~13 (affinity + Helm + load tests) | 0 | 3–5 days | 1 Rust-backend, 1 Helm/K8s, 1 load-test (extends `bot/`) |

ADRs land as `decision`-type beads inside their owning phase, but the convoy daemon skips them (per `IsSlingableType`). They're written by-hand or by a polecat slung explicitly with `--review-only`.

---

## When to call the overseer

- After each phase's final wave closes → overseer reviews diff, decides whether to:
  - Approve materialisation of the next phase, OR
  - Approve a `git push origin experimental-sfu` checkpoint, OR
  - Pause and amend `PLAN.md` for course-correction.
- On any Mayor escalation (`gt escalate`).
- Disk crosses the 80% soft alert (Mayor pages overseer; halt at 85%).
- When a phase's DAG changes mid-flight (a polecat discovers a bead is mis-scoped).

---

## CI / quality gates

These are repo-level gates that every phase exit must satisfy. Wire them into `videocall-rs`'s existing Make/just targets:

| Gate | Command | Phase introduced |
| --- | --- | --- |
| `cargo fmt --check` | `cargo fmt --check` | P0 (existing project standard) |
| `cargo clippy -- -D warnings` | `cargo clippy --workspace -- -D warnings` | P0 (existing) |
| Unit tests | `cargo test --workspace` | P0 |
| Forwarder parity (golden trace) | `cargo test -p videocall-actix-api --features sfu sfu::tests::parity` | P2 |
| Speaker/subscription integration | `cargo test -p videocall-actix-api --features sfu` + 12-bot driver | P3 |
| Throttled-receiver E2E | `network_throttle.py + cargo test --features sfu -- --include-ignored layer_selector::throttle` | P4 |
| Priority-queue burst test | `cargo test --features sfu priority_queue::burst -- --include-ignored` | P5 |
| 50-bot 5-min smoke (merge gate) | `bot/run-smoke.sh --participants 50 --duration 300s` | P6 (CI) |
| 200-bot nightly (release gate) | `bot/run-loadtest.sh --participants 200 --duration 600s` | P6 (nightly CI) |

The 50-bot smoke becomes the *merge gate* once P6 ships. Until then, the merge gate is `cargo test` + `clippy -D warnings`.

---

## Materialisation procedure (for each subsequent phase)

The Mayor (or overseer) runs these from inside `gastown-sandbox`, cwd `/gt/videocall`:

```bash
# 1. Author the phase entries in convoy-manifest.yaml (PLAN.md DAG section is the source).
#    Add the new beads + their `blocked_by` edges. Don't change earlier phases.

# 2. Re-run materialise. Idempotent: only new beads/edges are created.
bash /mnt/llms/videocall/sfu-update/scripts/materialize.sh

# 3. Stage the new convoy from the epic. Gives you a Wave plan and warnings.
gt convoy stage <new-convoy-key>     # uses the convoy id from .materialize-state.json

# 4. Resolve any 'staged:warnings' (typically: dep names a bead that hasn't been
#    materialised; fix manifest, re-run step 2).

# 5. Launch. Daemon's event-driven feeder takes over.
gt convoy launch <new-convoy-id>

# 6. (Optional) Pre-warm the polecat pool to match the polecat profile in this doc.
gt polecat pool-init videocall --size <N>

# 7. Witness/Refinery handle dispatch + merge. If a polecat stalls, Witness
#    intervenes; if Witness can't, Mayor escalates to overseer.
```

---

## Failure modes to expect

- **Decision beads never auto-dispatch.** They're `type=decision` and `IsSlingableType` rejects them. Approach: sling them explicitly with `--review-only` to a polecat that's good at writing ADRs (or author by hand). After ADRs are written, run `bd close <id>` so they don't gate the convoy close.
- **Polecat exhausts context mid-bead.** `gt handoff` from polecat creates a fresh session with the hook context preserved. Daemon's stranded scan covers crash cases.
- **Refinery merge conflict.** The 14 P0 beads touch mostly disjoint files (sfu-update/*, separate ADRs, two binaries). Conflicts shouldn't happen in P0. P1 will have conflicts (multiple client encoders edited concurrently); plan to sling P1 polecats *sequentially* not parallel-wave for the encoder beads.
- **Auto-import dolt warnings.** `auto-importing N bytes from issues.jsonl into empty database` appears on most bd commands because Dolt's in-memory state is sometimes empty after process restart. Non-fatal; bd re-imports from the JSONL on disk.
- **`auto-export: git add failed` warnings.** Cosmetic — `.beads/` is gitignored. Bead state lives in Dolt + issues.jsonl, separate from code commits.

---

## Out-of-scope-for-now (re-confirm before materialising)

- Audio mixdown (ADR-0006). Will get its own convoy after P3 closes.
- Conference shape (30–50 active senders). Plan revisits after P6 closes if needed.
- AV1 / H.264 codecs. VP9 SVC only.
- Recording bots. Capability bit only; no recorder workflow built.
