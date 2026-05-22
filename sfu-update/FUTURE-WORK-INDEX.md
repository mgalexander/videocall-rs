# SFU Future-Work Index (for future gastown work)

v1 (webinar-shape, ≤200 participants) is validated and proposed for merge separately
(branch `sfu-webinar-proposal`). This index points to everything **deferred to future
requests** — the analyses, plans, designs, and open beads that live on `experimental-sfu`
but are intentionally NOT in the v1 PR. Pick up here when resuming scaling work.

## Plans / backlogs
- **`SCALING-BACKLOG.md`** — the ordered >v1 scaling track (binding-constraint order):
  drain parallelism (K>1), per-receiver video-egress budget, multi-pod spillover, town-hall.
- **`CROSS-POD-DATA-PLANE-FUTURE.md`** — one room across multiple pods (spillover data plane);
  13-commit arc didn't converge; recommends reverting to a clean baseline first.

## Designs (not built)
- **`adr/0009-hybrid-presenter-scaling-townhall.md`** — dynamic audio-mixdown "town-hall" at
  500 users. **Security-CLEARED to prototype** (web-security-auditor); binding crypto
  conditions tracked in the ADR (media-scoped key, GCM nonce NF-1/NF-2, Ed25519 switch auth,
  RSA→OAEP). Only relevant >500 and after the video-egress budget.

## Root-cause analyses (the durable knowledge)
`audits/200bot-monitor/`:
- **`PRESENTER-SCALING-ROOTCAUSE.md`** — single per-room dispatcher = O(presenters×receivers)/core.
- **`LATE-JOINER-INTEGRATION-ROOTCAUSE.md`** — AllowSet cache thundering-herd (the v1 fix, lj-1).
- **`FORWARDING-STALL-ROOTCAUSE.md`** — single inbound-drain saturation cliff (the 20p/400 overload; lj-7..lj-9).
- **`DELIVERY-SCALING-ROOTCAUSE.md`**, **`TOWNHALL-CHOKEPOINT-ANALYSIS.md`** (mixdown optimizes a non-binding term — video egress is the real >500 wall), **`MULTIPOD-ROOTCAUSE.md`**.
- Defect write-ups: `DEFECT-JOINHANDLE-PANIC.md`, `DEFECT2-VIDEO-KEYFRAME.md`, `DEFECT3-CROSSPOD-DATAPLANE.md`, `spillover-decode/DEFECT1-REDIRECT-BOUNCE.md`.

## Test findings + harness
- `audits/200bot-monitor/matrix/FINDINGS.md` + `matrix-results.csv` — the 10-pattern matrix.
- `soak-4cpu/`, `soak-10k/`, `stress-*/`, `spillover-*/` FINDINGS + CSVs + soak `.sh` scripts
  (reusable load harness; raw `.log`/`.jsonl` are gitignored).
- `b1-validate/` — the late-joiner validation soaks (Track 1 + lj-fixes).

## Open beads (work items)
- `beads/` — bead bodies for the deferred fixes: vc-8rh2 (lj-8 pipeline), vc-rcpp (lj-9 K>1 +
  bot sharded publish), vc-stee (B13 video egress budget), vc-133g/vc-3hxy (lj-3/lj-4),
  town-hall B5–B12 specs (in ADR-0009). Cross-pod + spillover beads.

## Local dev / browser tooling
- **`LOCAL-BROWSER-RUNBOOK.md`** — run a real browser against the local k3d SFU (all-localhost
  via port-forwards, WebSocket transport). Companion: `docker/docker-compose.local-ui.yaml`.

## Resuming
Order for the next scaling push: vc-stee (video egress budget, the real >500 lever, keeps
E2EE) → vc-rcpp (K>1 drain) → multi-pod spillover → town-hall (only >500). See
`SCALING-BACKLOG.md` for the binding-constraint rationale. Validate with the `audits/` soak
harness on a FRESH pod (back-to-back soaks degrade state).
