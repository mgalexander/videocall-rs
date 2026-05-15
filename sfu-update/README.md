# sfu-update/

Planning artifacts for the SFU refactor of videocall-rs.

| File | Purpose |
| --- | --- |
| [`PLAN.md`](./PLAN.md) | Source-of-truth implementation plan (mirrored from harness plan file). |
| [`GAP-ANALYSIS.md`](./GAP-ANALYSIS.md) | Pre-execution audit: plan inconsistencies + adversarial security review filed by priority (P0–P3, C). Includes a suggested `S0` security pre-flight convoy. |
| [`audits/nats-acl-audit.md`](./audits/nats-acl-audit.md) | S-P0-4: NATS deployment audit. Findings + remediation order. |
| [`audits/nats-auth-rollout.md`](./audits/nats-auth-rollout.md) | Six-phase rollout playbook to enable NATS basic auth + TLS without dropping pods. Code side + integration matrix already verified; phases A–D are runnable scripts in this directory. |
| `audits/nats-{sandbox-up,auth-phase-{a,b,c,d}-*}.sh` | Runnable scripts for the rollout phases. Pre-flight verified with the local NATS sandbox and the integration matrix in `actix-api/tests/nats_auth_integration.rs`. |
| [`scripts/bd-sync.sh`](./scripts/bd-sync.sh) | Wrap any state-mutating `bd` command and synchronously flush to JSONL; safe to run from any cwd in the container. |
| [`convoy-manifest.yaml`](./convoy-manifest.yaml) | Machine-readable representation of the bead/convoy DAG for gastown. |
| [`scripts/materialize.sh`](./scripts/materialize.sh) | Idempotent script that walks the manifest and invokes `bd create` / `bd update --add-dep`. |
| [`ops-log.md`](./ops-log.md) | Running log of bootstrap actions, disk readings, mayor responses. |
| [`SCALE-UP.md`](./SCALE-UP.md) | Operational ramp: when to materialise P1..P6, polecat capability mix, CI gate thresholds. *(written after first-bead proof loop)* |
| [`FANOUT.md`](./FANOUT.md) | Per-phase polecat reservation strategy for the convoy daemon. *(written after first-bead proof loop)* |
| `adr/` | Architectural Decision Records (one file per decision). |
| `capacity-model.md` | Back-of-envelope from PLAN.md §J, refined in P6. |
| `packet-diagrams.md` | Sequence diagrams for the new wire protocol packets. |
| `test-matrix.md` | Codec × browser × meeting-shape coverage. |

Branch: `experimental-sfu` (local-only). Pushing to remote requires user approval.
Prefix: `vc-` (proposed).
Umbrella RFC: `/rfc/rfc-2-sfu-architecture.md`.
