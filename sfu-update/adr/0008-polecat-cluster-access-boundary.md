# ADR-0008: Polecat ↔ Local Cluster Access Boundary (Track 3)

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** overseer (malexander)
- **Related:** [`PLAN.md`](../PLAN.md) "Bootstrap into Gastown" guardrails, [`ops-convoy-manifest.yaml`](../ops-convoy-manifest.yaml) (Track 2 / Track 3 work), [`ops-log.md`](../ops-log.md) (2026-05-16 vco-ow8.10 redeploy), bead vco-ow8.10 (postgres redeploy), bead vco-ow8.11 (this ADR).

## Context

Polecat sandboxes run inside the `gastown-sandbox` container. From inside that container they have **no path to the local k3d cluster** that hosts the development SFU stack:

- No docker socket is mounted (cannot `docker exec` into k3d nodes or run `k3d kubeconfig get` to bootstrap a context).
- No kubeconfig in the sandbox filesystem (the host's `~/.kube/config` is not bind-mounted).
- No `kubectl` / `helm` / `k3d` binaries in the sandbox image (these were installed only at `~/.local/bin` on the host).

This was discovered concretely during **vco-ow8.10** (postgres redeploy after the chart rewrite). Obsidian correctly recognised the bead's steps — `helm uninstall`, `bash up.sh`, `kubectl exec`, `kubectl logs` — were unexecutable from her sandbox, closed the bead `no-changes: host-only operation`, and filed an escalation. The overseer ran the redeploy from the host.

The same wall blocks every per-phase ops-validation step in **Track 3** of the operating model (per-phase deployment + integration validation of dev-rig artifacts after each phase merges to `experimental-sfu`). The dev/ops agent split breaks down at the `kubectl` boundary: any bead whose verification requires `kubectl` / `helm` / `k3d` against the local cluster will hit the same escalation pattern, costing one overseer round-trip per phase.

The four options considered (recorded on vco-ow8.11):

- **A. Mount docker socket + kubeconfig + binaries into the polecat sandbox.** Simplest fix. Edit `/mnt/llms/gas-town/docker-compose.override.yml` to bind-mount the socket, `~/.kube`, and the host's `k3d`/`kubectl`/`helm` binaries. Trade-off: any polecat (current or future) running in that sandbox gets effective root-on-host via the docker socket. Acceptable for a solo developer; unacceptable for multi-tenant / shared / production setups.
- **B. Run a kind/k3d cluster inside the `gastown-sandbox` container.** DinD or rootless variant; polecats reach it without escaping the sandbox. Heavier; needs DinD plumbing + binary installs baked into the sandbox image.
- **C. "Deploy proxy" pattern.** Polecat writes deploy intent (e.g. an `apply.yaml` artifact + a marker file) to a known path; a host-side watcher reconciles. Highest isolation, most operational surface.
- **D. Keep the status quo.** Codify the boundary: cluster-touching ops are overseer-only by policy for the experimental-sfu phase. Polecats author code, charts, and validation scripts; the overseer (or, later, a dedicated host-side ops agent) executes.

## Decision

**Adopt Option D for the experimental-sfu phase.** Cluster-touching ops against the local k3d cluster are **overseer-only by policy**. Polecats remain confined to the sandbox and do not gain kubectl/helm/k3d access.

Concretely:

1. **Polecats own:** code authoring, chart rewrites, values-overlay authoring, manifest authoring, runnable scripts under `sfu-update/audits/` and `helm/local/`, and any verification that runs entirely inside the sandbox (cargo tests, container-only smoke tests via `docker compose`, local NATS sandbox via `docker/docker-compose.nats-dev.yaml`, etc.).
2. **Overseer (or future host-side ops agent) owns:** anything that requires `kubectl --context k3d-videocall-local`, `helm --kube-context k3d-videocall-local`, `k3d cluster *`, or docker access to k3d nodes. This includes `helm install/upgrade/uninstall` against the local cluster, `kubectl exec` into pods, log tails (`kubectl logs`), pod/service inspection, and the post-merge per-phase ops-validation runs in Track 3.
3. **Bead authoring rule.** Beads whose acceptance criteria require step (2) operations must be **explicitly marked** in their `summary:` as host-only (e.g. "Procedure: run from host" + the literal commands). Polecats hitting such a bead close it `no-changes: host-only operation per ADR-0008` and the bead is re-dispatched to the overseer's queue. Do not assign these beads to polecats with the expectation that they will execute — assign them so the polecat can verify the procedure is well-formed (the right context, the right commands, the right validation) and the overseer executes from the host.
4. **Track 3 cadence preserved.** Dev rig closes phase → overseer runs the phase's `helm/local/redeploy.sh` + the phase's validation scripts (authored by polecats under `sfu-update/audits/` and `helm/local/`) against `k3d-videocall-local` → overseer logs the run in `ops-log.md` → next dev phase opens. The overseer is the executor; the polecats supply everything except the kubectl invocation.
5. **Escalation path when a polecat hits the wall.** Close the bead with `--reason="no-changes: host-only operation per ADR-0008"` and `gt escalate -s HIGH "Host-only ops: <bead-id>"`. Do not retry. Do not attempt to bootstrap a context from within the sandbox.

## Consequences

**Pro:**
- Zero new container privilege. Polecats remain unprivileged inside the sandbox; no path to the docker socket, no risk of cross-rig blast radius via shared cluster access.
- No sandbox-image churn. The `gastown-sandbox` image is shared across rigs (videocall_ops, lps-, imap-, …); not adding kubectl/helm/k3d there avoids dragging cluster-management tooling into rigs that don't need it.
- Forces clean separation between authoring and operating. Validation scripts authored by polecats are exercised in CI-style isolation; the overseer's only job is to invoke them. This is the same separation we already have for `git push origin` (manual-approval gate per [ADR-0006](./0006-refinery-push-contract.md)).
- The Track 3 deliverable (`helm/local/redeploy.sh`, per-phase validation scripts) is **strengthened** by this constraint: scripts must be parameterless, idempotent, and observable, because the overseer runs them blind. That hardens them for the eventual "totally new small production rig" template (vco-7).

**Con:**
- Every Track 3 phase incurs an overseer round-trip. For a 6-phase rollout that is ~6 manual invocations spaced over weeks — tolerable for a solo-developer cadence, not for high-frequency rollouts.
- The "dev rig closes → ops rig validates → ops mails dev rig 'go'" automation envisioned in the operating-model writeup becomes "dev rig closes → ops rig stages validation script → overseer runs it → overseer closes the loop." The ops rig's role narrows to **script authorship + verification specification** rather than execution.
- Polecats can hit and close several host-only beads in a row before the overseer notices, creating bead-thrash. Mitigation: the bead's `summary:` must mark host-only operations explicitly (rule #3 above) so polecats close them in one step, not after a discovery loop. The Witness/Mayor can also flag patterns of `no-changes: host-only` closures and surface them.
- If multi-user or production-grade rigs land before we revisit this ADR, polecats remain blind to the cluster they're notionally targeting. That is a known limitation; see "When to revisit" below.

## When to revisit

This ADR is **right-sized for the experimental-sfu phase and the single-developer local stack.** Revisit when any of these become true:

- **More than one polecat needs cluster access concurrently** on the same shared sandbox. At that point Option B (in-sandbox k3d via DinD) becomes the right shape: each rig gets its own cluster, no shared docker socket. Trigger: a Track-3 bead requires >1 polecat working in parallel against the cluster.
- **The project graduates from local k3d to a managed cluster** (DO, GKE, EKS, etc.) for routine validation. At that point the access pattern shifts from docker-socket-on-host to kubeconfig-with-scoped-RBAC — a fundamentally different security model. Option A becomes safer (no docker socket; just a read-only kubeconfig with a namespaced ServiceAccount) and the trade-off inverts.
- **An ops-specific agent role lands** (something between "polecat" and "overseer" — a sandboxed agent with cluster access but not arbitrary code-execution access). At that point the boundary moves from "no cluster access for polecats" to "no cluster access for general-purpose polecats; ops agents have scoped access."
- **The "host-only" closure rate rises above ~1/phase.** If the per-phase ops-validation work fragments into many small kubectl-touching beads, the round-trip cost stops being tolerable.

Until one of those triggers fires, the policy stands.

## Rejected alternatives

**Option A (mount docker socket + kubeconfig + binaries).** Rejected for this phase. Docker socket access is effectively root-on-host; granting it to every polecat in a shared sandbox is a sharp escalation of trust. Revisit when the trust model is no longer "solo developer on a workstation" (see "When to revisit").

**Option B (DinD inside the sandbox).** Rejected as over-engineered for a single-developer single-cluster setup. Adds DinD plumbing + an image rebuild + cluster-state management inside a container that is rebooted as part of routine rig operations. Re-evaluate when concurrent cluster access is needed (see "When to revisit").

**Option C (deploy proxy).** Rejected as the most operational surface for the least incremental capability. The watcher itself becomes a critical component that needs monitoring, error handling, and audit; meanwhile the overseer's manual `helm upgrade` invocation is one shell command. The juice isn't worth the squeeze at this scale.

## Implementation

- [x] (Documentation) This ADR landed at `sfu-update/adr/0008-polecat-cluster-access-boundary.md`.
- [x] (Documentation) `sfu-update/ops-log.md` gains a one-line entry pointing at this ADR under the existing Track 3 reference.
- [x] (Documentation) `sfu-update/ops-convoy-manifest.yaml` gains a one-line comment near the existing Track 3 reference (vco-6) pointing at this ADR.
- [x] (Documentation) `sfu-update/PLAN.md` "Standing guardrails" gains a one-line note pointing at this ADR (Track 3 boundary). PLAN.md does not have a dedicated "Track 3" section — the Track 1/2/3 terminology lives in `ops-log.md` and `ops-convoy-manifest.yaml`, both of which are also updated.
- [ ] (Future bead authoring) When the next Track 3 bead lands, its `summary:` is to mark host-only steps explicitly per rule #3.

## Status

Accepted 2026-05-17. Effective immediately. Cluster-touching ops against `k3d-videocall-local` are overseer-only for the experimental-sfu phase. Polecats hitting the wall close their bead `no-changes: host-only operation per ADR-0008` and escalate.
