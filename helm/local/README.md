# helm/local — local K8s lifecycle for the videocall stack

This directory holds scripts and (eventually) overlays for spinning up a
**local Kubernetes cluster** to run the videocall stack end-to-end during
development. It is the dev inner-loop counterpart to the production Helm
charts in the rest of `helm/`.

See `sfu-update/PLAN.md` for the broader SFU/local stack plan.

## Purpose

Give every engineer (and CI, eventually) a reproducible local Kubernetes
environment that mirrors the topology we run in production: ingress,
cert-manager, NATS, postgres, meeting-api, and the SFU pods — all wired up
with the same Helm charts/values overlays as production where possible.

The goal is **fast inner-loop iteration**: rebuild a single image, push it to
the local registry, redeploy a single pod, and reload the browser — without
touching staging or production.

## Cluster shape (v0)

The local cluster is a [`k3d`](https://k3d.io) cluster named
`videocall-local`:

- **1 control-plane node + 2 worker nodes** (3 nodes total)
- **Local image registry** running at `localhost:5000`
  (k3d-managed container `videocall-local-registry`)
- **Bundled traefik disabled** — we install `ingress-nginx` ourselves in a
  later bead, to match production
- Kubeconfig context: **`k3d-videocall-local`**

## Conventions for subsequent scripts

All scripts in this directory share a small set of conventions so they
chain cleanly:

- They live alongside `up.sh` (e.g. `down.sh`, `pause.sh`, ingress install,
  cert-manager install, NATS install, postgres install, meeting-api install,
  SFU pod install). No nested subdirs unless a script grows into a
  multi-file unit.
- They target the **same kubeconfig context**: `KUBECONTEXT=k3d-videocall-local`.
  Scripts source this from the output of `up.sh` (which emits a
  `KUBECONTEXT=...` line on stdout) or default to that value.
- They are **idempotent**. Re-running a script on an already-installed
  component should be a no-op, not an error.
- They use `set -euo pipefail` and a `[<script>.sh]` log prefix so output
  is readable when chained.

## Image push convention

For fast inner-loop redeploys, push images to the local registry and import
them into the k3d nodes:

```bash
# Build, tag for the local registry, push, then import into k3d nodes.
docker build -t localhost:5000/meeting-api:dev .
docker push localhost:5000/meeting-api:dev
k3d image import localhost:5000/meeting-api:dev --cluster videocall-local
```

The `docker push` step makes the image available to anything that resolves
`localhost:5000`, and `k3d image import` makes it available inside the k3s
nodes themselves (which is the fast path for restart-in-place workflows).

## Usage

```bash
# Bring up the cluster. Idempotent — safe to re-run.
./helm/local/up.sh

# Verify nodes are healthy.
kubectl --context k3d-videocall-local get nodes
```

`up.sh` prints two machine-readable lines at the end for downstream scripts
to grep:

```
KUBECONTEXT=k3d-videocall-local
REGISTRY=localhost:5000
```

## Preflight requirements

`up.sh` checks for these binaries on `PATH` and exits with a clear hint if
any are missing. It does **not** auto-install — that's an operator decision.

- `docker` — runtime for the k3d nodes and the local registry container
- `kubectl` — required to verify node readiness
- `k3d`  — install via `brew install k3d` (macOS) or
  `curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash`

## Out of scope for v0

This bead (`vco-ow8.1`) is **cluster bringup only**. The following ship in
later beads (`vco-ow8.2`, `vco-ow8.3`, `vco-ow8.4`, ...):

- `down.sh` / `pause.sh` cluster lifecycle (`vco-ow8.2`)
- `ingress-nginx` install (`vco-ow8.3`)
- `cert-manager` install (`vco-ow8.3`)
- NATS install (`vco-ow8.4`)
- postgres install (`vco-ow8.4`)
- `meeting-api` deploy
- SFU pod deploy
- `down.sh` / `pause.sh` lifecycle scripts
