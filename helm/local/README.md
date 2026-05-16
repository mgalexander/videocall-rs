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

## Cluster shape (v1)

The local cluster is a [`k3d`](https://k3d.io) cluster named
`videocall-local`:

- **1 control-plane node + 2 worker nodes** (3 nodes total)
- **Local image registry** running at `localhost:5000`
  (k3d-managed container `videocall-local-registry`)
- **Bundled traefik disabled** — we install `ingress-nginx` ourselves
  (see below), to match production
- Kubeconfig context: **`k3d-videocall-local`**

## What `up.sh` installs (v3)

After cluster bringup, `up.sh` deploys the cluster-wide platform layer:

1. **`ingress-nginx`** (namespace `ingress-nginx`) — installed from
   `helm/ingress-nginx/` with the local overlay
   `helm/ingress-nginx/values-local.yaml`. Uses **NodePort** for the
   controller Service (http `30080`, https `30443`) rather than
   LoadBalancer — k3d's bundled klipper LB has been flaky in our setup, so
   we side-step it. Production (`helm/global/us-east/ingress-nginx/`) still
   uses LoadBalancer + DigitalOcean annotations; that overlay is **not**
   inherited locally.
2. **`cert-manager`** (namespace `cert-manager`) — installed from
   `helm/cert-manager/` with CRDs enabled. `up.sh` waits for the
   `cert-manager`, `cert-manager-webhook`, and `cert-manager-cainjector`
   deployments to report Available before proceeding (the webhook in
   particular needs a few seconds to register).
3. **`local-selfsigned` `ClusterIssuer`** — applied from
   `helm/cert-manager-issuer/cluster-issuer-local.yaml`. This is a
   `selfSigned` issuer suitable for `*.videocall.local` dev certs. It is
   the local counterpart to the production Let's Encrypt + DigitalOcean
   DNS `Issuer` in `cert-manager-issuer.yaml` (which is **not** applied
   locally).
4. **Dev-credentials Secrets** — `nats-credentials` and
   `postgres-credentials` (namespace `default`), created from
   `helm/local/.env`. If no `.env` file exists yet, `up.sh` bootstraps it
   from `helm/local/.env.example` (gitignored so each developer can
   override credentials without touching the repo).
5. **NATS** (namespace `default`) — installed from `helm/global/local/nats/`
   (sibling of `helm/global/us-east/nats/` and `helm/global/singapore/nats/`).
   Single-replica, no JetStream, no cross-region gateway, **basic auth
   enabled from day one**. Credentials are injected via `--set-string`
   from `.env` (kept out of `values.yaml`) and persisted to the
   `nats-credentials` Secret for app pods to consume.
6. **postgres** (namespace `default`) — installed from `helm/postgres/`
   with the local overlay `helm/postgres/values-local.yaml`. Standalone,
   ephemeral storage (no PVC), credentials sourced from the
   `postgres-credentials` Secret.
7. **App images** — `Dockerfile.meeting-api` and `Dockerfile.actix` are
   built at the repo root, tagged `:dev`, pushed to the local registry
   (`localhost:5000`), and side-loaded into the cluster via `k3d image
   import`. Tags are explicit (no `:latest`) so warm-restart workflows
   pick up the exact image the developer just built.
8. **`jwt-secret`** (namespace `default`) — created from `JWT_SECRET`
   in `helm/local/.env`. Consumed by meeting-api (signs room access
   tokens) and the SFU pods (verify them). Same Secret name/key as
   production.
9. **`transport-videocall-local` Certificate** — cert-manager issues
   the TLS material for the WebTransport QUIC listener (mounted at
   `/certs` in the pod). Backed by the `local-selfsigned` ClusterIssuer.
   Applied from `helm/local/manifests/webtransport-certificate.yaml`.
10. **meeting-api + rustlemania-{websocket,webtransport}** — installed
    from their respective Helm charts with each chart's
    `values-local.yaml` overlay. The overlays point `image.repository`
    at the local registry, set `pullPolicy: IfNotPresent`, and target
    `*.videocall.local` hostnames with the `local-selfsigned`
    ClusterIssuer (`cert-manager.io/cluster-issuer` annotation — NOT
    `cert-manager.io/issuer`).
11. **WebTransport `/healthz` Ingress** — applied from
    `helm/local/manifests/webtransport-health-ingress.yaml`. Routes
    `https://transport.videocall.local/healthz` through nginx to the
    chart's `-lb` Service on TCP/444. Local-only — production hits
    the same endpoint via the DigitalOcean LB on the same port.

## `/etc/hosts` requirement

The cluster's ingress controller publishes HTTPS on host port `30443`
(see `helm/ingress-nginx/values-local.yaml` for the NodePort and the
`--port 30443:30443@loadbalancer` mapping in `up.sh`'s k3d cluster
create). To reach the app hostnames from your browser/curl, add a
single line to `/etc/hosts`:

```
127.0.0.1 api.videocall.local ws.videocall.local transport.videocall.local
```

`validate-app.sh` and a curl from the host both use port `30443`
(e.g. `https://transport.videocall.local:30443/healthz`). If you want
to drop the `:30443` for browser convenience, run a privileged
reverse proxy that forwards 443 → 30443 — out of scope for `up.sh`,
which deliberately avoids requiring `sudo`.

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

The full lifecycle is four scripts, all idempotent (re-running them is safe):

```bash
# Bring up the cluster + platform layer. Idempotent — safe to re-run.
./helm/local/up.sh

# Verify nodes are healthy.
kubectl --context k3d-videocall-local get nodes

# Verify the ingress controller pod is Ready.
kubectl --context k3d-videocall-local get pods -n ingress-nginx

# Verify cert-manager pods are Ready.
kubectl --context k3d-videocall-local get pods -n cert-manager

# Verify the self-signed ClusterIssuer is Ready.
kubectl --context k3d-videocall-local get clusterissuer

# Verify NATS and postgres are Running.
kubectl --context k3d-videocall-local get pods -n default

# Verify NATS auth is actually enforced (probes with + without creds).
./helm/local/validate-nats.sh

# Verify the meeting-api + SFU install: /healthz=200 on the WebTransport
# ingress AND each app's logs show `auth=on` from nats_connect.
./helm/local/validate-app.sh

# Hibernate: stop the node containers but keep cluster state on disk.
# Use this to free CPU/RAM between coding sessions without losing installed
# components or images you've pushed to the local registry.
./helm/local/pause.sh

# Wake the cluster back up. Same final stdout shape as up.sh, so anything
# that consumes up.sh's KUBECONTEXT=/REGISTRY= lines can consume resume.sh too.
./helm/local/resume.sh

# Fast inner-loop refresh after a Rust source change: rebuild the affected
# images, push + k3d-import them, `helm upgrade` the affected releases with
# a unique per-run tag (so pods actually roll), wait for rollout, and tail
# logs for ~5s to surface immediate errors. Assumes up.sh has already run.
./helm/local/redeploy.sh                       # all three (meeting-api + both SFUs)
./helm/local/redeploy.sh meeting-api           # just meeting-api
./helm/local/redeploy.sh websocket webtransport  # both SFU releases, single media-server build

# Full teardown. Deletes the cluster and the local registry container.
# Next `up.sh` starts from a clean slate.
./helm/local/down.sh
```

`up.sh` and `resume.sh` both print two machine-readable lines at the end for
downstream scripts to grep:

```
KUBECONTEXT=k3d-videocall-local
REGISTRY=localhost:5000
```

## Preflight requirements

`up.sh` checks for these binaries on `PATH` and exits with a clear hint if
any are missing. It does **not** auto-install — that's an operator decision.

- `docker` — runtime for the k3d nodes and the local registry container
- `kubectl` — required to verify node readiness and apply the ClusterIssuer
- `k3d`  — install via `brew install k3d` (macOS) or
  `curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash`
- `helm` — required to install `ingress-nginx` and `cert-manager`
  (macOS: `brew install helm`)

## Shipped so far

- `up.sh` v3 — cluster bringup + `ingress-nginx` (NodePort overlay) +
  `cert-manager` + self-signed `ClusterIssuer` + NATS (with auth) +
  postgres + `meeting-api` + SFU pods (websocket + webtransport) with
  local image builds, `jwt-secret`, and the WebTransport `/healthz`
  ingress (`vco-ow8.1`, `vco-ow8.3`, `vco-ow8.4`, `vco-ow8.5`)
- `down.sh` / `pause.sh` / `resume.sh` — cluster lifecycle (`vco-ow8.2`)
- `redeploy.sh` — fast inner-loop image + helm refresh (rebuild changed
  Rust crates, push + k3d-import, `helm upgrade` with a unique per-run
  tag, wait for rollout, tail logs ~5s) (`vco-ow8.6`)
- `validate-nats.sh` — local equivalent of
  `sfu-update/audits/nats-auth-phase-d-validate.sh` (`vco-ow8.4`)
- `validate-app.sh` — `/healthz=200` + per-app `auth=on` check
  (`vco-ow8.5`)
- `helm/{meeting-api,rustlemania-websocket,rustlemania-webtransport}/`
  each gained a `values-local.yaml` overlay (`vco-ow8.5`)
- `helm/local/manifests/` — local-only `Certificate` for the
  WebTransport TLS Secret and an `Ingress` for the WebTransport
  `/healthz` health endpoint (`vco-ow8.5`)

## Out of scope for now

- Real DNS / external-DNS wiring
- `dioxus-ui` deploy
- Privileged 443 → 30443 reverse proxy (drops the `:30443` from URLs)
