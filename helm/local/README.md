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

# End-to-end smoke (up → validate-nats → validate-app → down). Use this
# for one-shot verification in CI / when bisecting "is the stack broken?"
# See `./helm/local/smoke.sh --help` for `--no-teardown` / `--no-bringup`
# / `--no-app` flags. Exits non-zero on any failed phase and leaves the
# cluster up so you can `kubectl --context k3d-videocall-local …` it.
./helm/local/smoke.sh

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

## Adapting this template to a new production rig

The `helm/local/` layout — overlay-per-chart, declarative scripts, an
`.env`-driven Secret bootstrap, an idempotent `up.sh` — is intended as the
template for **standing up a brand-new small production rig** (a fresh
region, a single-tenant deployment, a staging cluster, etc.). The local
stack and a real prod rig differ only at a handful of well-defined
override points; everything below walks each numbered item in **What
`up.sh` installs (v3)** and says what to change for prod.

The mental model: for each component, keep the local overlay's *shape*
(file paths, Secret names, chart releases) and only swap the *values*.
That keeps the diff between your dev rig and a fresh prod rig small,
auditable, and easy to keep in sync.

### Operational changes that aren't per-chart

Before touching any values file, three things differ from local in every
prod rig:

- **Cluster substrate** — replace `k3d` with whatever managed Kubernetes
  the rig runs on (DOKS, EKS, GKE, etc.). The rest of this directory
  doesn't care which substrate is underneath; `KUBECONTEXT` becomes the
  context name your provider hands you instead of `k3d-videocall-local`.
- **DNS** — `*.videocall.local` mapped to `127.0.0.1` is replaced by
  real records pointing at the ingress LoadBalancer. Either provision
  them manually (Route 53 / DigitalOcean DNS / Cloudflare) or install
  `helm/external-dns/` to automate from `Ingress`/`Service` annotations.
  Required records mirror the `ingress.hosts[].host` values you set in
  step 10 below (e.g., `api.<domain>`, `ws.<domain>`,
  `transport.<domain>`).
- **Secrets** — DO NOT commit prod credentials to a `.env` file (the
  pattern `helm/local/up.sh` uses is dev-only). For prod, source them
  from SOPS / Sealed Secrets / your cloud secrets manager, or
  `kubectl create secret` them out-of-band before running any
  `helm install`. The Secret *names* and *keys* below match the local
  rig — keep them identical so the app charts mount them unchanged.

### Per-component override checklist

The numbering mirrors **What `up.sh` installs (v3)** above so you can
diff this section against `up.sh` line by line.

1. **`ingress-nginx`** — local: NodePort overlay
   (`helm/ingress-nginx/values-local.yaml`).
   - For prod, copy `helm/global/us-east/ingress-nginx/values.yaml` as
     a template and adjust the cloud-provider annotations:
     - DigitalOcean: `service.beta.kubernetes.io/do-loadbalancer-name`,
       `-size-unit`, `-healthcheck-protocol`, `-healthcheck-port`
       (already wired in the us-east overlay).
     - AWS: replace with `service.beta.kubernetes.io/aws-load-balancer-*`
       annotations + the AWS LB controller add-on.
     - GKE: set `cloud.google.com/load-balancer-type` and friends.
   - Set `controller.service.type` to `LoadBalancer` (already so in
     us-east; local flips it back to `NodePort` because k3d's klipper
     LB has been flaky).
   - Drop the k3d `--port 30080:30080@loadbalancer` / `30443:30443@…`
     mappings from `up.sh`'s `k3d cluster create` invocation — a real
     LB has its own external IP, no host port-forward needed.
   - Bump `replicaCount` from 1 (local) to ≥2 for HA.

2. **`cert-manager`** — local: installed from `helm/cert-manager/` with
   CRDs.
   - **No values change for prod.** Same chart, same release name. CRD
     install is the same — cert-manager is cluster-scoped, identical
     across substrates.
   - In a hardened prod cluster you may want to add
     `installCRDs: true` only on first install and pin
     `--version` for reproducibility.

3. **ClusterIssuer / Issuer** — local: applies
   `helm/cert-manager-issuer/cluster-issuer-local.yaml`
   (`selfSigned: {}`).
   - For prod, apply
     `helm/cert-manager-issuer/cert-manager-issuer.yaml` (Let's Encrypt
     + DigitalOcean DNS-01 solver) — or your cloud-DNS equivalent. The
     file is **`kind: Issuer`** (per-namespace), not ClusterIssuer; the
     prod app charts annotate with `cert-manager.io/issuer:
     letsencrypt-prod`, NOT `…/cluster-issuer:`.
   - Before applying, set `spec.acme.email` to your team's mailbox and
     create the `digitalocean-dns` Secret with a DNS API token (or
     swap the `solvers[].dns01` block to your cloud provider —
     `cloudflare`, `route53`, etc.).
   - Flip every chart's
     `cert-manager.io/cluster-issuer: local-selfsigned` annotation in
     the *production* values overlays to
     `cert-manager.io/issuer: letsencrypt-prod`. The two annotations
     are mutually exclusive; the chart's `null` trick in
     `values-local.yaml` is how we already drop the prod annotation
     locally — invert the same trick in prod.

4. **Dev credentials Secrets** (`nats-credentials`,
   `postgres-credentials`, `jwt-secret`, `meeting-api-db`) — local:
   created by `up.sh` from `helm/local/.env`.
   - For prod: do NOT keep a `.env` file. Create each Secret directly,
     either out-of-band (`kubectl create secret generic <name> --from-literal=…`)
     or via SOPS / Sealed Secrets committed to a separate repo / your
     cloud secret manager (AWS SSM, GCP Secret Manager, DO Spaces).
   - Required Secrets (keep these *exact* names — the app charts'
     `secretKeyRef`s use them verbatim across local and prod):
     - `nats-credentials` — keys: `user`, `password`. Strong, unique.
     - `postgres-credentials` — keys: `postgres-password`, `password`
       (the bitnami chart wants both, see
       `helm/postgres/values-local.yaml`).
     - `jwt-secret` — key: `secret`. ≥256-bit; rotate quarterly.
     - `meeting-api-db` — key: `url`. The full
       `postgres://<user>:<pass>@<host>:5432/<db>?sslmode=require` DSN.
       Use `sslmode=require` for prod (local uses `disable`).
   - If you use OAuth/OIDC, also create `oauth-credentials`
     (`client-id`, `client-secret`) and `digitalocean-dns`
     (`access-token`) — the local `values-local.yaml` references both
     as `optional: true` so a fresh prod rig without OAuth wired still
     starts.

5. **NATS** — local: `helm/global/local/nats/` (single replica, no
   JetStream, no gateway, auth on from day one).
   - For prod, use `helm/global/<region>/nats/` — `us-east` and
     `singapore` are the two existing examples. Override knobs to lift
     from local to prod:
     - `nats.nats.cluster.enabled: true`, `replicas: 3` for HA.
     - `nats.nats.jetstream.enabled: true` *only* if you need
       persistence; the current room-plane uses pub/sub only, so both
       prod overlays keep JS off. If you flip it on, set
       `fileStore.pvc.storageClassName: do-block-storage` (or cloud
       equivalent) and `fileStore.pvc.size` ≥5Gi.
     - `nats.gateway.enabled: true` + `gateways[]` entries for
       cross-region (us-east overlay has this for the Singapore
       gateway).
     - `resources.{requests,limits}`: bump from local
       (100m / 256Mi limit) to a region-appropriate target.
     - **`auth.enabled` MUST be `true` for a new prod rig.** The local
       rig flipped it on from day one; the existing us-east overlay
       (`helm/global/us-east/nats/values.yaml`) still has
       `auth.enabled: false` pending the multi-step rollout in
       `sfu-update/audits/nats-acl-audit.md` (S-P0-4) +
       `sfu-update/audits/nats-auth-rollout.md`. A *fresh* prod rig
       has no existing pods to drop, so flip it on as part of bringup
       (no rollout dance needed) — and follow the rollout procedure
       for any cluster that's already serving traffic.
   - Credentials still flow via the `nats-credentials` Secret (step
     4) — no `--set-string` on prod, since the password should never
     appear in shell history.

6. **postgres** — local: `helm/postgres/values-local.yaml` (no PVC,
   ephemeral, 100m/256Mi).
   - For prod, use `helm/global/us-east/postgres/values.yaml` as the
     template. Lift these knobs:
     - `primary.persistence.enabled: true`,
       `storageClass: do-block-storage` (or cloud equivalent),
       `size: 10Gi` (or larger for production load).
     - `metrics.enabled: true` + `serviceMonitor.enabled: true` if you
       have Prometheus installed (we install `helm/prometheus/` in
       us-east).
     - **`image.registry: public.ecr.aws`** — bitnami no longer hosts
       on Docker Hub; using the default registry will fail to pull.
       This is the #1 gotcha when promoting `helm/postgres` to a new
       region. The local overlay inherits it from `helm/postgres/values.yaml`
       (currently set to ECR), but verify before you deploy.
     - Backups: NOT in this chart. Arrange `pg_dump` + object-storage
       snapshots out-of-band, or migrate to a managed Postgres service
       (DO Managed Databases, RDS) and point the
       `meeting-api-db` DSN at it — then this chart is unneeded.

7. **App images** (`videocall-meeting-api`, `videocall-media-server`) —
   local: built from `Dockerfile.meeting-api` / `Dockerfile.actix`,
   tagged `:dev`, pushed to `localhost:5000`, k3d-imported.
   - For prod:
     - Build and push to a real registry — `ghcr.io/<org>/<image>`,
       `<acct>.dkr.ecr.<region>.amazonaws.com/<image>`,
       `registry.digitalocean.com/<registry>/<image>`, etc.
     - Use **immutable tags** — the git commit SHA is recommended
       (`videocall-media-server:c63fcc8e`). The us-east overlay still
       uses `:latest` with `pullPolicy: Always`, which loses build
       provenance — fix this when you copy it. Mutable tags also break
       Helm's diffing.
     - Drop the `k3d image import` step from `up.sh` for prod — that's
       a local-only optimisation that side-loads the image into the
       k3s nodes; real nodes pull from the registry.
     - Set `imagePullSecrets:` in the chart values if your registry
       is private. Create the corresponding `regcred` Secret via
       `kubectl create secret docker-registry …`.

8. **`jwt-secret`** — local: created by `up.sh` from
   `JWT_SECRET=` in `.env`.
   - For prod: same Secret name (`jwt-secret`), same key (`secret`).
     Create it out-of-band — do NOT commit it. Rotate quarterly; an
     `actix-api` restart picks up the new value (no zero-downtime
     rotation yet, file a bead if you need one).
   - The Secret is consumed by **three** charts (`meeting-api`,
     `rustlemania-websocket`, `rustlemania-webtransport`). All three
     reference `name: jwt-secret`, `key: secret` — keep both stable.

9. **WebTransport TLS Certificate** — local:
   `helm/local/manifests/webtransport-certificate.yaml` against the
   `local-selfsigned` ClusterIssuer.
   - For prod, prefer the **explicit `Certificate` CR** pattern (don't
     piggy-back on another chart's Ingress annotation — keeps the
     ownership clear). Copy
     `helm/local/manifests/webtransport-certificate.yaml` to a new
     file in your prod overrides directory and change:
     - `metadata.name: transport-<rig>-cert`
     - `spec.secretName: transport-<rig>-tls` (must match
       `tlsSecret:` in the `rustlemania-webtransport` values overlay).
     - `spec.dnsNames: ["transport.<your-domain>"]`.
     - `spec.issuerRef.name: letsencrypt-prod`,
       `spec.issuerRef.kind: Issuer` (NOT ClusterIssuer — see step 3).
   - cert-manager will solve the DNS-01 challenge automatically once
     the Issuer is configured with a working DNS provider.

10. **`meeting-api` + `rustlemania-{websocket,webtransport}`** — local:
    each chart's `values-local.yaml` overlay.
    - For prod, copy `helm/global/us-east/{meeting-api,websocket,webtransport}/values.yaml`
      as templates (one per region / rig). For each chart, override:
      - `image.repository` — your prod registry path.
      - `image.tag` — pinned immutable tag (commit SHA preferred).
      - `image.pullPolicy: Always` (prod) vs `IfNotPresent` (local —
        because we side-load). Use `Always` for prod so a rollout with
        the same tag still re-pulls (rare but useful for
        emergency-rebuilt images).
      - `nameOverride` / `fullnameOverride` — region-suffixed:
        `meeting-api-us-east`, `webtransport-us-east`, etc. See
        `helm/global/us-east/webtransport/values.yaml` for the
        convention.
      - `env`:
        - `NATS_URL` — per-region service name
          (`nats-us-east:4222` in us-east, vs the bare `nats:4222`
          local uses). If you're single-region, `nats:4222` is fine.
        - `UI_ENDPOINT` — real UI URL
          (`https://app.<domain>`), used by CORS.
        - `REGION` — logical region tag for telemetry
          (us-east overlay sets `us-east`).
        - `RUST_LOG: warn,quinn=warn,…` — drop debug verbosity in
          prod; local uses `debug,quinn=warn`.
        - `COOKIE_SECURE: "true"` (prod) vs `"false"` (local). Real
          HTTPS lets the browser accept Secure cookies; self-signed
          local certs do not.
        - `COOKIE_DOMAIN: ".<your-domain>"` (e.g. `.example.com`).
        - `CORS_ALLOWED_ORIGIN`, `AFTER_LOGIN_URL`,
          `ALLOWED_REDIRECT_URLS`, `OAUTH_REDIRECT_URL` — set to your
          real UI URL.
      - `ingress.hosts[].host` — real DNS:
        - `api.<domain>` (meeting-api)
        - `ws.<domain>` (websocket)
        - `transport.<domain>` (webtransport `/healthz`)
      - `ingress.annotations`:
        - Drop `cert-manager.io/cluster-issuer: local-selfsigned`.
        - Add `cert-manager.io/issuer: letsencrypt-prod` (note Issuer
          vs ClusterIssuer; see step 3).
      - `resources.{requests,limits}` — bump from local
        (200m / 256Mi typical) to your region target. us-east
        webtransport runs `3500m / 6000Mi` (lots of headroom for
        crypto + QUIC); meeting-api can stay much smaller.
      - `replicaCount` — 1 (local) → N (HA). Combine with HPA if
        you've installed `autoscaling/v2` HPAs.
      - **For `rustlemania-webtransport` specifically:**
        `loadBalancerAnnotations` block carries the DO LB tuning
        (`do-loadbalancer-healthcheck-port: "444"`, hostname,
        size-unit). The chart template hardcodes
        `service.type: LoadBalancer` so there's no NodePort switch
        from values alone. Local zeroes the annotations
        (`loadBalancerAnnotations:` empty) so klipper doesn't try; in
        prod, keep the us-east annotations and let the DO LB carry
        TCP/443 (QUIC) AND TCP/444 (`/healthz`) through to the pod.

11. **WebTransport `/healthz` Ingress** —
    `helm/local/manifests/webtransport-health-ingress.yaml`.
    - **Delete this manifest in prod.** The prod LB already exposes
      `/healthz` directly on TCP/444 via the DO LB annotation
      (`do-loadbalancer-healthcheck-port: "444"`); the Ingress is
      strictly a workaround for klipper being disabled locally.
    - If you still want HTTP `/healthz` reachable via nginx Ingress
      in prod for observability tooling, you *can* keep an Ingress —
      just retarget the hostname (real DNS) and the issuer
      (`letsencrypt-prod`).

### Small per-rig overrides file

The shape we're converging on: each prod rig is a *short* values-only
overlay per chart, NOT a copy of the whole chart. A reasonable layout
for a new rig `eu-west`:

```
helm/global/eu-west/
├── ingress-nginx/
│   └── values.yaml          # DO LB annotations, replicaCount: 2
├── nats/
│   └── values.yaml          # cluster.replicas: 3, gateway off (single-region)
├── postgres/
│   └── values.yaml          # storageClass + PVC size + ECR registry
├── meeting-api/
│   └── values.yaml          # image.tag, ingress host, OAuth env
├── websocket/
│   └── values.yaml          # image.tag, UI_ENDPOINT, ingress host
└── webtransport/
    └── values.yaml          # image.tag, tlsSecret, LB annotations, ingress host
```

Plus a single `manifests/` directory for the `Certificate` CR
(step 9) and any optional `external-dns` annotations.

Then a thin `up.sh` analog — call it `helm/global/eu-west/up.sh` —
that:
1. Skips cluster create (`kubectl --context <rig-context> get nodes`
   should already work — the cluster is provisioned out-of-band, e.g.,
   `doctl kubernetes cluster create`).
2. Applies the prod `Issuer` (step 3) and asserts the
   `digitalocean-dns` Secret exists.
3. Asserts `nats-credentials`, `postgres-credentials`, `jwt-secret`,
   `meeting-api-db` Secrets exist (do NOT create from `.env`).
4. Runs the same `helm upgrade --install` invocations as
   `helm/local/up.sh` but with the per-rig overlay as
   `--values helm/global/eu-west/<chart>/values.yaml`.
5. Applies the prod `Certificate` CR(s).
6. Emits the same `KUBECONTEXT=…` / `REGISTRY=…` stdout lines for
   downstream scripts (so `redeploy.sh` works against prod with no
   changes).

Keeping `up.sh` symmetrical across local and prod is the whole point of
this template — `redeploy.sh`, the validation scripts, and the
inner-loop developer story all transfer.

### Validating a fresh prod rig

After bringup, run the NATS auth probe. It's substrate-agnostic — works
against any `KUBECTX`, not just k3d:

```bash
# NATS auth probe (refused-without / success-with credentials).
KUBECTX=<rig-context> NATS_USER=<u> NATS_PASSWORD=<p> \
    bash sfu-update/audits/nats-auth-phase-d-validate.sh
```

The app `/healthz` + `auth=on` probe (`helm/local/validate-app.sh`) is
**not yet portable to prod** — it hardcodes the local hostnames
(`api.videocall.local`, `ws.videocall.local`,
`transport.videocall.local`) and the local NodePort (`:30443`). Until
that's parameterised, hit the prod hostnames manually:

```bash
# /healthz on the WebTransport pod via the prod ingress / LB.
curl -fsS https://transport.<your-domain>/healthz   # must return 200

# Tail the apps' logs and check the actix-api NATS connect line.
for app in meeting-api rustlemania-websocket rustlemania-webtransport; do
    kubectl --context <rig-context> -n default logs \
        -l app.kubernetes.io/name=$app --tail=200 \
    | grep 'auth=on' || echo "MISSING auth=on on $app"
done
```

Generalising `validate-app.sh` to take `HEALTHZ_HOST` /
`INGRESS_HTTPS_PORT` / namespace env knobs is filed as the follow-up
bead linked from `vco-ow8.7` — pick that up before standing up a
second rig so we stop maintaining two probes.

If any probe fails, the most common culprit is one of: missing Secret
(step 4), Issuer not provisioning certs (step 3), or DNS not pointing at
the LB (operational changes section).

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
- `smoke.sh` — one-shot `up.sh → validate-nats.sh → validate-app.sh →
  down.sh` runner; supports `--no-teardown` / `--no-bringup` /
  `--no-app` for partial flows (`vco-ow8.7`)
- "Adapting this template to a new production rig" — per-component
  prod override checklist + small per-rig overrides file layout
  (`vco-ow8.7`)
- `helm/{meeting-api,rustlemania-websocket,rustlemania-webtransport}/`
  each gained a `values-local.yaml` overlay (`vco-ow8.5`)
- `helm/local/manifests/` — local-only `Certificate` for the
  WebTransport TLS Secret and an `Ingress` for the WebTransport
  `/healthz` health endpoint (`vco-ow8.5`)

## Out of scope for now

- Real DNS / external-DNS wiring
- `dioxus-ui` deploy
- Privileged 443 → 30443 reverse proxy (drops the `:30443` from URLs)
