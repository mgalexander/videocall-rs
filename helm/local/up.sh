#!/usr/bin/env bash
#
# helm/local/up.sh — v3 local k3d cluster bringup for the videocall stack.
#
# Scope (v3):
#   1. Create the k3d cluster (idempotent).
#   2. Install ingress-nginx (NodePort) into namespace ingress-nginx.
#   3. Install cert-manager (with CRDs) into namespace cert-manager.
#   4. Apply a self-signed ClusterIssuer (`local-selfsigned`) for
#      *.videocall.local certs.
#   5. Create `nats-credentials` + `postgres-credentials` + `jwt-secret`
#      Secrets from helm/local/.env (auto-bootstrapped from .env.example
#      if missing).
#   6. Install NATS via helm/global/local/nats (single replica, auth on).
#   7. Install postgres via helm/postgres + values-local.yaml.
#   8. Build + push + k3d-import meeting-api and SFU images from
#      Dockerfile.meeting-api / Dockerfile.actix (tags: :dev).
#   9. Apply the WebTransport TLS Certificate (cert-manager) and wait
#      for the resulting Secret to be Ready.
#  10. helm install meeting-api + rustlemania-{websocket,webtransport}
#      with their values-local.yaml overlays.
#  11. Apply the local-only /healthz Ingress for the WebTransport pod.
#
# Out of scope (later beads): real DNS / external-DNS, dioxus-ui deploy.
# See helm/local/README.md.
#
# Idempotent: re-running is a no-op. Uses `helm upgrade --install` and
# `kubectl apply` throughout. Re-runs against an existing cluster simply
# re-converge state.

set -euo pipefail

# ----- Configurable knobs (env-overridable) -----------------------------------
CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
REGISTRY_NAME="${REGISTRY_NAME:-videocall-local-registry}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"

# How long (seconds) to wait for all 3 nodes to report Ready after create.
NODE_READY_TIMEOUT="${NODE_READY_TIMEOUT:-120}"

# How long (seconds) to wait for deployments (ingress-nginx, cert-manager).
DEPLOY_READY_TIMEOUT="${DEPLOY_READY_TIMEOUT:-180}"

# Resolve the helm/ directory and repo root relative to this script so
# the script can be invoked from any cwd. REPO_ROOT is the docker build
# context for the meeting-api and SFU images (their Dockerfiles live at
# the repo root and `COPY . /app` the whole tree).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${HELM_DIR}/.." && pwd)"

LOG_PREFIX="[up.sh]"

log() {
    echo "${LOG_PREFIX} $*"
}

err() {
    echo "${LOG_PREFIX} ERROR: $*" >&2
}

# ----- Preflight: required binaries -------------------------------------------
require_bin() {
    local bin="$1"
    local hint="$2"
    if ! command -v "${bin}" >/dev/null 2>&1; then
        err "'${bin}' not found on PATH."
        err "${hint}"
        exit 1
    fi
}

require_bin docker \
    "Install Docker Desktop / Docker Engine for your platform: https://docs.docker.com/get-docker/"

require_bin kubectl \
    "Install kubectl: https://kubernetes.io/docs/tasks/tools/  (macOS: brew install kubectl)"

require_bin k3d \
    "Install k3d. macOS: brew install k3d. Or: curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash"

require_bin helm \
    "Install helm: https://helm.sh/docs/intro/install/  (macOS: brew install helm)"

# ----- Idempotency check ------------------------------------------------------
if k3d cluster list "${CLUSTER_NAME}" >/dev/null 2>&1; then
    log "cluster '${CLUSTER_NAME}' already up — skipping create"
else
    log "creating k3d cluster '${CLUSTER_NAME}' (1 server + 2 agents, registry on localhost:${REGISTRY_PORT}, traefik disabled)"
    # Port maps publish the ingress-nginx NodePorts (30080/30443 — see
    # helm/ingress-nginx/values-local.yaml) onto the host so the
    # /etc/hosts → 127.0.0.1 → cluster path works end-to-end. Without
    # these, `curl https://transport.videocall.local/healthz` would
    # only reach the ingress from inside a pod.
    #
    # Existing clusters created before this change do NOT get the port
    # maps retroactively — recreate with helm/local/down.sh + up.sh to
    # pick them up.
    k3d cluster create "${CLUSTER_NAME}" \
        --servers 1 \
        --agents 2 \
        --registry-create "${REGISTRY_NAME}:0.0.0.0:${REGISTRY_PORT}" \
        --k3s-arg "--disable=traefik@server:0" \
        --port "30080:30080@loadbalancer" \
        --port "30443:30443@loadbalancer" \
        --wait
    log "cluster '${CLUSTER_NAME}' created"
fi

# ----- Wait for 3 Ready nodes -------------------------------------------------
log "waiting up to ${NODE_READY_TIMEOUT}s for 3 nodes to report Ready (context: ${KUBECONTEXT})"
deadline=$(( $(date +%s) + NODE_READY_TIMEOUT ))
while :; do
    # Count nodes whose Ready condition column is exactly "Ready" (not "NotReady").
    ready_count=$(kubectl --context "${KUBECONTEXT}" get nodes --no-headers 2>/dev/null \
        | awk '{print $2}' \
        | grep -cx "Ready" || true)

    if [ "${ready_count}" -ge 3 ]; then
        log "${ready_count} nodes Ready"
        break
    fi

    now=$(date +%s)
    if [ "${now}" -ge "${deadline}" ]; then
        err "timed out after ${NODE_READY_TIMEOUT}s waiting for 3 Ready nodes (saw ${ready_count})"
        kubectl --context "${KUBECONTEXT}" get nodes || true
        exit 1
    fi

    sleep 2
done

# ----- Helpers for the install phases -----------------------------------------
ensure_namespace() {
    local ns="$1"
    if kubectl --context "${KUBECONTEXT}" get namespace "${ns}" >/dev/null 2>&1; then
        log "namespace '${ns}' already exists"
    else
        log "creating namespace '${ns}'"
        kubectl --context "${KUBECONTEXT}" create namespace "${ns}"
    fi
}

# ----- Phase: ingress-nginx ---------------------------------------------------
log "installing ingress-nginx (NodePort overlay) via helm/ingress-nginx"
ensure_namespace ingress-nginx

log "running 'helm dependency update' for helm/ingress-nginx"
helm dependency update "${HELM_DIR}/ingress-nginx" >/dev/null

helm --kube-context "${KUBECONTEXT}" upgrade --install ingress-nginx \
    "${HELM_DIR}/ingress-nginx" \
    --namespace ingress-nginx \
    --values "${HELM_DIR}/ingress-nginx/values-local.yaml"

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for ingress-nginx controller to become available"
kubectl --context "${KUBECONTEXT}" wait \
    --for=condition=available \
    --timeout="${DEPLOY_READY_TIMEOUT}s" \
    --namespace ingress-nginx \
    deployment/ingress-nginx-controller

# ----- Phase: cert-manager ----------------------------------------------------
log "installing cert-manager via helm/cert-manager"
ensure_namespace cert-manager

log "running 'helm dependency update' for helm/cert-manager"
helm dependency update "${HELM_DIR}/cert-manager" >/dev/null

# Release name 'cert-manager' matches the subchart name, so the upstream
# fullname helper collapses to bare 'cert-manager'; deployments end up as
# cert-manager / cert-manager-webhook / cert-manager-cainjector (matches the
# wait loop below). Same trick applies to the ingress-nginx phase above.
helm --kube-context "${KUBECONTEXT}" upgrade --install cert-manager \
    "${HELM_DIR}/cert-manager" \
    --namespace cert-manager

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for cert-manager deployments to become available"
# The webhook in particular needs a few seconds to register before a
# ClusterIssuer apply will succeed — wait for all three core deployments.
for dep in cert-manager cert-manager-webhook cert-manager-cainjector; do
    kubectl --context "${KUBECONTEXT}" wait \
        --for=condition=available \
        --timeout="${DEPLOY_READY_TIMEOUT}s" \
        --namespace cert-manager \
        "deployment/${dep}"
done

# ----- Phase: self-signed ClusterIssuer ---------------------------------------
# Even after the webhook Deployment reports Available, the validating webhook's
# Service endpoints sometimes 503 for a few seconds. Retry a few times before
# giving up.
log "applying self-signed ClusterIssuer (local-selfsigned)"
issuer_attempts=0
max_issuer_attempts=10
until kubectl --context "${KUBECONTEXT}" apply \
        -f "${HELM_DIR}/cert-manager-issuer/cluster-issuer-local.yaml" 2>/dev/null; do
    issuer_attempts=$(( issuer_attempts + 1 ))
    if [ "${issuer_attempts}" -ge "${max_issuer_attempts}" ]; then
        err "failed to apply ClusterIssuer after ${max_issuer_attempts} attempts"
        kubectl --context "${KUBECONTEXT}" apply \
            -f "${HELM_DIR}/cert-manager-issuer/cluster-issuer-local.yaml"
        exit 1
    fi
    log "ClusterIssuer apply failed (attempt ${issuer_attempts}/${max_issuer_attempts}) — webhook likely still warming up; retrying in 3s"
    sleep 3
done

# ----- Phase: dev credentials (.env + Secrets) --------------------------------
# helm/local/.env holds the dev credential values for NATS and postgres. If a
# developer hasn't created one yet, bootstrap from .env.example so up.sh is
# a one-shot — no surprise "missing file" failures the first time it runs.
ENV_FILE="${SCRIPT_DIR}/.env"
ENV_EXAMPLE="${SCRIPT_DIR}/.env.example"
if [ ! -f "${ENV_FILE}" ]; then
    if [ -f "${ENV_EXAMPLE}" ]; then
        log "helm/local/.env not found — copying from .env.example (dev defaults)"
        cp "${ENV_EXAMPLE}" "${ENV_FILE}"
    else
        err "neither helm/local/.env nor helm/local/.env.example exists"
        exit 1
    fi
fi

log "sourcing dev credentials from helm/local/.env"
# shellcheck disable=SC1090
set -a; . "${ENV_FILE}"; set +a

: "${NATS_USER:?NATS_USER not set in helm/local/.env}"
: "${NATS_PASSWORD:?NATS_PASSWORD not set in helm/local/.env}"
: "${POSTGRES_USER:?POSTGRES_USER not set in helm/local/.env}"
: "${POSTGRES_DB:?POSTGRES_DB not set in helm/local/.env}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD not set in helm/local/.env}"
: "${JWT_SECRET:?JWT_SECRET not set in helm/local/.env (add it; see .env.example)}"

# Compute a checksum of the NATS credentials so the meeting-api +
# rustlemania-{websocket,webtransport} Deployments roll their pods
# whenever NATS_USER or NATS_PASSWORD change.
#
# Why: the env vars are wired via secretKeyRef into the external
# `nats-credentials` Secret. Kubernetes resolves secretKeyRef at pod
# start, but changes to a Secret's content do NOT trigger a rollout on
# their own — `helm upgrade --install` is a no-op when the rendered
# Deployment spec is unchanged, so old pods keep running with stale
# (or empty) creds. Stamping this hash into podAnnotations forces a
# spec diff (and therefore a rollout) iff the credentials actually
# changed; steady-state re-runs of up.sh remain no-ops.
#
# Annotation key mirrors the bitnami/upstream `checksum/<secret-name>`
# convention. See bead vco-757 for the original integration gap.
NATS_CRED_CHECKSUM=$(printf '%s|%s' "${NATS_USER}" "${NATS_PASSWORD}" | sha256sum | awk '{print $1}')

apply_secret() {
    # apply_secret <name> <namespace> <key1=val1> [<key2=val2> ...]
    #
    # Renders a generic Secret via `kubectl create --dry-run=client -o yaml`
    # so the values never appear in process listings, then pipes through
    # `kubectl apply` for idempotency (create-or-update).
    local name="$1" ns="$2"; shift 2
    local args=()
    local kv
    for kv in "$@"; do
        args+=( "--from-literal=${kv}" )
    done
    kubectl --context "${KUBECONTEXT}" -n "${ns}" create secret generic "${name}" \
        "${args[@]}" --dry-run=client -o yaml \
        | kubectl --context "${KUBECONTEXT}" -n "${ns}" apply -f - >/dev/null
}

log "applying nats-credentials Secret to namespace 'default'"
apply_secret nats-credentials default \
    "user=${NATS_USER}" \
    "password=${NATS_PASSWORD}"

log "applying postgres-credentials Secret to namespace 'default'"
# The rewritten helm/postgres chart (rustlemania-postgres 1.1.0, self-
# contained postgres:16-alpine — no bitnami subchart) reads
# POSTGRES_PASSWORD from the existingSecret using key `password` (the
# default for `auth.secretKeys.userPasswordKey`). We additionally populate
# `postgres-password` with the same value for forward compatibility with
# any production-style consumer that reads the superuser key — the local
# stack only has one human-facing role, so one rotation point is enough.
apply_secret postgres-credentials default \
    "postgres-password=${POSTGRES_PASSWORD}" \
    "password=${POSTGRES_PASSWORD}"

log "applying jwt-secret Secret to namespace 'default'"
# Consumed by meeting-api (signs room access tokens) and
# rustlemania-{websocket,webtransport} (verify them). The default
# values.yaml on all three charts references this Secret name with
# key `secret`.
apply_secret jwt-secret default \
    "secret=${JWT_SECRET}"

# ----- Phase: NATS ------------------------------------------------------------
log "installing NATS via helm/global/local/nats"

log "running 'helm dependency update' for helm/global/local/nats"
helm dependency update "${HELM_DIR}/global/local/nats" >/dev/null

# Inject the user/password via --set-string so the credentials stay out of
# values.yaml.
#
# Field path: the upstream nats helm chart (0.19.x) puts basic-auth users at
# `auth.basic.users[]` (NOT `auth.users[]`), and `auth` is a TOP-LEVEL key of
# the subchart — a SIBLING of the chart's own `nats:` block, not a child of
# it. The wrapper subchart alias is `nats`, so the full path from this
# wrapper chart's perspective is `nats.auth.basic.users[0].{user,password}`.
#
# Earlier versions of this script used `nats.nats.auth.users[0].*`, which is
# wrong on BOTH counts (extra `nats.` prefix AND missing `.basic`) — the
# values silently fell on the floor, the rendered nats.conf had no
# `authorization {}` block, and the server listened unauthenticated. See
# helm/global/local/nats/values.yaml for the full subchart-path convention
# writeup, and bead vco-ciu for the audit trail.
#
# NOTE: the production phase-c script at
# sfu-update/audits/nats-auth-phase-c-enable-nats-auth.sh uses the same
# (wrong) path pattern as the old local code did. It is OUT OF SCOPE for
# this commit — tracked as bead vco-k2r for a separate fix after the
# local stack is verified end-to-end. Do NOT copy the path style here
# back to prod without re-rendering against the prod chart version first.
helm --kube-context "${KUBECONTEXT}" upgrade --install nats \
    "${HELM_DIR}/global/local/nats" \
    --namespace default \
    --set-string "nats.auth.basic.users[0].user=${NATS_USER}" \
    --set-string "nats.auth.basic.users[0].password=${NATS_PASSWORD}"

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for the NATS StatefulSet to become ready"
kubectl --context "${KUBECONTEXT}" -n default rollout status statefulset/nats \
    --timeout="${DEPLOY_READY_TIMEOUT}s"

# ----- Phase: postgres --------------------------------------------------------
log "installing postgres via helm/postgres (values-local overlay)"

# rustlemania-postgres 1.1.0 is a thin, self-contained chart (no subcharts);
# `helm dependency update` is intentionally absent.
helm --kube-context "${KUBECONTEXT}" upgrade --install postgres \
    "${HELM_DIR}/postgres" \
    --namespace default \
    --values "${HELM_DIR}/postgres/values-local.yaml" \
    --set "auth.username=${POSTGRES_USER}" \
    --set "auth.database=${POSTGRES_DB}"

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for the postgres StatefulSet to become ready"
# Chart names the StatefulSet `{{ .Release.Name }}-postgresql`, so with
# release name `postgres` it lands as `postgres-postgresql`.
kubectl --context "${KUBECONTEXT}" -n default rollout status statefulset/postgres-postgresql \
    --timeout="${DEPLOY_READY_TIMEOUT}s"

# ----- Phase: build + push + k3d-import app images ----------------------------
#
# Two images, both built from the repo-root Dockerfiles with the whole
# tree as build context. Tags are :dev — explicit, not :latest, so the
# in-cluster image reference is stable and registry caches don't serve
# stale builds.
#
# `docker push` makes the image available via the registry running at
# localhost:5000 (k3d-managed container `videocall-local-registry`).
# `k3d image import` side-loads the image directly into every k3s node,
# which is the fast path for warm-restart workflows — the kubelets
# never need to talk to the registry.
LOCAL_REGISTRY="localhost:${REGISTRY_PORT}"
MEETING_API_IMAGE="${LOCAL_REGISTRY}/videocall-meeting-api:dev"
MEDIA_SERVER_IMAGE="${LOCAL_REGISTRY}/videocall-media-server:dev"

# Build metadata baked into the image labels (matches the convention in
# the production CI: GIT_SHA / GIT_BRANCH / BUILD_TIMESTAMP).
GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
GIT_BRANCH="$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
BUILD_TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

build_push_import() {
    # build_push_import <image-tag> <dockerfile-relpath>
    local tag="$1" dockerfile="$2"
    log "building ${tag} from ${dockerfile} (context: ${REPO_ROOT})"
    docker build \
        --tag "${tag}" \
        --file "${REPO_ROOT}/${dockerfile}" \
        --build-arg "GIT_SHA=${GIT_SHA}" \
        --build-arg "GIT_BRANCH=${GIT_BRANCH}" \
        --build-arg "BUILD_TIMESTAMP=${BUILD_TIMESTAMP}" \
        "${REPO_ROOT}"
    log "pushing ${tag} to local registry"
    docker push "${tag}"
    log "k3d image import ${tag} -> cluster '${CLUSTER_NAME}'"
    k3d image import "${tag}" --cluster "${CLUSTER_NAME}"
}

build_push_import "${MEETING_API_IMAGE}" "Dockerfile.meeting-api"
build_push_import "${MEDIA_SERVER_IMAGE}" "Dockerfile.actix"

# ----- Phase: WebTransport TLS Certificate ------------------------------------
#
# The webtransport pod reads its TLS material from a Secret mounted at
# /certs (see helm/rustlemania-webtransport/values-local.yaml,
# tlsSecret: transport-videocall-local-tls). Provision it via
# cert-manager BEFORE the helm install so the pod starts with the
# Secret in place.
log "applying WebTransport TLS Certificate (cert-manager)"
kubectl --context "${KUBECONTEXT}" apply \
    -f "${SCRIPT_DIR}/manifests/webtransport-certificate.yaml"

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for Certificate/transport-videocall-local to be Ready"
kubectl --context "${KUBECONTEXT}" -n default wait \
    --for=condition=Ready \
    --timeout="${DEPLOY_READY_TIMEOUT}s" \
    certificate/transport-videocall-local

# ----- Phase: meeting-api -----------------------------------------------------
# Compose the postgres URL from helm/local/.env and stash it in the
# `meeting-api-db` Secret (key: url). values-local.yaml references this
# via secretKeyRef, so the helm install carries no credential material
# on the command line and the env list order is no longer load-bearing.
log "applying meeting-api-db Secret to namespace 'default'"
DB_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres-postgresql:5432/${POSTGRES_DB}?sslmode=disable"
apply_secret meeting-api-db default \
    "url=${DB_URL}"

log "installing meeting-api via helm/meeting-api (values-local overlay)"
helm --kube-context "${KUBECONTEXT}" upgrade --install meeting-api \
    "${HELM_DIR}/meeting-api" \
    --namespace default \
    --values "${HELM_DIR}/meeting-api/values-local.yaml" \
    --set "podAnnotations.checksum/nats-credentials=${NATS_CRED_CHECKSUM}"

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for the meeting-api Deployment to roll out"
kubectl --context "${KUBECONTEXT}" -n default rollout status deployment/meeting-api \
    --timeout="${DEPLOY_READY_TIMEOUT}s"

# ----- Phase: rustlemania-websocket -------------------------------------------
log "installing rustlemania-websocket via helm/rustlemania-websocket (values-local overlay)"
helm --kube-context "${KUBECONTEXT}" upgrade --install rustlemania-websocket \
    "${HELM_DIR}/rustlemania-websocket" \
    --namespace default \
    --values "${HELM_DIR}/rustlemania-websocket/values-local.yaml" \
    --set "podAnnotations.checksum/nats-credentials=${NATS_CRED_CHECKSUM}"

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for the rustlemania-websocket Deployment to roll out"
kubectl --context "${KUBECONTEXT}" -n default rollout status deployment/rustlemania-websocket \
    --timeout="${DEPLOY_READY_TIMEOUT}s"

# ----- Phase: rustlemania-webtransport ----------------------------------------
log "installing rustlemania-webtransport via helm/rustlemania-webtransport (values-local overlay)"
helm --kube-context "${KUBECONTEXT}" upgrade --install rustlemania-webtransport \
    "${HELM_DIR}/rustlemania-webtransport" \
    --namespace default \
    --values "${HELM_DIR}/rustlemania-webtransport/values-local.yaml" \
    --set "podAnnotations.checksum/nats-credentials=${NATS_CRED_CHECKSUM}"

log "waiting up to ${DEPLOY_READY_TIMEOUT}s for the rustlemania-webtransport Deployment to roll out"
kubectl --context "${KUBECONTEXT}" -n default rollout status deployment/rustlemania-webtransport \
    --timeout="${DEPLOY_READY_TIMEOUT}s"

# ----- Phase: WebTransport /healthz Ingress -----------------------------------
#
# The webtransport chart's `loadbalancer.yaml` template hardcodes
# `type: LoadBalancer` (no service.type knob), so under k3d (klipper LB
# disabled) the LB sits Pending. The Service still has a ClusterIP and
# endpoints though, so we route /healthz through the nginx Ingress
# controller at transport.videocall.local:30443.
log "applying WebTransport /healthz Ingress (transport.videocall.local)"
kubectl --context "${KUBECONTEXT}" apply \
    -f "${SCRIPT_DIR}/manifests/webtransport-health-ingress.yaml"

# ----- Emit machine-readable handles for downstream scripts -------------------
log "cluster ready"
echo "KUBECONTEXT=${KUBECONTEXT}"
echo "REGISTRY=localhost:${REGISTRY_PORT}"
