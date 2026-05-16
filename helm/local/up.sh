#!/usr/bin/env bash
#
# helm/local/up.sh — v1 local k3d cluster bringup for the videocall stack.
#
# Scope (v1):
#   1. Create the k3d cluster (idempotent).
#   2. Install ingress-nginx (NodePort) into namespace ingress-nginx.
#   3. Install cert-manager (with CRDs) into namespace cert-manager.
#   4. Apply a self-signed ClusterIssuer (`local-selfsigned`) for
#      *.videocall.local certs.
#
# Out of scope (later beads): NATS, postgres, meeting-api, SFU, ingresses
# for app services. See helm/local/README.md.
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

# Resolve the helm/ directory relative to this script so the script can be
# invoked from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

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
    k3d cluster create "${CLUSTER_NAME}" \
        --servers 1 \
        --agents 2 \
        --registry-create "${REGISTRY_NAME}:0.0.0.0:${REGISTRY_PORT}" \
        --k3s-arg "--disable=traefik@server:0" \
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

# ----- Emit machine-readable handles for downstream scripts -------------------
log "cluster ready"
echo "KUBECONTEXT=${KUBECONTEXT}"
echo "REGISTRY=localhost:${REGISTRY_PORT}"
