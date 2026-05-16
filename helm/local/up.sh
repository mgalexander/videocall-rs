#!/usr/bin/env bash
#
# helm/local/up.sh — v0 local k3d cluster bringup for the videocall stack.
#
# Scope: cluster bringup ONLY. No app services, no ingress, no cert-manager.
# Subsequent beads (vco-ow8.3, vco-ow8.4, ...) layer those on top.
#
# Idempotent: if the cluster already exists, this script is a no-op aside from
# printing the kubeconfig context and registry endpoint for downstream scripts.

set -euo pipefail

# ----- Configurable knobs (env-overridable) -----------------------------------
CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
REGISTRY_NAME="${REGISTRY_NAME:-videocall-local-registry}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"

# How long (seconds) to wait for all 3 nodes to report Ready after create.
NODE_READY_TIMEOUT="${NODE_READY_TIMEOUT:-120}"

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

# ----- Emit machine-readable handles for downstream scripts -------------------
log "cluster ready"
echo "KUBECONTEXT=${KUBECONTEXT}"
echo "REGISTRY=localhost:${REGISTRY_PORT}"
