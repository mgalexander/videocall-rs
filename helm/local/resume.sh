#!/usr/bin/env bash
#
# helm/local/resume.sh — wake the local k3d videocall cluster from pause.
#
# Scope: starts the cluster's node containers via `k3d cluster start`, then
# waits for all 3 nodes to report Ready (same wait loop as up.sh), and emits
# the same machine-readable handles on stdout so downstream scripts can
# consume resume.sh output identically to up.sh output.
#
# Idempotent:
#   - If the cluster is already running, log and skip the start — but still
#     wait for Ready nodes and emit the KUBECONTEXT/REGISTRY handles.
#
# Not-OK case:
#   - If the cluster does not exist, error out with a hint pointing to up.sh.
#     You can't resume something that was never created.

set -euo pipefail

# ----- Configurable knobs (env-overridable) -----------------------------------
CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
REGISTRY_NAME="${REGISTRY_NAME:-videocall-local-registry}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"

# How long (seconds) to wait for all 3 nodes to report Ready after start.
NODE_READY_TIMEOUT="${NODE_READY_TIMEOUT:-120}"

LOG_PREFIX="[resume.sh]"

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

# ----- Existence check --------------------------------------------------------
if ! k3d cluster list "${CLUSTER_NAME}" >/dev/null 2>&1; then
    err "cluster '${CLUSTER_NAME}' does not exist — cannot resume"
    err "hint: create it first with ./helm/local/up.sh"
    exit 1
fi

# ----- Start (idempotent in k3d 5.x) ------------------------------------------
# `k3d cluster start` is a fast no-op against an already-running cluster, and
# crucially it also recovers from the "server up but some agents down" partial
# state — which a servers-only running-count check would mis-classify as
# "already running" and skip, leaving the Ready wait below to time out at 120s
# instead of healing.
log "starting k3d cluster '${CLUSTER_NAME}' (no-op if already running)"
k3d cluster start "${CLUSTER_NAME}"

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
