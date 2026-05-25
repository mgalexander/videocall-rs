#!/usr/bin/env bash
#
# helm/local/pause.sh — hibernate the local k3d videocall cluster.
#
# Scope: stops the cluster's node containers via `k3d cluster stop`, leaving
# cluster state on disk so `resume.sh` can wake it back up without losing
# installed components or pushed images.
#
# Idempotent:
#   - If the cluster is already stopped, log and exit 0.
#   - If the cluster does not exist, log and exit 0 (no error — we treat
#     "nothing to pause" as success, same shape as down.sh).

set -euo pipefail

# ----- Configurable knobs (env-overridable) -----------------------------------
CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
REGISTRY_NAME="${REGISTRY_NAME:-videocall-local-registry}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"

LOG_PREFIX="[pause.sh]"

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

require_bin k3d \
    "Install k3d. macOS: brew install k3d. Or: curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash"

# ----- Existence check --------------------------------------------------------
if ! k3d cluster list "${CLUSTER_NAME}" >/dev/null 2>&1; then
    log "cluster '${CLUSTER_NAME}' does not exist — nothing to pause"
    exit 0
fi

# ----- Running-state check ----------------------------------------------------
# k3d reports per-cluster server/agent counts as "running/total" (e.g. "1/1",
# "0/1"). If servers running == 0, the cluster is already stopped.
servers_running=$(k3d cluster list "${CLUSTER_NAME}" --no-headers 2>/dev/null \
    | awk '{print $2}' \
    | awk -F/ '{print $1}')

if [ "${servers_running:-0}" = "0" ]; then
    log "cluster '${CLUSTER_NAME}' is already stopped — nothing to pause"
    exit 0
fi

# ----- Stop -------------------------------------------------------------------
log "stopping k3d cluster '${CLUSTER_NAME}' (state preserved on disk; resume with resume.sh)"
k3d cluster stop "${CLUSTER_NAME}"
log "cluster '${CLUSTER_NAME}' paused (context '${KUBECONTEXT}' will be unreachable until resume)"
