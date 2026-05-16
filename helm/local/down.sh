#!/usr/bin/env bash
#
# helm/local/down.sh — full teardown of the local k3d videocall cluster.
#
# Scope: cluster teardown ONLY. Deletes the k3d cluster (which also removes
# the k3d-managed registry container created with `--registry-create` in up.sh).
# If a stray registry container is still around after cluster delete (e.g.
# because the cluster was already gone), this script removes it too so the
# next `up.sh` starts from a fully clean slate.
#
# Idempotent: if the cluster is already absent, this script is a no-op (logs
# a friendly line and exits 0).

set -euo pipefail

# ----- Configurable knobs (env-overridable) -----------------------------------
CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
REGISTRY_NAME="${REGISTRY_NAME:-videocall-local-registry}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"

LOG_PREFIX="[down.sh]"

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

# ----- Delete cluster (idempotent) --------------------------------------------
if k3d cluster list "${CLUSTER_NAME}" >/dev/null 2>&1; then
    log "deleting k3d cluster '${CLUSTER_NAME}' (this also removes the k3d-managed registry)"
    k3d cluster delete "${CLUSTER_NAME}"
    log "cluster '${CLUSTER_NAME}' deleted"
else
    log "cluster '${CLUSTER_NAME}' not present — nothing to delete"
fi

# ----- Sweep any stray registry container -------------------------------------
# `k3d cluster delete` removes the registry created via --registry-create, but
# if the cluster was already gone (or the registry was created standalone) the
# container can outlive the cluster. Remove it explicitly so up.sh can recreate
# it cleanly on port ${REGISTRY_PORT}.
#
# k3d prefixes registry container names with `k3d-`, so look for either form.
# `--type=container` scopes the inspect so a same-named docker *image* tag
# (perfectly legal) doesn't trigger a `docker rm` that would then fail.
for candidate in "k3d-${REGISTRY_NAME}" "${REGISTRY_NAME}"; do
    if docker inspect --type=container "${candidate}" >/dev/null 2>&1; then
        log "removing stray registry container '${candidate}'"
        docker rm -f "${candidate}" >/dev/null
    fi
done

log "teardown complete (context '${KUBECONTEXT}' is gone; registry on localhost:${REGISTRY_PORT} is gone)"
