#!/usr/bin/env bash
#
# helm/local/validate-nats.sh — verify the local NATS install enforces auth.
#
# Mirrors the production validation in
# sfu-update/audits/nats-auth-phase-d-validate.sh against the local k3d
# cluster. Two probes:
#   1. nats sub WITHOUT credentials  → must be refused
#   2. nats sub WITH credentials     → must succeed
#
# Both probes run inside the cluster (so `nats:4222` resolves via the
# Service that the chart created in the `default` namespace). The probes
# use `kubectl exec` into the deps-natsbox pod that the upstream chart
# provisions when `natsbox.enabled: true`.
#
# Usage:
#   ./helm/local/validate-nats.sh
#
# Exits 0 only if both probes match the expected outcome.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"
NAMESPACE="${NAMESPACE:-default}"
NATS_HOST="${NATS_HOST:-nats:4222}"

LOG_PREFIX="[validate-nats.sh]"
log() { echo "${LOG_PREFIX} $*"; }
err() { echo "${LOG_PREFIX} ERROR: $*" >&2; }

# Source dev credentials from the same .env that up.sh uses.
ENV_FILE="${SCRIPT_DIR}/.env"
if [ ! -f "${ENV_FILE}" ]; then
    err "helm/local/.env not found — run helm/local/up.sh first"
    exit 1
fi
# shellcheck disable=SC1090
set -a; . "${ENV_FILE}"; set +a
: "${NATS_USER:?NATS_USER not set in helm/local/.env}"
: "${NATS_PASSWORD:?NATS_PASSWORD not set in helm/local/.env}"

# Resolve the natsbox pod the chart installs alongside the server.
NATSBOX_POD=$(kubectl --context "${KUBECONTEXT}" -n "${NAMESPACE}" \
    get pods -l app=nats-box -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -z "${NATSBOX_POD}" ]; then
    err "no pod matched selector 'app=nats-box' in namespace '${NAMESPACE}'."
    err "Check: kubectl get pods -n ${NAMESPACE} --show-labels"
    exit 1
fi
log "using nats-box pod: ${NATSBOX_POD}"

# Run `nats sub` with a short timeout. The CLI exits non-zero on auth
# refusal; on success it would block forever, so we set NATS_TIMEOUT and
# accept the timeout exit as the success signal.
probe() {
    local label="$1" url="$2" expect="$3"
    log "probe '${label}' (expect: ${expect})"
    set +e
    out=$(kubectl --context "${KUBECONTEXT}" -n "${NAMESPACE}" exec "${NATSBOX_POD}" -- \
        timeout 3 nats --server="${url}" sub --count=1 'validate.>' 2>&1)
    rc=$?
    set -e
    echo "${out}" | head -3 | sed "s/^/${LOG_PREFIX}   /"

    case "${expect}" in
        refused)
            # nats CLI exits non-zero AND the output mentions Authorization
            # Violation (or a TCP refusal). We only fail if it stayed
            # connected — that would mean auth is off.
            if [ "${rc}" -eq 0 ]; then
                err "FAIL: probe '${label}' expected refusal but nats sub returned success"
                return 1
            fi
            if ! echo "${out}" | grep -qiE 'authorization|unauthorized|auth required|nats: ([^a-z]|$)'; then
                err "FAIL: probe '${label}' exited non-zero (${rc}) but output did not look like an auth refusal"
                return 1
            fi
            ;;
        success)
            # `timeout 3` returns exit code 124 on timeout (which is the
            # expected outcome — we connected, subscribed, and waited for a
            # message that never arrived). Treat 124 as success. Other
            # non-zero codes indicate the connection was rejected.
            if [ "${rc}" -ne 0 ] && [ "${rc}" -ne 124 ]; then
                err "FAIL: probe '${label}' expected success but nats sub exited ${rc}"
                return 1
            fi
            if echo "${out}" | grep -qiE 'authorization violation|unauthorized'; then
                err "FAIL: probe '${label}' expected success but got an authorization violation"
                return 1
            fi
            ;;
    esac
}

probe "no-creds" "nats://${NATS_HOST}" refused
probe "with-creds" "nats://${NATS_USER}:${NATS_PASSWORD}@${NATS_HOST}" success

log "NATS auth is enforced on ${KUBECONTEXT}"
