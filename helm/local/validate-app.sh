#!/usr/bin/env bash
#
# helm/local/validate-app.sh — verify the local meeting-api + SFU
# install matches the vco-ow8.5 acceptance criteria.
#
# Two probes:
#   1. HTTP GET https://transport.videocall.local/healthz → 200
#      (via the local nginx Ingress on NodePort 30443; the k3d cluster
#      created by helm/local/up.sh publishes 30443 → host:30443).
#   2. kubectl logs for one pod each of meeting-api, rustlemania-
#      websocket, rustlemania-webtransport — grep for the
#      `auth=on` line emitted by actix-api's nats_connect helper.
#
# Usage:
#   ./helm/local/validate-app.sh
#
# Exits 0 only if all probes pass.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"
NAMESPACE="${NAMESPACE:-default}"

# The ingress controller listens on the host at this port (k3d publishes
# the NodePort via the --port loadbalancer mapping in up.sh).
INGRESS_HTTPS_PORT="${INGRESS_HTTPS_PORT:-30443}"

LOG_PREFIX="[validate-app.sh]"
log() { echo "${LOG_PREFIX} $*"; }
err() { echo "${LOG_PREFIX} ERROR: $*" >&2; }

# ----- /etc/hosts nudge (non-mutating) ----------------------------------------
HOSTS_NEEDED=(
    "api.videocall.local"
    "ws.videocall.local"
    "transport.videocall.local"
)
missing_hosts=()
for h in "${HOSTS_NEEDED[@]}"; do
    if ! getent hosts "${h}" 2>/dev/null | grep -q '^127\.0\.0\.1\b'; then
        missing_hosts+=( "${h}" )
    fi
done
if [ "${#missing_hosts[@]}" -gt 0 ]; then
    log "the following hostnames are not mapped to 127.0.0.1:"
    for h in "${missing_hosts[@]}"; do
        log "  - ${h}"
    done
    log "add this single line to /etc/hosts to fix:"
    log "  127.0.0.1 ${HOSTS_NEEDED[*]}"
    log "(validate-app.sh does not modify /etc/hosts itself.)"
fi

# ----- Probe 1: /healthz via the ingress --------------------------------------
HEALTHZ_URL="https://transport.videocall.local:${INGRESS_HTTPS_PORT}/healthz"
log "probe: GET ${HEALTHZ_URL}"
# -k: accept the self-signed cert (local-selfsigned ClusterIssuer).
# --resolve covers the case where /etc/hosts isn't set yet.
set +e
http_code=$(curl --silent --output /dev/null --write-out '%{http_code}' \
    --insecure \
    --resolve "transport.videocall.local:${INGRESS_HTTPS_PORT}:127.0.0.1" \
    --max-time 5 \
    "${HEALTHZ_URL}")
rc=$?
set -e
if [ "${rc}" -ne 0 ]; then
    err "curl failed (exit ${rc}); is the ingress controller reachable on :${INGRESS_HTTPS_PORT}?"
    err "(if the cluster was created before vco-ow8.5, the NodePort isn't"
    err " published to the host. Recreate with helm/local/down.sh + up.sh.)"
    exit 1
fi
if [ "${http_code}" != "200" ]; then
    err "FAIL: ${HEALTHZ_URL} returned HTTP ${http_code} (expected 200)"
    exit 1
fi
log "probe '/healthz' OK (HTTP 200)"

# ----- Probe 2: NATS auth=on in each app's logs -------------------------------
#
# actix-api's nats_connect helper logs a line shaped like:
#     connecting to NATS at <url> auth=on tls=off
# when NATS_USER/NATS_PASSWORD env are populated (per the vco-ow8.5
# wiring + sfu-update/audits/nats-acl-audit.md). Grep the most recent
# pod logs for that line on each of the three apps.
#
# Rollout timing across the three apps is not synchronized: a freshly
# rolled pod may not have logged the NATS connect line yet when we
# probe. Retry up to AUTH_ON_MAX_ATTEMPTS × AUTH_ON_SLEEP_SECS seconds
# (45s default) per pod, re-resolving the pod name each iteration so
# we follow a roll if it happens mid-probe.
AUTH_ON_MAX_ATTEMPTS="${AUTH_ON_MAX_ATTEMPTS:-15}"
AUTH_ON_SLEEP_SECS="${AUTH_ON_SLEEP_SECS:-3}"

check_auth_on() {
    # check_auth_on <app-label> <selector>
    local label="$1" selector="$2"
    local pod="" out="" last_out=""
    local attempt=0
    log "checking ${label} for 'auth=on' (up to $(( AUTH_ON_MAX_ATTEMPTS * AUTH_ON_SLEEP_SECS ))s)"
    while [ "${attempt}" -lt "${AUTH_ON_MAX_ATTEMPTS}" ]; do
        attempt=$(( attempt + 1 ))
        # Re-resolve the pod name every iteration in case a rollout
        # replaced it since the last attempt.
        pod=$(kubectl --context "${KUBECONTEXT}" -n "${NAMESPACE}" \
            get pods -l "${selector}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
        if [ -z "${pod}" ]; then
            log "  attempt ${attempt}/${AUTH_ON_MAX_ATTEMPTS}: no pod yet for ${label} (selector: ${selector}); retrying in ${AUTH_ON_SLEEP_SECS}s"
            sleep "${AUTH_ON_SLEEP_SECS}"
            continue
        fi
        set +e
        out=$(kubectl --context "${KUBECONTEXT}" -n "${NAMESPACE}" logs "${pod}" --tail=200 2>&1)
        set -e
        last_out="${out}"
        if echo "${out}" | grep -q 'auth=on'; then
            log "${pod}: auth=on"
            return 0
        fi
        log "  attempt ${attempt}/${AUTH_ON_MAX_ATTEMPTS}: ${label} pod ${pod} has no 'auth=on' yet; retrying in ${AUTH_ON_SLEEP_SECS}s"
        sleep "${AUTH_ON_SLEEP_SECS}"
    done
    err "FAIL: ${label} did not log 'auth=on' within $(( AUTH_ON_MAX_ATTEMPTS * AUTH_ON_SLEEP_SECS ))s (last pod: ${pod:-<none>})"
    err "last 5 log lines:"
    echo "${last_out}" | tail -5 | sed "s/^/${LOG_PREFIX}   /" >&2
    return 1
}

# The three charts share a selector convention: the chart's
# `app.kubernetes.io/name` label is the chart name (meeting-api,
# rustlemania-websocket, rustlemania-webtransport).
check_auth_on "meeting-api" "app.kubernetes.io/name=meeting-api"
check_auth_on "rustlemania-websocket" "app.kubernetes.io/name=rustlemania-websocket"
check_auth_on "rustlemania-webtransport" "app.kubernetes.io/name=rustlemania-webtransport"

log "all probes passed: /healthz=200 and NATS auth=on across all three apps"
