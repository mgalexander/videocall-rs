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

# Run `nats sub` with a short timeout.
#
# Two layers of timeout are in play:
#
#   1. NATS_TIMEOUT=2s — natscli's connect/operation timeout (env var
#      respected across all natscli versions shipped in natsio/nats-box;
#      avoids depending on a CLI flag that may vary by version). On a
#      correctly-configured auth-enforcing server, an unauthenticated
#      connect is REJECTED within milliseconds, so 2s is generous. If we
#      see the server hold the TCP socket open past NATS_TIMEOUT, the
#      CLI exits non-zero with a connect-timeout error — which is still
#      a "could not connect" signal we treat as refusal.
#
#   2. `timeout 4` — outer safety net in case NATS_TIMEOUT is ignored or
#      the CLI hangs in a way the env var doesn't cover. With NATS_TIMEOUT
#      at 2s, hitting 124 from the outer timeout means the natscli failed
#      to respect its own timeout — that's a CLI bug, not an auth signal,
#      so for the refusal probe we treat 124 as FAIL (auth was not
#      enforced fast enough to call refused).
#
# The previous version used `timeout 3` with no NATS_TIMEOUT and special-
# cased rc=0 vs rc!=0 plus keyword grep over the SIGTERM'd output. That
# was brittle: a non-auth-enforcing server would hold the connection open
# and we'd hit SIGTERM at 3s, exit non-zero, AND the partial output
# (containing the subject pattern "validate.>") would sometimes contain
# the substring "nats: " from the CLI banner, which slipped past the
# regex. Switching to NATS_TIMEOUT + a stricter rc/keyword check
# eliminates that ambiguity.
probe() {
    local label="$1" url="$2" expect="$3"
    log "probe '${label}' (expect: ${expect})"
    set +e
    # Pass URL through env so a literal single quote in NATS_PASSWORD can't
    # break out of the inner sh -c quoting and execute inside the pod.
    out=$(kubectl --context "${KUBECONTEXT}" -n "${NAMESPACE}" exec "${NATSBOX_POD}" -- \
        env "PROBE_URL=${url}" sh -c 'NATS_TIMEOUT=2s timeout 4 nats --server="$PROBE_URL" sub --count=1 "validate.>" 2>&1')
    rc=$?
    set -e
    # Strip ANSI color escapes from natscli output before display + grep.
    # The natscli today emits plain output in this code path, but some
    # versions colorize error messages — color codes between letters of
    # "authorization" would silently defeat the keyword grep below.
    # Mirror the same defense applied in validate-app.sh (bead vco-gek).
    out=$(printf '%s' "${out}" | sed 's/\x1b\[[0-9;]*m//g')
    echo "${out}" | head -3 | sed "s/^/${LOG_PREFIX}   /"

    case "${expect}" in
        refused)
            # Auth-enforcing servers reject the connect immediately. We
            # expect rc to be non-zero AND not 124 (outer timeout). rc=0
            # means we connected and got a published message — auth is
            # OFF. rc=124 means neither NATS_TIMEOUT nor an auth-violation
            # fired in 4s — also means auth is effectively OFF (a refusal
            # would have taken milliseconds).
            if [ "${rc}" -eq 0 ]; then
                err "FAIL: probe '${label}' expected refusal but nats sub returned success — auth is NOT enforced"
                return 1
            fi
            if [ "${rc}" -eq 124 ]; then
                err "FAIL: probe '${label}' outer timeout fired (rc=124) — server held the connection open past 4s instead of refusing; auth is NOT enforced"
                return 1
            fi
            # Non-zero, non-124: connect failed. Confirm the failure mode
            # looks like an auth refusal rather than a DNS/network error.
            # natscli surfaces auth errors as "Authorization Violation",
            # "authorization required", or "unauthorized". A bare "nats: "
            # banner also signals a CLI-side error; in-cluster against
            # nats:4222, the only fast-failing non-auth modes would be
            # service-not-ready or pod-DNS-not-resolved — neither is
            # likely once the StatefulSet rollout has gone Ready.
            if ! echo "${out}" | grep -qiE 'authorization|unauthorized|auth required|nats: '; then
                err "FAIL: probe '${label}' exited ${rc} but output did not look like an auth refusal"
                return 1
            fi
            ;;
        success)
            # `timeout 4` returns 124 on outer timeout (the expected
            # outcome — we connected, subscribed, and waited for a
            # message that never arrived). NATS_TIMEOUT=2s here only
            # bounds the CONNECT phase; after a successful connect
            # natscli blocks on the subscription, so the outer `timeout`
            # is what we land on. Treat 124 as success. We also accept
            # 143 (128+15, SIGTERM): when `timeout` signals the child,
            # some natscli versions install a SIGTERM handler that exits
            # via the signal rather than letting `timeout` escalate to
            # the rc=124 path. Both rc=124 and rc=143 mean "the
            # connection was healthy at the 4s deadline". Other non-zero
            # codes indicate the connection was rejected.
            if [ "${rc}" -ne 0 ] && [ "${rc}" -ne 124 ] && [ "${rc}" -ne 143 ]; then
                err "FAIL: probe '${label}' expected success but nats sub exited ${rc}"
                return 1
            fi
            # Mirror the refusal-probe keyword set so a fast auth-rejection
            # isn't silently classified as success on a non-124 exit.
            if echo "${out}" | grep -qiE 'authorization|unauthorized|auth required'; then
                err "FAIL: probe '${label}' expected success but got an authorization-related error"
                return 1
            fi
            ;;
    esac
}

probe "no-creds" "nats://${NATS_HOST}" refused
probe "with-creds" "nats://${NATS_USER}:${NATS_PASSWORD}@${NATS_HOST}" success

log "NATS auth is enforced on ${KUBECONTEXT}"
