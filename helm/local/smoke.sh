#!/usr/bin/env bash
#
# helm/local/smoke.sh — end-to-end smoke test for the local k3d stack.
#
# Chains the existing scripts in helm/local/ into a single
# "stand-up, validate, tear-down" run so an operator (or CI) can
# verify the whole stack with one command:
#
#   ./helm/local/smoke.sh
#
# Phases:
#   1. up.sh             — k3d cluster + ingress-nginx + cert-manager
#                          + ClusterIssuer + dev Secrets + NATS + postgres
#                          + meeting-api + SFU pods.
#   2. validate-nats.sh  — local equivalent of
#                          sfu-update/audits/nats-auth-phase-d-validate.sh.
#                          Probes NATS without/with creds against
#                          KUBECTX=k3d-videocall-local.
#   3. validate-app.sh   — curl https://transport.videocall.local/healthz
#                          AND grep `auth=on` from each app's logs.
#   4. down.sh           — tear the cluster + registry down.
#
# Flags:
#   --no-teardown   keep the cluster up after validation (skip down.sh)
#   --no-bringup    skip up.sh (cluster already running)
#   --no-app        skip validate-app.sh (only run nats probe)
#
# Exit codes:
#   0  every requested phase succeeded
#   1  at least one phase failed; cluster is left as-is for inspection
#      (down.sh is NOT auto-run when an earlier phase fails)
#
# Preflight: same as up.sh — docker, kubectl, k3d, helm on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LOG_PREFIX="[smoke.sh]"
log() { echo "${LOG_PREFIX} $*"; }
err() { echo "${LOG_PREFIX} ERROR: $*" >&2; }

# ----- Flags ------------------------------------------------------------------
DO_BRINGUP=1
DO_APP_VALIDATE=1
DO_TEARDOWN=1
for arg in "$@"; do
    case "${arg}" in
        --no-teardown) DO_TEARDOWN=0 ;;
        --no-bringup)  DO_BRINGUP=0 ;;
        --no-app)      DO_APP_VALIDATE=0 ;;
        -h|--help)
            # Print the header comment block (lines 2..33 — through the
            # 'Preflight' line, stopping before `set -euo pipefail`).
            sed -n '2,33p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            err "unknown flag: ${arg}"
            err "use --help to see supported flags"
            exit 1
            ;;
    esac
done

# ----- Phase 1: bring the cluster up + deploy the stack -----------------------
if [ "${DO_BRINGUP}" -eq 1 ]; then
    log "phase 1/4: ./helm/local/up.sh"
    "${SCRIPT_DIR}/up.sh"
else
    log "phase 1/4: SKIPPED (--no-bringup)"
fi

# ----- Phase 2: NATS auth probe -----------------------------------------------
log "phase 2/4: ./helm/local/validate-nats.sh"
"${SCRIPT_DIR}/validate-nats.sh"

# ----- Phase 3: app /healthz + auth=on probe ----------------------------------
if [ "${DO_APP_VALIDATE}" -eq 1 ]; then
    log "phase 3/4: ./helm/local/validate-app.sh"
    "${SCRIPT_DIR}/validate-app.sh"
else
    log "phase 3/4: SKIPPED (--no-app)"
fi

# ----- Phase 4: teardown ------------------------------------------------------
if [ "${DO_TEARDOWN}" -eq 1 ]; then
    log "phase 4/4: ./helm/local/down.sh"
    "${SCRIPT_DIR}/down.sh"
else
    log "phase 4/4: SKIPPED (--no-teardown) — cluster left up for inspection"
fi

log "smoke OK"
