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
#   --capture       structured capture mode. Writes per-phase stdout/stderr,
#                   host info, pod snapshots, and a report.json into
#                   sfu-update/audits/smoke-results/<utc-timestamp>/ — the
#                   absolute path is printed at completion. Without --capture
#                   the script keeps its original stdout passthrough behaviour.
#
# Exit codes:
#   0  every requested phase succeeded
#   1  at least one phase failed; cluster is left as-is for inspection
#      (down.sh is NOT auto-run when an earlier phase fails)
#
# Preflight: same as up.sh — docker, kubectl, k3d, helm on PATH.
#            --capture additionally needs `jq` on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LOG_PREFIX="[smoke.sh]"
log() { echo "${LOG_PREFIX} $*"; }
err() { echo "${LOG_PREFIX} ERROR: $*" >&2; }

# ----- Flags ------------------------------------------------------------------
DO_BRINGUP=1
DO_APP_VALIDATE=1
DO_TEARDOWN=1
DO_CAPTURE=0
for arg in "$@"; do
    case "${arg}" in
        --no-teardown) DO_TEARDOWN=0 ;;
        --no-bringup)  DO_BRINGUP=0 ;;
        --no-app)      DO_APP_VALIDATE=0 ;;
        --capture)     DO_CAPTURE=1 ;;
        -h|--help)
            # Print the header comment block (lines 2..39 — through the
            # 'Preflight' lines, stopping before `set -euo pipefail`).
            sed -n '2,39p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            err "unknown flag: ${arg}"
            err "use --help to see supported flags"
            exit 1
            ;;
    esac
done

# ----- Capture infrastructure (only when --capture) ---------------------------
#
# When --capture is on we write everything to a fresh results dir under
# sfu-update/audits/smoke-results/<utc-timestamp>/ and emit a report.json
# describing per-phase outcomes plus an overall pass/fail. A trap on EXIT
# finalises the report even if a phase aborts the run early — the overseer
# (per ADR-0008) is the one consuming this report, so it must exist on disk
# regardless of which phase blew up.

CAPTURE_DIR=""
REPORT_JSON=""
PHASES_JSONL=""
OVERALL_PASS=1

if [ "${DO_CAPTURE}" -eq 1 ]; then
    if ! command -v jq >/dev/null 2>&1; then
        err "--capture requires jq on PATH"
        exit 1
    fi
    REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    CAPTURE_TS="$(date -u +%Y%m%dT%H%M%SZ)"
    CAPTURE_DIR="${REPO_ROOT}/sfu-update/audits/smoke-results/${CAPTURE_TS}"
    mkdir -p "${CAPTURE_DIR}"
    REPORT_JSON="${CAPTURE_DIR}/report.json"
    PHASES_JSONL="${CAPTURE_DIR}/.phases.jsonl"
    : > "${PHASES_JSONL}"

    log "capture mode ON — results dir: ${CAPTURE_DIR}"

    # Host info snapshot. Best-effort; missing tools shouldn't fail capture
    # (the report.json is more important than a complete host-info dump).
    {
        echo "=== uname -a ==="
        uname -a 2>&1 || true
        echo
        echo "=== docker --version ==="
        docker --version 2>&1 || true
        echo
        echo "=== k3d version ==="
        k3d version 2>&1 || true
        echo
        echo "=== kubectl version --client ==="
        kubectl version --client 2>&1 || true
        echo
        echo "=== helm version ==="
        helm version 2>&1 || true
    } > "${CAPTURE_DIR}/host-info.txt" 2>&1 || true
fi

# finalize_report — write report.json from the JSONL records collected so far.
# Called via EXIT trap so the report is produced even on early abort.
finalize_report() {
    local rc
    rc=$?
    if [ "${DO_CAPTURE}" -ne 1 ]; then
        return "${rc}"
    fi
    if [ -z "${REPORT_JSON}" ] || [ ! -f "${PHASES_JSONL}" ]; then
        return "${rc}"
    fi
    local overall
    overall="true"
    if [ "${OVERALL_PASS}" -eq 0 ] || [ "${rc}" -ne 0 ]; then
        overall="false"
    fi
    local phases_arr
    phases_arr="$(jq -s '.' "${PHASES_JSONL}")"
    jq -n \
        --arg cluster_name "videocall-local" \
        --arg kubectx "${KUBECTX:-k3d-videocall-local}" \
        --arg results_dir "${CAPTURE_DIR}" \
        --arg host_info_path "host-info.txt" \
        --argjson phases "${phases_arr}" \
        --argjson overall_pass "${overall}" \
        --argjson script_exit_code "${rc}" \
        '{cluster_name: $cluster_name,
          kubectx: $kubectx,
          results_dir: $results_dir,
          host_info_path: $host_info_path,
          phases: $phases,
          overall_pass: $overall_pass,
          script_exit_code: $script_exit_code}' \
        > "${REPORT_JSON}" || true
    rm -f "${PHASES_JSONL}" || true
    log "capture: ${CAPTURE_DIR}"
    return "${rc}"
}
trap finalize_report EXIT

# record_phase NAME SKIPPED START_EPOCH END_EPOCH RC STDOUT_PATH STDERR_PATH
record_phase() {
    [ "${DO_CAPTURE}" -eq 1 ] || return 0
    local name="$1"
    local skipped="$2"
    local start_epoch="$3"
    local end_epoch="$4"
    local rc="$5"
    local stdout_path="$6"
    local stderr_path="$7"

    if [ "${skipped}" -eq 1 ]; then
        jq -nc \
            --arg phase "${name}" \
            '{phase: $phase, skipped: true, pass: null}' \
            >> "${PHASES_JSONL}"
        return 0
    fi

    local duration=$(( end_epoch - start_epoch ))
    local start_ts end_ts pass
    start_ts="$(date -u -d "@${start_epoch}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
        || date -u +%Y-%m-%dT%H:%M:%SZ)"
    end_ts="$(date -u -d "@${end_epoch}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
        || date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [ "${rc}" -eq 0 ]; then
        pass="true"
    else
        pass="false"
        OVERALL_PASS=0
    fi
    jq -nc \
        --arg phase "${name}" \
        --arg start_ts "${start_ts}" \
        --arg end_ts "${end_ts}" \
        --argjson duration_sec "${duration}" \
        --argjson exit_code "${rc}" \
        --arg stdout_path "${stdout_path}" \
        --arg stderr_path "${stderr_path}" \
        --argjson pass "${pass}" \
        '{phase: $phase,
          skipped: false,
          start_ts: $start_ts,
          end_ts: $end_ts,
          duration_sec: $duration_sec,
          exit_code: $exit_code,
          stdout_path: $stdout_path,
          stderr_path: $stderr_path,
          pass: $pass}' \
        >> "${PHASES_JSONL}"
}

# run_phase NAME CMD...
# In capture mode: tees stdout+stderr to per-phase files and records the run.
# In passthrough mode: just runs CMD (set -e still aborts on failure).
# Returns the wrapped command's exit code in both modes.
run_phase() {
    local name="$1"
    shift
    if [ "${DO_CAPTURE}" -ne 1 ]; then
        "$@"
        return $?
    fi
    local stdout_path="${CAPTURE_DIR}/${name}.stdout.txt"
    local stderr_path="${CAPTURE_DIR}/${name}.stderr.txt"
    local start_epoch end_epoch rc
    start_epoch="$(date -u +%s)"
    set +e
    "$@" > >(tee "${stdout_path}") 2> >(tee "${stderr_path}" >&2)
    rc=$?
    set -e
    end_epoch="$(date -u +%s)"
    record_phase "${name}" 0 "${start_epoch}" "${end_epoch}" \
        "${rc}" "${stdout_path}" "${stderr_path}"
    return "${rc}"
}

# snapshot_failed_pods LABEL
# Best-effort: dump non-Running pods in default ns plus tail=50 of their logs
# to failed-pods-<LABEL>.txt. No-op outside capture mode. Never fails the run.
snapshot_failed_pods() {
    [ "${DO_CAPTURE}" -eq 1 ] || return 0
    local label="$1"
    local out="${CAPTURE_DIR}/failed-pods-${label}.txt"
    {
        echo "=== Non-Running/Completed pods in default ns ==="
        kubectl get pods -n default --no-headers 2>/dev/null \
            | awk '$3 != "Running" && $3 != "Completed"' || true
        echo
        kubectl get pods -n default --no-headers 2>/dev/null \
            | awk '$3 != "Running" && $3 != "Completed" {print $1}' \
            | while read -r pod; do
                  [ -z "${pod}" ] && continue
                  echo "=== Logs (tail=50): ${pod} ==="
                  kubectl logs -n default "${pod}" --tail=50 2>&1 || true
                  echo
              done
    } > "${out}" 2>&1 || true
}

# ----- Phase 1: bring the cluster up + deploy the stack -----------------------
if [ "${DO_BRINGUP}" -eq 1 ]; then
    log "phase 1/4: ./helm/local/up.sh"
    if [ "${DO_CAPTURE}" -eq 1 ]; then
        set +e
        run_phase "up" "${SCRIPT_DIR}/up.sh"
        UP_RC=$?
        set -e
        if [ "${UP_RC}" -eq 0 ]; then
            kubectl get pods -n default -o wide \
                > "${CAPTURE_DIR}/pods-postup.txt" 2>&1 || true
        fi
        if [ "${UP_RC}" -ne 0 ]; then
            err "phase 1 (up) failed with rc=${UP_RC}"
            exit "${UP_RC}"
        fi
    else
        run_phase "up" "${SCRIPT_DIR}/up.sh"
    fi
else
    log "phase 1/4: SKIPPED (--no-bringup)"
    record_phase "up" 1 0 0 0 "" ""
fi

# ----- Phase 2: NATS auth probe -----------------------------------------------
log "phase 2/4: ./helm/local/validate-nats.sh"
if [ "${DO_CAPTURE}" -eq 1 ]; then
    set +e
    run_phase "validate-nats" "${SCRIPT_DIR}/validate-nats.sh"
    NATS_RC=$?
    set -e
    snapshot_failed_pods "validate-nats"
    if [ "${NATS_RC}" -ne 0 ]; then
        err "phase 2 (validate-nats) failed with rc=${NATS_RC}"
        exit "${NATS_RC}"
    fi
else
    run_phase "validate-nats" "${SCRIPT_DIR}/validate-nats.sh"
fi

# ----- Phase 3: app /healthz + auth=on probe ----------------------------------
if [ "${DO_APP_VALIDATE}" -eq 1 ]; then
    log "phase 3/4: ./helm/local/validate-app.sh"
    if [ "${DO_CAPTURE}" -eq 1 ]; then
        set +e
        run_phase "validate-app" "${SCRIPT_DIR}/validate-app.sh"
        APP_RC=$?
        set -e
        snapshot_failed_pods "validate-app"
        if [ "${APP_RC}" -ne 0 ]; then
            err "phase 3 (validate-app) failed with rc=${APP_RC}"
            exit "${APP_RC}"
        fi
    else
        run_phase "validate-app" "${SCRIPT_DIR}/validate-app.sh"
    fi
else
    log "phase 3/4: SKIPPED (--no-app)"
    record_phase "validate-app" 1 0 0 0 "" ""
fi

# ----- Phase 4: teardown ------------------------------------------------------
if [ "${DO_TEARDOWN}" -eq 1 ]; then
    log "phase 4/4: ./helm/local/down.sh"
    if [ "${DO_CAPTURE}" -eq 1 ]; then
        set +e
        run_phase "down" "${SCRIPT_DIR}/down.sh"
        DOWN_RC=$?
        set -e
        if [ "${DOWN_RC}" -ne 0 ]; then
            err "phase 4 (down) failed with rc=${DOWN_RC}"
            exit "${DOWN_RC}"
        fi
    else
        run_phase "down" "${SCRIPT_DIR}/down.sh"
    fi
else
    log "phase 4/4: SKIPPED (--no-teardown) — cluster left up for inspection"
    record_phase "down" 1 0 0 0 "" ""
fi

log "smoke OK"
