#!/usr/bin/env bash
#
# helm/local/load-test.sh — run an in-cluster load test against the
# local k3d videocall stack and evaluate it against CI thresholds.
#
# Assumes the cluster is already up (./helm/local/up.sh has run).
# Steps:
#   1. Optionally scale the SFU StatefulSet to --replicas R.
#   2. Build localhost:5000/videocall-bot:dev from bot/Dockerfile and
#      k3d-import it into the cluster.
#   3. Apply a fresh `load-test-config` ConfigMap with the run params.
#   4. (Re-)apply helm/local/manifests/load-test-job.yaml.
#   5. Wait for the Job to complete (or fail-fast on Job failure).
#   6. Pull the bot Pod's stdout, extract the trailing JSON summary, and
#      pipe it into scripts/eval-load-test.py. Write a verdict JSON to
#      ./load-test-verdict.json.
#   7. On failure, dump SFU + bot logs and any non-Running pods.
#   8. Snapshot SFU pod restartCount pre/post and fail if it increased
#      during the run (a crashed-then-restarted SFU is a release-gate
#      failure regardless of the bot's loss number).
#
# Flags (all required unless defaulted):
#   --senders N
#   --listeners M
#   --duration S
#   --max-loss-pct X      (passed straight to eval-load-test.py)
#   --replicas R          (number of rustlemania-webtransport SFU pods)
#   --room NAME           (default: ci-loadtest-<unix-ts>)
#   --server-url URL      (default: https://webtransport-headless.default.svc.cluster.local:443)
#   --verdict-path PATH   (default: $(pwd)/load-test-verdict.json)
#   --skip-build          (don't rebuild/import the bot image — useful when
#                          iterating on the eval script locally)
#
# Exit code mirrors eval-load-test.py.

set -euo pipefail

LOG_PREFIX="[load-test.sh]"
log() { echo "${LOG_PREFIX} $*"; }
err() { echo "${LOG_PREFIX} ERROR: $*" >&2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${HELM_DIR}/.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-videocall-local}"
KUBECONTEXT="${KUBECONTEXT:-k3d-${CLUSTER_NAME}}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
BOT_IMAGE="localhost:${REGISTRY_PORT}/videocall-bot:dev"

SFU_STATEFULSET="rustlemania-webtransport"
SFU_SELECTOR="app.kubernetes.io/name=${SFU_STATEFULSET}"

# ----- Defaults / arg parsing -------------------------------------------------
SENDERS=""
LISTENERS=""
DURATION=""
MAX_LOSS_PCT=""
REPLICAS=""
ROOM="ci-loadtest-$(date +%s)"
SERVER_URL="https://webtransport-headless.default.svc.cluster.local:443"
VERDICT_PATH="$(pwd)/load-test-verdict.json"
SKIP_BUILD=0

usage() {
    sed -n '2,40p' "${BASH_SOURCE[0]}"
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --senders)        SENDERS="$2"; shift 2 ;;
        --listeners)      LISTENERS="$2"; shift 2 ;;
        --duration)       DURATION="$2"; shift 2 ;;
        --max-loss-pct)   MAX_LOSS_PCT="$2"; shift 2 ;;
        --replicas)       REPLICAS="$2"; shift 2 ;;
        --room)           ROOM="$2"; shift 2 ;;
        --server-url)     SERVER_URL="$2"; shift 2 ;;
        --verdict-path)   VERDICT_PATH="$2"; shift 2 ;;
        --skip-build)     SKIP_BUILD=1; shift ;;
        -h|--help)        usage ;;
        *)                err "unknown flag: $1"; exit 2 ;;
    esac
done

for var in SENDERS LISTENERS DURATION MAX_LOSS_PCT REPLICAS; do
    if [ -z "${!var}" ]; then
        err "--${var,,} is required"
        exit 2
    fi
done

# Preflight: required binaries
for bin in kubectl k3d docker python3; do
    if ! command -v "${bin}" >/dev/null 2>&1; then
        err "'${bin}' not found on PATH"
        exit 1
    fi
done

KCTL=(kubectl --context "${KUBECONTEXT}")

# ----- Scale the SFU StatefulSet ----------------------------------------------
log "scaling statefulset/${SFU_STATEFULSET} to ${REPLICAS} replicas"
"${KCTL[@]}" -n default scale "statefulset/${SFU_STATEFULSET}" "--replicas=${REPLICAS}" >/dev/null
log "waiting for statefulset/${SFU_STATEFULSET} rollout (timeout 300s)"
"${KCTL[@]}" -n default rollout status "statefulset/${SFU_STATEFULSET}" --timeout=300s

# Snapshot SFU restart counts BEFORE the run so we can detect crashes that
# get auto-recovered by the kubelet. A release-gate run with even one
# restart should fail; the bot might still report low loss because the
# new pod recovers fast, but we don't want to ship a regression that
# panics under load.
sfu_restart_sum() {
    "${KCTL[@]}" -n default get pods -l "${SFU_SELECTOR}" \
        -o jsonpath='{range .items[*]}{.status.containerStatuses[*].restartCount}{"\n"}{end}' \
        2>/dev/null \
        | awk 'BEGIN{s=0} {for(i=1;i<=NF;i++) s+=$i} END{print s+0}'
}
RESTARTS_BEFORE="$(sfu_restart_sum)"
log "SFU restart-count snapshot (pre-run): ${RESTARTS_BEFORE}"

# ----- Build + import the bot image ------------------------------------------
if [ "${SKIP_BUILD}" -eq 0 ]; then
    GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    GIT_BRANCH="$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
    BUILD_TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    log "building ${BOT_IMAGE} from bot/Dockerfile (context: ${REPO_ROOT})"
    docker build \
        --tag "${BOT_IMAGE}" \
        --file "${REPO_ROOT}/bot/Dockerfile" \
        --build-arg "GIT_SHA=${GIT_SHA}" \
        --build-arg "GIT_BRANCH=${GIT_BRANCH}" \
        --build-arg "BUILD_TIMESTAMP=${BUILD_TIMESTAMP}" \
        "${REPO_ROOT}"
    log "pushing ${BOT_IMAGE} to local registry"
    docker push "${BOT_IMAGE}"
    log "k3d image import ${BOT_IMAGE} -> cluster '${CLUSTER_NAME}'"
    k3d image import "${BOT_IMAGE}" --cluster "${CLUSTER_NAME}"
else
    log "--skip-build: reusing existing ${BOT_IMAGE} in the cluster"
fi

# ----- Apply the load-test ConfigMap -----------------------------------------
log "applying ConfigMap/load-test-config (room=${ROOM} senders=${SENDERS} listeners=${LISTENERS} duration=${DURATION}s)"
"${KCTL[@]}" -n default create configmap load-test-config \
    --from-literal="ROOM=${ROOM}" \
    --from-literal="SENDERS=${SENDERS}" \
    --from-literal="LISTENERS=${LISTENERS}" \
    --from-literal="DURATION_S=${DURATION}" \
    --from-literal="SERVER_URL=${SERVER_URL}" \
    --dry-run=client -o yaml \
    | "${KCTL[@]}" -n default apply -f - >/dev/null

# ----- (Re-)apply the Job manifest -------------------------------------------
log "deleting prior Job/load-test (if any)"
"${KCTL[@]}" -n default delete job/load-test --ignore-not-found --wait=true >/dev/null

log "applying Job/load-test"
"${KCTL[@]}" apply -f "${SCRIPT_DIR}/manifests/load-test-job.yaml" >/dev/null

# ----- Wait for the Job, fail-fast on Job failure ----------------------------
#
# `kubectl wait --for=condition=complete` blocks until success. We also
# want fast-fail on `condition=failed` (e.g. ImagePullBackOff or a crash
# loop). Run both waits in the background and take whichever returns
# first; whoever loses gets killed.
log "waiting for Job/load-test (complete or failed; timeout 900s)"
COMPLETE_RC_FILE="$(mktemp)"
FAILED_RC_FILE="$(mktemp)"
trap 'rm -f "${COMPLETE_RC_FILE}" "${FAILED_RC_FILE}"' EXIT

(
    "${KCTL[@]}" -n default wait --for=condition=complete job/load-test --timeout=900s >/dev/null 2>&1
    echo "$?" > "${COMPLETE_RC_FILE}"
) &
COMPLETE_PID=$!

(
    "${KCTL[@]}" -n default wait --for=condition=failed job/load-test --timeout=900s >/dev/null 2>&1
    echo "$?" > "${FAILED_RC_FILE}"
) &
FAILED_PID=$!

# Poll the two waiter files; first non-empty one wins.
JOB_OUTCOME="timeout"
deadline=$(( $(date +%s) + 920 ))
while :; do
    if [ -s "${COMPLETE_RC_FILE}" ] && [ "$(cat "${COMPLETE_RC_FILE}")" = "0" ]; then
        JOB_OUTCOME="complete"; break
    fi
    if [ -s "${FAILED_RC_FILE}" ] && [ "$(cat "${FAILED_RC_FILE}")" = "0" ]; then
        JOB_OUTCOME="failed"; break
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
        JOB_OUTCOME="timeout"; break
    fi
    sleep 2
done
kill "${COMPLETE_PID}" 2>/dev/null || true
kill "${FAILED_PID}" 2>/dev/null || true
wait 2>/dev/null || true

log "Job outcome: ${JOB_OUTCOME}"

# ----- Always grab Pod logs (they hold the JSON summary) ---------------------
LOGS_FILE="$(mktemp)"
"${KCTL[@]}" -n default logs job/load-test --tail=-1 > "${LOGS_FILE}" 2>/dev/null || true
log "captured bot Pod logs to ${LOGS_FILE} ($(wc -l < "${LOGS_FILE}") lines)"

dump_failure_artifacts() {
    err "dumping failure artifacts"
    echo "--- non-Running pods in default ns ---" >&2
    "${KCTL[@]}" -n default get pods --no-headers 2>/dev/null \
        | awk '$3 != "Running" && $3 != "Completed"' >&2 || true
    echo "--- SFU pod logs (tail=200) ---" >&2
    "${KCTL[@]}" -n default logs -l "${SFU_SELECTOR}" --tail=200 --prefix=true >&2 || true
    echo "--- last 80 lines of bot Pod logs ---" >&2
    tail -n 80 "${LOGS_FILE}" >&2 || true
}

if [ "${JOB_OUTCOME}" != "complete" ]; then
    err "load-test Job did not complete (outcome=${JOB_OUTCOME})"
    dump_failure_artifacts
    exit 1
fi

# ----- Extract the trailing JSON summary from bot stdout ---------------------
#
# The orchestrator writes exactly one top-level JSON object to stdout at
# the end of the run (logs go to stderr). It's pretty-printed across many
# lines. We find the last line that starts with `{`, then take everything
# from that point to EOF — that's our summary.
SUMMARY_JSON="$(mktemp)"
LAST_OPEN_LINE="$(awk '/^\{/ {n=NR} END {print n+0}' "${LOGS_FILE}")"
if [ "${LAST_OPEN_LINE}" -gt 0 ]; then
    tail -n "+${LAST_OPEN_LINE}" "${LOGS_FILE}" > "${SUMMARY_JSON}"
else
    err "couldn't find a JSON object in bot Pod logs"
    dump_failure_artifacts
    exit 1
fi

# Quick sanity check: it must parse.
if ! python3 -c "import json,sys; json.load(open('${SUMMARY_JSON}'))" 2>/dev/null; then
    err "extracted summary is not valid JSON; see ${SUMMARY_JSON}"
    head -n 5 "${SUMMARY_JSON}" >&2 || true
    dump_failure_artifacts
    exit 1
fi

# ----- Snapshot SFU restart counts AFTER the run -----------------------------
RESTARTS_AFTER="$(sfu_restart_sum)"
log "SFU restart-count snapshot (post-run): ${RESTARTS_AFTER}"
if [ "${RESTARTS_AFTER}" -gt "${RESTARTS_BEFORE}" ]; then
    err "SFU pod restartCount increased during the run (${RESTARTS_BEFORE} -> ${RESTARTS_AFTER}) — failing the gate"
    dump_failure_artifacts
    # Still try to write a verdict for CI artifact upload before exiting.
    python3 "${REPO_ROOT}/scripts/eval-load-test.py" \
        --max-loss-pct "${MAX_LOSS_PCT}" \
        --out-json "${VERDICT_PATH}" \
        < "${SUMMARY_JSON}" \
        > /dev/null 2>&1 || true
    exit 1
fi

# ----- Evaluate the summary against thresholds -------------------------------
log "evaluating summary: max_loss_pct=${MAX_LOSS_PCT}"
set +e
python3 "${REPO_ROOT}/scripts/eval-load-test.py" \
    --max-loss-pct "${MAX_LOSS_PCT}" \
    --out-json "${VERDICT_PATH}" \
    < "${SUMMARY_JSON}"
EVAL_RC=$?
set -e

log "verdict written to ${VERDICT_PATH} (eval rc=${EVAL_RC})"
if [ "${EVAL_RC}" -ne 0 ]; then
    dump_failure_artifacts
fi

exit "${EVAL_RC}"
