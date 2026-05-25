#!/usr/bin/env bash
#
# scripts/sfu_p6_failover_test.sh
#
# E2E test scaffold for bead vc-607 (p6-11): pod-kill failover with <15s
# recovery. The test:
#
#   1. Preflights kubectl + the rustlemania-webtransport StatefulSet.
#   2. Builds the bot binary in release mode.
#   3. Launches the bot in --failover-test mode (default: 1 sender, 5
#      listeners, room "failover-test", 30s run).
#   4. Sleeps STEADY_STATE_S (default 3s) for steady-state media flow.
#   5. kubectl deletes the named owner pod (default rustlemania-webtransport-0)
#      and prints the timestamp.
#   6. Waits for the bot to finish.
#   7. Parses the JSON summary, asserts max_downtime_ms < 15000, and prints
#      a per-listener breakdown.
#   8. Exits 0 on success, non-zero on any failure with a clear message.
#
# This script is the orchestrator. The bot itself owns the reconnect /
# redirect-parsing logic (see bot/src/failover.rs, bot/src/webtransport_client.rs).
#
# All knobs are env-overridable:
#
#   ROOM                  Meeting id (default: failover-test).
#   SENDERS               Number of senders (default: 1).
#   LISTENERS             Number of listeners (default: 5).
#   DURATION_S            Bot total run time in seconds (default: 30).
#   STEADY_STATE_S        Steady-state wait before pod-kill (default: 3).
#   OWNER_POD             Pod to delete (default: rustlemania-webtransport-0).
#   NAMESPACE             K8s namespace (default: default).
#   STS_NAME              StatefulSet name (default: rustlemania-webtransport).
#   KUBECONTEXT           kubectl context (default: current context).
#   SERVER_URL            WebTransport URL the bots connect to. REQUIRED.
#                         Example: https://transport.videocall.local:30443
#   INSECURE              "1" to pass --insecure to the bot (default: 1).
#   MAX_DOWNTIME_MS       Pass/fail threshold (default: 15000).
#   RECONNECT_INTERVAL_MS Bot's reconnect interval (default: 500).
#   BOT_BIN               Path to a pre-built bot binary; skips cargo build.
#   BOT_LOG_FILE          Where to write bot stderr (default: temp).
#   BOT_JSON_FILE         Where to write bot stdout JSON (default: temp).
#
# Exit codes:
#   0  Success — max downtime within budget.
#   1  Generic failure (preflight, build, kubectl, etc.).
#   2  Bot crashed before finishing.
#   3  JSON parse failure.
#   4  Assertion failure (max_downtime_ms >= MAX_DOWNTIME_MS).

set -euo pipefail

# ----- Tunables -------------------------------------------------------------
ROOM="${ROOM:-failover-test}"
SENDERS="${SENDERS:-1}"
LISTENERS="${LISTENERS:-5}"
DURATION_S="${DURATION_S:-30}"
STEADY_STATE_S="${STEADY_STATE_S:-3}"
OWNER_POD="${OWNER_POD:-rustlemania-webtransport-0}"
NAMESPACE="${NAMESPACE:-default}"
STS_NAME="${STS_NAME:-rustlemania-webtransport}"
KUBECONTEXT="${KUBECONTEXT:-}"
INSECURE="${INSECURE:-1}"
MAX_DOWNTIME_MS="${MAX_DOWNTIME_MS:-15000}"
RECONNECT_INTERVAL_MS="${RECONNECT_INTERVAL_MS:-500}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ----- Helpers --------------------------------------------------------------
log() { printf '[%s] %s\n' "$(date +'%Y-%m-%dT%H:%M:%S%z')" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

ts_ms() { date +%s%3N; }

kubectl_cmd() {
    if [[ -n "${KUBECONTEXT}" ]]; then
        kubectl --context "${KUBECONTEXT}" -n "${NAMESPACE}" "$@"
    else
        kubectl -n "${NAMESPACE}" "$@"
    fi
}

# ----- Preflight ------------------------------------------------------------
if ! command -v kubectl >/dev/null 2>&1; then
    die "kubectl not found in PATH"
fi
if [[ -z "${SERVER_URL:-}" ]]; then
    die "SERVER_URL must be set (e.g. https://transport.videocall.local:30443)"
fi
if ! command -v jq >/dev/null 2>&1; then
    die "jq not found in PATH (used for parsing the bot summary JSON)"
fi

log "Preflight: kubectl context=$(kubectl_cmd config current-context 2>/dev/null || echo "<unset>")"

if ! kubectl_cmd get statefulset "${STS_NAME}" >/dev/null 2>&1; then
    die "StatefulSet '${STS_NAME}' not found in namespace '${NAMESPACE}'"
fi

REPLICAS=$(kubectl_cmd get statefulset "${STS_NAME}" -o jsonpath='{.spec.replicas}')
if [[ -z "${REPLICAS}" || "${REPLICAS}" -lt 2 ]]; then
    die "StatefulSet '${STS_NAME}' has replicas=${REPLICAS:-0}; need >= 2. Scale with: kubectl scale sts ${STS_NAME} --replicas=2"
fi
log "Preflight: ${STS_NAME} replicas=${REPLICAS} OK"

if ! kubectl_cmd get pod "${OWNER_POD}" >/dev/null 2>&1; then
    die "Owner pod '${OWNER_POD}' not found in namespace '${NAMESPACE}'"
fi
log "Preflight: owner pod ${OWNER_POD} present"

# ----- Build bot ------------------------------------------------------------
if [[ -n "${BOT_BIN:-}" ]]; then
    if [[ ! -x "${BOT_BIN}" ]]; then
        die "BOT_BIN=${BOT_BIN} is not executable"
    fi
    BOT="${BOT_BIN}"
    log "Using pre-built bot at ${BOT}"
else
    if ! command -v cargo >/dev/null 2>&1; then
        die "cargo not found in PATH (set BOT_BIN to a pre-built binary to skip)"
    fi
    log "Building bot (cargo build --release -p bot)..."
    (cd "${REPO_ROOT}" && cargo build --release -p bot >&2)
    BOT="${REPO_ROOT}/target/release/bot"
    if [[ ! -x "${BOT}" ]]; then
        die "Bot binary not found after build at ${BOT}"
    fi
fi

# ----- Spawn bot ------------------------------------------------------------
JSON_FILE="${BOT_JSON_FILE:-$(mktemp -t sfu_p6_failover_test.XXXXXX.json)}"
LOG_FILE="${BOT_LOG_FILE:-$(mktemp -t sfu_p6_failover_test.XXXXXX.log)}"

BOT_ARGS=(
    --failover-test
    --room "${ROOM}"
    --senders "${SENDERS}"
    --listeners "${LISTENERS}"
    --duration "${DURATION_S}"
    --server-url "${SERVER_URL}"
    --reconnect-interval-ms "${RECONNECT_INTERVAL_MS}"
)
if [[ "${INSECURE}" == "1" ]]; then
    BOT_ARGS+=(--insecure)
fi

log "Spawning bot: ${BOT} ${BOT_ARGS[*]}"
log "  stdout JSON -> ${JSON_FILE}"
log "  stderr logs -> ${LOG_FILE}"

# RUST_LOG kept modest so the log file is human-skimmable.
RUST_LOG="${RUST_LOG:-info,bot=info}" "${BOT}" "${BOT_ARGS[@]}" >"${JSON_FILE}" 2>"${LOG_FILE}" &
BOT_PID=$!

# Ensure we don't leave a bot dangling if the script dies.
cleanup() {
    if kill -0 "${BOT_PID}" 2>/dev/null; then
        log "Cleanup: killing bot pid=${BOT_PID}"
        kill "${BOT_PID}" 2>/dev/null || true
        wait "${BOT_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ----- Steady state, then kill ---------------------------------------------
log "Sleeping ${STEADY_STATE_S}s for steady-state media flow..."
sleep "${STEADY_STATE_S}"

KILL_TS_MS=$(ts_ms)
log "Deleting owner pod ${OWNER_POD} at unix-ms=${KILL_TS_MS}"
kubectl_cmd delete pod "${OWNER_POD}" --grace-period=0 --force --wait=false || \
    die "kubectl delete pod ${OWNER_POD} failed"

# ----- Wait for bot to finish ----------------------------------------------
log "Waiting for bot pid=${BOT_PID} to exit (run duration ~${DURATION_S}s)..."
# `wait` returns the bot's exit code; do not `set +e` because we want to
# distinguish bot failure (exit 2) from assertion failure (exit 4).
if ! wait "${BOT_PID}"; then
    BOT_RC=$?
    log "Bot exited non-zero (rc=${BOT_RC}). Last 40 lines of stderr:"
    tail -n 40 "${LOG_FILE}" >&2 || true
    exit 2
fi
trap - EXIT

log "Bot finished. Parsing summary JSON from ${JSON_FILE}..."
if [[ ! -s "${JSON_FILE}" ]]; then
    log "Empty JSON file. Last 40 lines of stderr:"
    tail -n 40 "${LOG_FILE}" >&2 || true
    exit 3
fi
if ! jq empty <"${JSON_FILE}" 2>/dev/null; then
    log "JSON parse failed. File contents:"
    cat "${JSON_FILE}" >&2
    exit 3
fi

# ----- Assertions and reporting --------------------------------------------
MAX_DT=$(jq -r '.max_downtime_ms // "null"' <"${JSON_FILE}")
WITH_GAP=$(jq -r '.listeners_with_gap // 0' <"${JSON_FILE}")
RECOVERED=$(jq -r '.listeners_recovered // 0' <"${JSON_FILE}")

log "Summary:"
log "  listeners_with_gap   = ${WITH_GAP}"
log "  listeners_recovered  = ${RECOVERED}"
log "  max_downtime_ms      = ${MAX_DT}"
log "  threshold            = ${MAX_DOWNTIME_MS} ms"
log ""
log "Per-listener downtime breakdown:"
jq -r '
    .per_bot[]
    | select(.role == "listener")
    | "  \(.user_id)  connected=\(.connected)  packets=\(.packets_received)  downtime_ms=\(.downtime_ms // "n/a")  disconnect_at_ms=\(.disconnect_at_ms // "n/a")  reconnect_at_ms=\(.reconnect_at_ms // "n/a")"
' <"${JSON_FILE}" >&2

# Pass/fail logic:
#   - If no listener saw a gap, the test is inconclusive — the kill may
#     have missed entirely. Treat that as failure: the test exists to
#     measure recovery, and zero observations means we measured nothing.
#   - If some saw a gap but didn't recover, fail.
#   - If max_downtime_ms >= threshold, fail.
if [[ "${WITH_GAP}" -eq 0 ]]; then
    die "No listener observed a disconnect. Did the pod-kill land? Did the bots ever connect?"
fi
if [[ "${RECOVERED}" -lt "${WITH_GAP}" ]]; then
    log "ASSERTION FAILED: ${RECOVERED}/${WITH_GAP} listeners with gap recovered before duration end"
    exit 4
fi
if [[ "${MAX_DT}" == "null" || -z "${MAX_DT}" ]]; then
    die "max_downtime_ms is null even though listeners_with_gap=${WITH_GAP}"
fi
if (( MAX_DT >= MAX_DOWNTIME_MS )); then
    log "ASSERTION FAILED: max_downtime_ms=${MAX_DT} >= threshold=${MAX_DOWNTIME_MS}"
    exit 4
fi

log "PASS: max_downtime_ms=${MAX_DT} < ${MAX_DOWNTIME_MS}"
exit 0
