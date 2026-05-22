#!/usr/bin/env bash
# Decode-verification run — fixed duration so pods COMPLETE and emit summaries
# (decode counts + crc_mismatches). Multi-pod (replicas=3, prod limits) with one
# room driven past the 180 spillover cap, so we can confirm SPILL-ADMITTED
# listeners (on non-owner pods) actually receive+decode media, not just connect.
#
# 10 senders (1 pod) + NPODS x 100 listeners, all into ONE room.
# Usage: decode-verify.sh [NPODS] [DURATION_S]   defaults: 15 360
set -uo pipefail

NPODS="${1:-15}"
DURATION="${2:-360}"
SDUR=$(( DURATION + 60 ))
REPLICAS=3
CTX=k3d-videocall-local
KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="dvrun-$(date +%s)"
RUN_LOG="${DIR}/run.log"; : > "${RUN_LOG}"
SELECTOR="app.kubernetes.io/name=videocall-dvrun"

log() { echo "[dvrun] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
apply_pod() {
    local name="$1" shard="$2" senders="$3" listeners="$4" dur="$5" m
    m="$(mktemp --suffix=.yaml)"
    sed -e "s|__NAME__|${name}|g" -e "s|__SHARD__|${shard}|g" -e "s|__ROOM__|${ROOM}|g" \
        -e "s|__SENDERS__|${senders}|g" -e "s|__LISTENERS__|${listeners}|g" -e "s|__DURATION__|${dur}|g" \
        "${TMPL}" | sed 's|videocall-staircase|videocall-dvrun|g' > "${m}"
    "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"
}

log "ROOM=${ROOM} REPLICAS=${REPLICAS} listener_pods=${NPODS} (=$(( NPODS*100 )) listeners) dur=${DURATION}s"
"${KCTL[@]}" scale statefulset/rustlemania-webtransport --replicas="${REPLICAS}" >/dev/null 2>&1
"${KCTL[@]}" rollout status statefulset/rustlemania-webtransport --timeout=240s >/dev/null 2>&1
"${KCTL[@]}" delete pod -l "${SELECTOR}" --ignore-not-found --wait=true >/dev/null 2>&1

log "launching 10 senders (dur ${SDUR}s)"; apply_pod dvrun-s s 10 0 "${SDUR}"
sleep 20
log "launching ${NPODS} listener pods x100 (dur ${DURATION}s) into ${ROOM}"
for ((i=1;i<=NPODS;i++)); do apply_pod "dvrun-l${i}" "l${i}" 0 100 "${DURATION}"; done

log "waiting for completion"
START=$(date +%s); deadline=$(( START + DURATION + 240 ))
while :; do
    now=$(date +%s); (( now > deadline )) && { log "deadline exceeded"; break; }
    pend=$("${KCTL[@]}" get pods -l "${SELECTOR}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
    [ "${pend}" -eq 0 ] && { log "all pods done"; break; }
    sleep 15
done

log "collecting summaries"
for ((i=1;i<=NPODS;i++)); do "${KCTL[@]}" logs "dvrun-l${i}" > "${DIR}/l${i}.log" 2>&1 || true; done
"${KCTL[@]}" logs dvrun-s > "${DIR}/s.log" 2>&1 || true
log "owned-by-different-pod rejections (redirect path):"
for p in 0 1 2; do echo "  pod-$p: $("${KCTL[@]}" logs rustlemania-webtransport-$p --tail=100000 2>/dev/null | grep -c 'owned by a different pod')" | tee -a "${RUN_LOG}"; done
log "cleaning listener+sender pods"; "${KCTL[@]}" delete pod -l "${SELECTOR}" --ignore-not-found --wait=false >/dev/null 2>&1
log "dvrun done"
