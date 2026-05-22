#!/usr/bin/env bash
# Single-pod bot verification — replicas=1, CO-ARRIVAL (senders + listeners at
# T=0, the proven-working shape from v10r1 where the initial keyframe reaches
# listeners). Fixed duration so pods emit summaries. Verifies the bot end-to-end:
# video decode + audio decode + crc_mismatches=0 + integrity counters.
#
# Usage: verify.sh [NPODS] [DURATION_S]   defaults: 3 (=300 listeners) 240
set -uo pipefail
NPODS="${1:-3}"; DURATION="${2:-240}"; SDUR=$(( DURATION + 30 ))
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="spverify-$(date +%s)"; RUN_LOG="${DIR}/run.log"; : > "${RUN_LOG}"
SELECTOR="app.kubernetes.io/name=videocall-spverify"
log(){ echo "[spverify] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
apply_pod(){ local name="$1" shard="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${name}|g" -e "s|__SHARD__|${shard}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-spverify|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }

log "ROOM=${ROOM} replicas=1 listener_pods=${NPODS} (=$(( NPODS*100 ))) dur=${DURATION}s CO-ARRIVAL"
"${KCTL[@]}" scale statefulset/rustlemania-webtransport --replicas=1 >/dev/null 2>&1
"${KCTL[@]}" rollout status statefulset/rustlemania-webtransport --timeout=150s >/dev/null 2>&1
"${KCTL[@]}" delete pod -l "${SELECTOR}" --ignore-not-found --wait=true >/dev/null 2>&1
log "T=0: launching 10 senders + ${NPODS}x100 listeners TOGETHER"
apply_pod spverify-s s 10 0 "${SDUR}"
for ((i=1;i<=NPODS;i++)); do apply_pod "spverify-l${i}" "l${i}" 0 100 "${DURATION}"; done
log "waiting for completion"
START=$(date +%s); deadline=$(( START + DURATION + 200 ))
while :; do now=$(date +%s); (( now>deadline )) && { log "deadline"; break; }
  pend=$("${KCTL[@]}" get pods -l "${SELECTOR}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "${pend}" -eq 0 ] && { log "all done"; break; }; sleep 15; done
log "collecting"
for ((i=1;i<=NPODS;i++)); do "${KCTL[@]}" logs "spverify-l${i}" > "${DIR}/l${i}.log" 2>&1 || true; done
"${KCTL[@]}" logs spverify-s > "${DIR}/s.log" 2>&1 || true
log "SFU restarts: $("${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null)"
"${KCTL[@]}" delete pod -l "${SELECTOR}" --ignore-not-found --wait=false >/dev/null 2>&1
log "spverify done"
