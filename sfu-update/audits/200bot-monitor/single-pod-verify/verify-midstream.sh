#!/usr/bin/env bash
# Mid-stream verification of the Defect-2 keyframe fix (vc-7zjq): senders start
# FIRST, listeners join ~25s later (mid-stream) — the case that decoded 0 video
# before. Modest scale (10 senders, 300 listeners) so the minimal SFU is NOT the
# limiter, isolating the keyframe fix. replicas=1.
set -uo pipefail
NPODS="${1:-3}"; DURATION="${2:-240}"; STAGGER="${3:-25}"
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="midverify-$(date +%s)"; RUN_LOG="${DIR}/run-mid.log"; : > "${RUN_LOG}"
SEL="app.kubernetes.io/name=videocall-midverify"
log(){ echo "[midverify] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
ap(){ local n="$1" sh="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${n}|g" -e "s|__SHARD__|${sh}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-midverify|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
trap '"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
"${KCTL[@]}" scale statefulset/rustlemania-webtransport --replicas=1 >/dev/null 2>&1
"${KCTL[@]}" rollout status statefulset/rustlemania-webtransport --timeout=120s >/dev/null 2>&1
"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
log "ROOM=${ROOM} 10 senders T=0, ${NPODS}x100 listeners at T+${STAGGER}s (MID-STREAM), dur ${DURATION}s"
for j in $(seq 1 10); do ap "midverify-s${j}" "s${j}" 1 0 $(( DURATION + STAGGER + 30 )); done
sleep "${STAGGER}"
log "T+${STAGGER}: listeners join mid-stream"
for i in $(seq 1 "${NPODS}"); do ap "midverify-l${i}" "l${i}" 0 100 "${DURATION}"; done
START=$(date +%s); dl=$(( START + DURATION + 200 ))
while :; do now=$(date +%s); (( now>dl )) && { log deadline; break; }
  p=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "$p" -eq 0 ] && { log "all done"; break; }; sleep 15; done
for i in $(seq 1 "${NPODS}"); do "${KCTL[@]}" logs "midverify-l${i}" > "${DIR}/lmid${i}.log" 2>&1 || true; done
log "SFU restarts: $("${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null)"
"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1
log "midverify done"
