#!/usr/bin/env bash
# Realistic stress: 20 presenters EACH on their own pod (laptop-class resources,
# 1 VP9 encoder/pod — no artificial sender starvation) + 500→1000 listeners +
# churn, against a MINIMAL single SFU pod. Server minimal; clients realistic.
# Checks each listener for VIDEO + AUDIO decode failures + crc.
#
# Senders: 20 pods × 1 sender (existing template = up to 6 CPU / 4Gi per pod,
#          far more than one 720p30 VP9 encode needs — its natural footprint).
# T=0   ESTABLISH: 20 sender pods + 5 listener pods×100 = 500 listeners (co-arrival).
# T=180/300/420 CHURN: short-lived 100-listener pods join mid-stream & leave.
# T=540 GROW: +5 listener pods×100 => 1000 concurrent listeners.
set -uo pipefail
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="rstress-$(date +%s)"; RUN_LOG="${DIR}/run.log"; TOP_LOG="${DIR}/top.log"
: > "${RUN_LOG}"; : > "${TOP_LOG}"
SEL="app.kubernetes.io/name=videocall-rstress"
log(){ echo "[rstress] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
apply(){ local name="$1" shard="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${name}|g" -e "s|__SHARD__|${shard}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-rstress|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
restarts(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0; }
SAMPLER=""
( while :; do echo "----- $(date '+%H:%M:%S') SFU_restarts=$(restarts) -----" >> "${TOP_LOG}"
    "${KCTL[@]}" top pods --no-headers 2>/dev/null | grep -E 'rstress|rustlemania-webtransport' >> "${TOP_LOG}" 2>/dev/null || true
    sleep 20; done ) & SAMPLER=$!
trap '[ -n "${SAMPLER}" ] && kill ${SAMPLER} 2>/dev/null; "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
R0=$(restarts); log "ROOM=${ROOM} minimal SFU, baseline restarts=${R0}"

log "T=0 ESTABLISH: 20 presenters (1 sender/pod, laptop-class) + 500 listeners (5x100), co-arrival"
for i in $(seq -w 1 20); do apply "rstress-snd${i}" "snd${i}" 1 0 1100; done
for i in 1 2 3 4 5; do apply "rstress-core${i}" "c${i}" 0 100 1100; done
START=$(date +%s); wait_to(){ while :; do [ $(( $(date +%s)-START )) -ge "$1" ] && break; sleep 3; done; }

wait_to 180; log "T=180 stable check: SFU restarts=$(restarts); CHURN A"; apply rstress-churnA cA 0 100 150
wait_to 300; log "T=300 CHURN B (restarts=$(restarts))"; apply rstress-churnB cB 0 100 150
wait_to 420; log "T=420 CHURN C (restarts=$(restarts))"; apply rstress-churnC cC 0 100 150
wait_to 540; log "T=540 GROW +500 => 1000 listeners (restarts=$(restarts))"
for i in 1 2 3 4 5; do apply "rstress-grow${i}" "g${i}" 0 100 480; done

log "waiting for completion"; deadline=$(( START + 1200 ))
while :; do now=$(date +%s); (( now>deadline )) && { log "deadline"; break; }
  pend=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "${pend}" -eq 0 ] && { log "all pods done"; break; }; sleep 20; done
log "collecting"
for p in $("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
  "${KCTL[@]}" logs "$p" > "${DIR}/${p}.log" 2>&1 || true; done
log "post-run SFU restarts=$(restarts) (baseline ${R0})"
log "rstress done"
