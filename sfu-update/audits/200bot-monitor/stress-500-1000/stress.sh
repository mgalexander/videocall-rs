#!/usr/bin/env bash
# Stress: 500-user meeting w/ 20 presenters on a MINIMAL single SFU pod, then
# churn (users come & go), then grow to 1000 listeners. Checks each listener for
# VIDEO and AUDIO decode failures + crc. Runs long enough to capture failures.
#
# Topology (replicas=1, SFU already patched to minimal 500m/256Mi):
#   Senders: 20 presenters across 2 pods (10 each — a pod pegs 6 CPU at 10).
#   T=0   ESTABLISH: 5 listener pods x100 = 500 listeners (co-arrival, long-lived).
#   T=180/300/420 CHURN: short-lived 100-listener pods (dur 150s) join mid-stream
#                  and leave — "users come and go".
#   T=540 GROW: +5 listener pods x100 => 1000 concurrent listeners (mid-stream).
#   Run ends ~T=1080. All listener pods emit summaries; we classify v/a failures.
set -uo pipefail
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="stress-$(date +%s)"; RUN_LOG="${DIR}/run.log"; TOP_LOG="${DIR}/top.log"
: > "${RUN_LOG}"; : > "${TOP_LOG}"
SEL="app.kubernetes.io/name=videocall-stress"
log(){ echo "[stress] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
apply(){ local name="$1" shard="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${name}|g" -e "s|__SHARD__|${shard}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-stress|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
restarts(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0; }

# PID-bounded sampler (no self-grep)
SAMPLER=""
( while :; do echo "----- $(date '+%H:%M:%S') SFU_restarts=$(restarts) -----" >> "${TOP_LOG}"
    "${KCTL[@]}" top pods --no-headers 2>/dev/null | grep -E 'stress|rustlemania-webtransport' >> "${TOP_LOG}" 2>/dev/null || true
    sleep 20; done ) & SAMPLER=$!
trap '[ -n "${SAMPLER}" ] && kill ${SAMPLER} 2>/dev/null; "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT

"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
R0=$(restarts); log "ROOM=${ROOM} minimal SFU, baseline restarts=${R0}"

log "T=0 ESTABLISH: 20 senders (2x10) + 500 listeners (5x100), co-arrival"
apply stress-s1 s1 10 0 1100; apply stress-s2 s2 10 0 1100
for i in 1 2 3 4 5; do apply "stress-core${i}" "c${i}" 0 100 1100; done
START=$(date +%s)
wait_to(){ while :; do [ $(( $(date +%s)-START )) -ge "$1" ] && break; sleep 3; done; }

wait_to 180
log "T=180 stable check: SFU restarts=$(restarts) (baseline ${R0}); begin CHURN"
apply stress-churnA cA 0 100 150
wait_to 300; log "T=300 CHURN wave B (restarts=$(restarts))"; apply stress-churnB cB 0 100 150
wait_to 420; log "T=420 CHURN wave C (restarts=$(restarts))"; apply stress-churnC cC 0 100 150

wait_to 540
log "T=540 GROW: +500 listeners => 1000 concurrent (restarts=$(restarts))"
for i in 1 2 3 4 5; do apply "stress-grow${i}" "g${i}" 0 100 480; done

log "waiting for completion"
deadline=$(( START + 1200 ))
while :; do now=$(date +%s); (( now>deadline )) && { log "deadline"; break; }
  pend=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "${pend}" -eq 0 ] && { log "all pods done"; break; }; sleep 20; done

log "collecting summaries"
for p in $("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
  "${KCTL[@]}" logs "$p" > "${DIR}/${p}.log" 2>&1 || true; done
log "post-run SFU restarts=$(restarts) (baseline ${R0}, delta $(( $(restarts)-R0 )))"
log "stress done"
