#!/usr/bin/env bash
# 40-min escalation soak: single SFU pod @ 4 CPU / 4Gi. 20 presenters each on
# their OWN pod (unstarved). Listeners start at 500 and grow in +500 groups every
# GROUP_INTERVAL. Runs to MAX_SECONDS (40 min) OR stops early on UNRECOVERABLE SFU
# failure (CrashLoopBackOff / sustained NotReady / runaway restarts).
#
# Usage: soak.sh   (env: GROUP_INTERVAL=300 MAX_SECONDS=2400 MAX_GROUPS=8)
set -uo pipefail
GROUP_INTERVAL="${GROUP_INTERVAL:-300}"; MAX_SECONDS="${MAX_SECONDS:-2400}"; MAX_GROUPS="${MAX_GROUPS:-8}"
DUR=$(( MAX_SECONDS + 120 ))   # pod lifetime covers the whole soak
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="soak-$(date +%s)"; RUN_LOG="${DIR}/run.log"; TOP_LOG="${DIR}/top.log"
: > "${RUN_LOG}"; : > "${TOP_LOG}"
SEL="app.kubernetes.io/name=videocall-soak"
log(){ echo "[soak] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
ap(){ local n="$1" sh="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${n}|g" -e "s|__SHARD__|${sh}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-soak|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
sfu_restarts(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0; }
sfu_waiting(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].state.waiting.reason}' 2>/dev/null; }
sfu_ready(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null; }
SAMPLER=""
( while :; do echo "----- $(date '+%H:%M:%S') restarts=$(sfu_restarts) ready=$(sfu_ready) -----" >> "${TOP_LOG}"
    "${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | sed 's/^/SFU /' >> "${TOP_LOG}" 2>/dev/null || true
    sleep 30; done ) & SAMPLER=$!
trap '[ -n "${SAMPLER}" ] && kill ${SAMPLER} 2>/dev/null; "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
R0=$(sfu_restarts); NOTREADY_SINCE=0
log "ROOM=${ROOM} SFU 4CPU/4Gi, baseline restarts=${R0}, group=+500/${GROUP_INTERVAL}s, cap=${MAX_SECONDS}s"

log "T=0: 20 presenters (own pods) + group 1 = 500 listeners (co-arrival)"
for i in $(seq -w 1 20); do ap "soak-snd${i}" "snd${i}" 1 0 "${DUR}"; done
for i in 1 2 3 4 5; do ap "soak-g1p${i}" "g1p${i}" 0 100 "${DUR}"; done
START=$(date +%s); group=1; listeners=500

# sets global REASON, returns 1 on UNRECOVERABLE failure (called directly, NOT in
# a subshell, so NOTREADY_SINCE persists across iterations)
REASON=""
check_unrecoverable(){
  REASON=""
  local w r rd now
  w=$(sfu_waiting)
  if [ "${w}" = "CrashLoopBackOff" ]; then REASON="CrashLoopBackOff"; return 1; fi
  r=$(sfu_restarts)
  if (( r - R0 >= 3 )); then REASON="runaway restarts (+$((r-R0)))"; return 1; fi
  rd=$(sfu_ready); now=$(date +%s)
  if [ "${rd}" = "false" ]; then
    [ "${NOTREADY_SINCE}" -eq 0 ] && NOTREADY_SINCE=${now}
    if (( now - NOTREADY_SINCE > 120 )); then REASON="NotReady >120s"; return 1; fi
  else NOTREADY_SINCE=0; fi
  return 0
}

while :; do
  now=$(date +%s); elapsed=$(( now - START ))
  if (( elapsed >= MAX_SECONDS )); then log "reached ${MAX_SECONDS}s cap — soak complete, stable"; break; fi
  # next group?
  if (( group < MAX_GROUPS )) && (( elapsed >= group * GROUP_INTERVAL )); then
    group=$(( group + 1 )); listeners=$(( group * 500 ))
    log "T=${elapsed}s GROUP ${group}: +500 => ${listeners} listeners (SFU restarts=$(sfu_restarts) cpu=$("${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk '{print $2}'))"
    for i in 1 2 3 4 5; do ap "soak-g${group}p${i}" "g${group}p${i}" 0 100 "${DUR}"; done
  fi
  if ! check_unrecoverable; then
    log "!!! UNRECOVERABLE at T=${elapsed}s, ${listeners} listeners: ${REASON}"; break; fi
  sleep 20
done

log "collecting summaries ($(date '+%H:%M:%S'))"
for p in $("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
  "${KCTL[@]}" logs "$p" --tail=400 > "${DIR}/${p}.log" 2>&1 || true; done
log "final: restarts=$(sfu_restarts) (baseline ${R0}) ready=$(sfu_ready) peak listeners=${listeners}"
log "soak done"
