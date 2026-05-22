#!/usr/bin/env bash
# Escalation soak WITH listener decode + graphable metrics.
#   SFU 4CPU/4Gi single pod. 20 presenters on own pods.
#   Base listeners +500 every GROUP_INTERVAL (persistent, accumulating load).
#   Per-step DECODE PROBE: a 100-listener cohort (PROBE_DUR) joins at each step,
#     completes, and reports decode at that load level -> decode.csv.
#   metrics.csv: elapsed_s,total_listeners,sfu_cpu_m,sfu_mem_mi,restarts,panics
#     sampled every SAMPLE_S -> the step-up capacity curve.
#   Stops at MAX_SECONDS or UNRECOVERABLE (panic / runaway restarts / forwarding
#   flatline while listeners present / sustained NotReady).
set -uo pipefail
GROUP_INTERVAL="${GROUP_INTERVAL:-240}"; MAX_SECONDS="${MAX_SECONDS:-2400}"; MAX_GROUPS="${MAX_GROUPS:-10}"
PROBE_DUR="${PROBE_DUR:-150}"; SAMPLE_S="${SAMPLE_S:-15}"; DUR=$(( MAX_SECONDS + 180 ))
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="soakg-$(date +%s)"; RUN_LOG="${DIR}/run.log"; METRICS="${DIR}/metrics.csv"; DECODE="${DIR}/decode.csv"
: > "${RUN_LOG}"
echo "elapsed_s,total_listeners,sfu_cpu_m,sfu_mem_mi,restarts,panics" > "${METRICS}"
echo "elapsed_s,total_listeners,probe_video_decoded,probe_audio_decoded,probe_decode_errors,probe_crc_mismatches" > "${DECODE}"
SEL="app.kubernetes.io/name=videocall-soakg"
log(){ echo "[soakg] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
ap(){ local n="$1" sh="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${n}|g" -e "s|__SHARD__|${sh}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-soakg|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
sfu_cpu(){ "${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk '{gsub(/m/,"",$2);print $2+0}'; }
sfu_mem(){ "${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk '{gsub(/Mi/,"",$3);print $3+0}'; }
sfu_restarts(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0; }
sfu_panics(){ "${KCTL[@]}" logs rustlemania-webtransport-0 --tail=4000 2>/dev/null | grep -c 'JoinHandle polled\|panicked' || echo 0; }
sfu_ready(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null; }

TOTAL=0
echo 0 > "${DIR}/.total"
START0=$(date +%s)   # single run epoch; sampler + main loop both use it
# background metrics sampler (reads current total from .total so it stays current)
( while :; do
    el=$(( $(date +%s) - START0 )); t=$(cat "${DIR}/.total" 2>/dev/null||echo 0)
    echo "${el},${t},$(sfu_cpu),$(sfu_mem),$(sfu_restarts),$(sfu_panics)" >> "${METRICS}"
    sleep "${SAMPLE_S}"; done ) & SAMPLER=$!
trap '[ -n "${SAMPLER:-}" ] && kill ${SAMPLER} 2>/dev/null; "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT

"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
R0=$(sfu_restarts); P0=$(sfu_panics); NOTREADY=0
log "ROOM=${ROOM} 4CPU/4Gi; +500/${GROUP_INTERVAL}s base; ${PROBE_DUR}s decode probe/step; cap ${MAX_SECONDS}s"
log "T=0: 20 presenters + base group 1 (500) + probe@500"
for i in $(seq -w 1 20); do ap "soakg-snd${i}" "snd${i}" 1 0 "${DUR}"; done
for i in 1 2 3 4 5; do ap "soakg-b1p${i}" "b1p${i}" 0 100 "${DUR}"; done
TOTAL=500; echo 500 > "${DIR}/.total"
ap "soakg-probe1" "pr1" 0 100 "${PROBE_DUR}"
START=${START0}; group=1; probe_pending=1; probe_at=500; probe_launch_at=0

collect_probe(){ # $1=group idx, $2=listeners-level
  local g="$1" lv="$2" f="${DIR}/probe${g}.log"
  "${KCTL[@]}" logs "soakg-probe${g}" > "${f}" 2>&1 || true
  python3 - "$f" "$lv" >> "${DECODE}" <<'PY'
import json,sys
f,lv=sys.argv[1],sys.argv[2]
try:
    L=open(f,errors='replace').read().splitlines()
    s=next(i for i in range(len(L)-1,-1,-1) if L[i].rstrip()=='{')
    o=json.loads("\n".join(L[s:])); t=o.get('listener_totals') or {}
    print(f"PROBE,{lv},{t.get('video_frames_decoded',0)},{t.get('audio_frames_decoded',0)},{t.get('decode_errors',0)},{t.get('crc_mismatches',0)}")
except Exception:
    print(f"PROBE,{lv},NA,NA,NA,NA")
PY
}

while :; do
  now=$(date +%s); elapsed=$(( now - START ))
  if (( elapsed >= MAX_SECONDS )); then log "reached ${MAX_SECONDS}s cap — complete, peak ${TOTAL} listeners"; break; fi
  # collect a finished probe (~PROBE_DUR+15s after launch) before launching the next group
  if (( probe_pending )) && (( elapsed >= probe_launch_at + PROBE_DUR + 15 )); then
    log "collecting decode probe @${probe_at} listeners"; collect_probe "${group}" "${probe_at}"; probe_pending=0
  fi
  # next group
  if (( group < MAX_GROUPS )) && (( elapsed >= group * GROUP_INTERVAL )); then
    group=$(( group + 1 )); TOTAL=$(( group * 500 )); echo "${TOTAL}" > "${DIR}/.total"
    log "T=${elapsed}s GROUP ${group}: base +500 => ${TOTAL} listeners (cpu=$(sfu_cpu)m mem=$(sfu_mem)Mi restarts=$(sfu_restarts))"
    for i in 1 2 3 4 5; do ap "soakg-b${group}p${i}" "b${group}p${i}" 0 100 "${DUR}"; done
    ap "soakg-probe${group}" "pr${group}" 0 100 "${PROBE_DUR}"
    probe_pending=1; probe_at=${TOTAL}; probe_launch_at=${elapsed}
  fi
  # unrecoverable checks
  reason=""
  (( $(sfu_panics) - P0 > 0 )) && reason="SFU panic (+$(( $(sfu_panics) - P0 )))"
  (( $(sfu_restarts) - R0 >= 2 )) && reason="${reason:+$reason; }runaway restarts"
  cpu=$(sfu_cpu); if (( TOTAL >= 1000 )) && [ -n "${cpu}" ] && (( cpu < 30 )); then reason="${reason:+$reason; }forwarding flatline (cpu=${cpu}m @ ${TOTAL} listeners)"; fi
  rd=$(sfu_ready); if [ "${rd}" = "false" ]; then [ "${NOTREADY}" -eq 0 ] && NOTREADY=${now}; (( now-NOTREADY>120 )) && reason="${reason:+$reason; }NotReady>120s"; else NOTREADY=0; fi
  if [ -n "${reason}" ]; then log "!!! UNRECOVERABLE T=${elapsed}s @${TOTAL} listeners: ${reason}"; break; fi
  sleep 20
done

# collect any last probe
(( probe_pending )) && { sleep $(( PROBE_DUR + 20 )); collect_probe "${group}" "${probe_at}"; }
log "draining: waiting for base pods to COMPLETE so summaries are captured (up to 200s)"
dl=$(( $(date +%s) + 220 ))
while :; do [ $(date +%s) -gt $dl ] && break
  pend=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "${pend}" -eq 0 ] && break; sleep 15; done
log "collecting base listener summaries (post-completion)"
for p in $("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E 'soakg-b'); do
  "${KCTL[@]}" logs "$p" > "${DIR}/${p}.log" 2>&1 || true; done
log "final: restarts=$(sfu_restarts) panics=$(sfu_panics) ready=$(sfu_ready) peak=${TOTAL}"
log "soakg done"
