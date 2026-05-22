#!/usr/bin/env bash
# 10k SFU EGRESS soak (find the real ceiling). SFU 4CPU/4Gi single pod.
#   20 presenters on own pods.
#   BASE = LIGHTWEIGHT listeners (--listener-no-decode, ~0.2 CPU/100): +1000/step
#     to 10000 — exercises SFU forwarding/egress without host decode cost.
#   DECODE PROBE per step = 100 FULL-decode listeners (PROBE_DUR) -> decode.csv
#     (video/audio/crc at that load level).
#   metrics.csv every SAMPLE_S: elapsed_s,total_listeners,sfu_cpu_m,sfu_mem_mi,restarts,panics
#   Stop at MAX_SECONDS or UNRECOVERABLE (panic / restarts>=2 / forwarding flatline / NotReady>120s).
# Robust: clean integer helpers, guarded arithmetic (the prior soak died on a
# multiline panic-count breaking `(( ))`).
set -uo pipefail
STEP="${STEP:-1000}"; STEP_INTERVAL="${STEP_INTERVAL:-200}"; MAX_GROUPS="${MAX_GROUPS:-10}"
MAX_SECONDS="${MAX_SECONDS:-2400}"; PROBE_DUR="${PROBE_DUR:-150}"; SAMPLE_S="${SAMPLE_S:-15}"
DUR=$(( MAX_SECONDS + 200 )); PODS_PER_STEP=$(( STEP / 100 ))
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LITE="${DIR}/lite-pod.tmpl.yaml"; PROBE="${DIR}/shard-pod.tmpl.yaml"
ROOM="soak10k-$(date +%s)"; RUN_LOG="${DIR}/run.log"; METRICS="${DIR}/metrics.csv"; DECODE="${DIR}/decode.csv"
: > "${RUN_LOG}"
echo "elapsed_s,total_listeners,sfu_cpu_m,sfu_mem_mi,restarts,panics" > "${METRICS}"
echo "elapsed_s,total_listeners,probe_video,probe_audio,probe_decerr,probe_crc" > "${DECODE}"
SEL="app.kubernetes.io/name=videocall-soak10k"
log(){ echo "[soak10k] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
apply(){ local tmpl="$1" n="$2" sh="$3" s="$4" l="$5" d="$6" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${n}|g" -e "s|__SHARD__|${sh}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${tmpl}" | sed 's|videocall-staircase|videocall-soak10k|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
# --- clean integer helpers (always echo a single integer) ---
cpu_m(){ local v; v=$("${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk 'NR==1{gsub(/m/,"",$2);print $2+0}'); echo "${v:-0}"; }
mem_mi(){ local v; v=$("${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk 'NR==1{gsub(/Mi/,"",$3);print $3+0}'); echo "${v:-0}"; }
restarts(){ local v; v=$("${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null); echo "${v:-0}"; }
panics(){ local v; v=$("${KCTL[@]}" logs rustlemania-webtransport-0 --tail=3000 2>/dev/null | grep -c -E 'JoinHandle polled|panicked'); echo "${v:-0}"; }
ready(){ "${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null; }

echo 0 > "${DIR}/.total"; START=$(date +%s)
( while :; do el=$(( $(date +%s) - START )); t=$(cat "${DIR}/.total" 2>/dev/null || echo 0)
    echo "${el},${t},$(cpu_m),$(mem_mi),$(restarts),$(panics)" >> "${METRICS}"; sleep "${SAMPLE_S}"; done ) & SAMPLER=$!
trap '[ -n "${SAMPLER:-}" ] && kill ${SAMPLER} 2>/dev/null; "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
R0=$(restarts); P0=$(panics); NOTREADY=0
log "ROOM=${ROOM} 4CPU/4Gi; lightweight base +${STEP}/${STEP_INTERVAL}s to $(( MAX_GROUPS*STEP )); decode probe/step; cap ${MAX_SECONDS}s"

log "T=0: 20 presenters + base step1 (${STEP} lite listeners) + decode probe@${STEP}"
for i in $(seq -w 1 20); do apply "${PROBE}" "soak10k-snd${i}" "snd${i}" 1 0 "${DUR}"; done
for i in $(seq 1 ${PODS_PER_STEP}); do apply "${LITE}" "soak10k-b1p${i}" "b1p${i}" 0 100 "${DUR}"; done
TOTAL=${STEP}; echo "${TOTAL}" > "${DIR}/.total"
apply "${PROBE}" "soak10k-pr1" "pr1" 0 100 "${PROBE_DUR}"
group=1; probe_pending=1; probe_at=${STEP}; probe_launch_at=0

collect_probe(){ local g="$1"; local lv="$2"; local f="${DIR}/probe${g}.log"
  "${KCTL[@]}" logs "soak10k-pr${g}" > "${f}" 2>&1 || true
  python3 - "$f" "$lv" >> "${DECODE}" <<'PY'
import json,sys
f,lv=sys.argv[1],sys.argv[2]
try:
    L=open(f,errors='replace').read().splitlines()
    s=next(i for i in range(len(L)-1,-1,-1) if L[i].rstrip()=='{')
    o=json.loads("\n".join(L[s:])); t=o.get('listener_totals') or {}
    print(f"PROBEROW,{lv},{t.get('video_frames_decoded',0)},{t.get('audio_frames_decoded',0)},{t.get('decode_errors',0)},{t.get('crc_mismatches',0)}")
except Exception:
    print(f"PROBEROW,{lv},NA,NA,NA,NA")
PY
}

while :; do
  now=$(date +%s); elapsed=$(( now - START ))
  if (( elapsed >= MAX_SECONDS )); then log "reached ${MAX_SECONDS}s cap — complete, peak ${TOTAL}"; break; fi
  if (( probe_pending == 1 )) && (( elapsed >= probe_launch_at + PROBE_DUR + 15 )); then
    log "collect decode probe @${probe_at}"; collect_probe "${group}" "${probe_at}"; probe_pending=0; fi
  if (( group < MAX_GROUPS )) && (( elapsed >= group * STEP_INTERVAL )); then
    group=$(( group + 1 )); TOTAL=$(( group * STEP )); echo "${TOTAL}" > "${DIR}/.total"
    log "T=${elapsed}s STEP ${group}: base +${STEP} => ${TOTAL} (cpu=$(cpu_m)m mem=$(mem_mi)Mi restarts=$(restarts))"
    for i in $(seq 1 ${PODS_PER_STEP}); do apply "${LITE}" "soak10k-b${group}p${i}" "b${group}p${i}" 0 100 "${DUR}"; done
    apply "${PROBE}" "soak10k-pr${group}" "pr${group}" 0 100 "${PROBE_DUR}"
    probe_pending=1; probe_at=${TOTAL}; probe_launch_at=${elapsed}
  fi
  reason=""
  pn=$(panics); (( pn - P0 > 0 )) && reason="SFU panic (+$(( pn - P0 )))"
  rs=$(restarts); (( rs - R0 >= 2 )) && reason="${reason:+$reason; }runaway restarts"
  c=$(cpu_m); (( TOTAL >= 2000 )) && (( c < 30 )) && reason="${reason:+$reason; }forwarding flatline (cpu=${c}m @ ${TOTAL})"
  rd=$(ready); if [ "${rd}" = "false" ]; then [ "${NOTREADY}" -eq 0 ] && NOTREADY=${now}; (( now-NOTREADY>120 )) && reason="${reason:+$reason; }NotReady>120s"; else NOTREADY=0; fi
  pend=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[?(@.status.phase=="Pending")]}{.metadata.name}{" "}{end}' 2>/dev/null)
  if [ -n "${pend}" ]; then sleep 60; pend2=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[?(@.status.phase=="Pending")]}{.metadata.name}{" "}{end}' 2>/dev/null); [ -n "${pend2}" ] && reason="${reason:+$reason; }pods Pending (host limit): ${pend2:0:60}"; fi
  if [ -n "${reason}" ]; then log "!!! STOP T=${elapsed}s @${TOTAL}: ${reason}"; break; fi
  sleep 20
done

(( probe_pending == 1 )) && { sleep $(( PROBE_DUR + 20 )); collect_probe "${group}" "${probe_at}"; }
log "final: restarts=$(restarts) panics=$(panics) ready=$(ready) peak=${TOTAL}"
log "soak10k done"
