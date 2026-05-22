#!/usr/bin/env bash
# Scaling-pattern matrix: 10 tests, 10-400 users, presenters 4/10/20, full
# real-decode + CRC validation. Patterns: large join group, large join/depart,
# slow joiners, join/rejoin, stepped. SFU 4CPU/4Gi (not the variable at this scale).
# Per test: collect every listener summary (decode video/audio + crc) AFTER pods
# complete, track SFU peak cpu/mem + restarts. Results -> matrix-results.csv.
set -uo pipefail
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
RES="${DIR}/matrix-results.csv"; RUN_LOG="${DIR}/run.log"; : > "${RUN_LOG}"
echo "test,pattern,presenters,peak_users,sfu_cpu_peak_m,sfu_mem_peak_mi,sfu_restarts_delta,video_decoded,audio_decoded,crc_mismatches,unexplained_gaps,listeners_summarized,listeners_with_video,listeners_with_audio" > "${RES}"
SEL=app.kubernetes.io/name=videocall-mtx
log(){ echo "[matrix] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
ap(){ local n="$1" sh="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${n}|g" -e "s|__SHARD__|${sh}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-mtx|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
cpu_m(){ local v; v=$("${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk 'NR==1{gsub(/m/,"",$2);print $2+0}'); echo "${v:-0}"; }
mem_mi(){ local v; v=$("${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk 'NR==1{gsub(/Mi/,"",$3);print $3+0}'); echo "${v:-0}"; }
restarts(){ local v; v=$("${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null); echo "${v:-0}"; }

PEAKF="${DIR}/.peak"; SAMPLER=""
start_peak(){ echo "0 0" > "${PEAKF}"; ( while :; do c=$(cpu_m); m=$(mem_mi); read pc pm < "${PEAKF}"; (( c>pc )) && pc=$c; (( m>pm )) && pm=$m; echo "$pc $pm" > "${PEAKF}"; sleep 8; done ) & SAMPLER=$!; }
stop_peak(){ [ -n "${SAMPLER}" ] && kill ${SAMPLER} 2>/dev/null; SAMPLER=""; }

wait_done(){ local dl=$(( $(date +%s) + ${1:-400} )); while :; do [ $(date +%s) -gt $dl ] && { log "  wait deadline"; break; }
  local p; p=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "${p}" -eq 0 ] && break; sleep 10; done; }

score(){ # score TEST PATTERN PRESENTERS PEAKUSERS R0
  local test="$1" pat="$2" P="$3" peak="$4" R0="$5"
  read pcpu pmem < "${PEAKF}"; local rd=$(( $(restarts) - R0 ))
  # collect every listener pod summary for this test
  for p in $("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E "${test}-l"); do
    "${KCTL[@]}" logs "$p" > "${DIR}/${test}_${p}.log" 2>&1 || true; done
  python3 - "$test" "$pat" "$P" "$peak" "$pcpu" "$pmem" "$rd" "${DIR}" >> "${RES}" <<'PY'
import json,glob,sys,os
test,pat,P,peak,pcpu,pmem,rd,dirp=sys.argv[1:9]
v=a=c=g=n=lv=la=0
for f in glob.glob(os.path.join(dirp,f"{test}_*-l*.log")):
    L=open(f,errors='replace').read().splitlines()
    s=next((i for i in range(len(L)-1,-1,-1) if L[i].rstrip()=='{'),None)
    if s is None: continue
    try: o=json.loads("\n".join(L[s:]))
    except: continue
    lt=o.get('listener_totals') or {}
    v+=lt.get('video_frames_decoded',0) or 0; a+=lt.get('audio_frames_decoded',0) or 0
    c+=lt.get('crc_mismatches',0) or 0; g+=lt.get('unexplained_gaps',0) or 0
    for b in o.get('per_bot',[]):
        n+=1
        if (b.get('video_frames_decoded') or 0)>0: lv+=1
        if (b.get('audio_frames_decoded') or 0)>0: la+=1
print(f"{test},{pat},{P},{peak},{pcpu},{pmem},{rd},{v},{a},{c},{g},{n},{lv},{la}")
PY
  log "  scored ${test}: $(tail -1 "${RES}")"
  "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
}

run_senders(){ local P="$1" d="$2"; for i in $(seq -w 1 "$P"); do ap "${TEST}-snd${i}" "s${i}" 1 0 "$d"; done; }
trap 'stop_peak; "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT

# ---- the 10 tests ----
runtest(){ TEST="$1"; ROOM="mtx-$1-$(date +%s)"; }

log "=== MATRIX START (SFU 4CPU/4Gi) ==="

# 1. Large join group, 4 presenters, 100 co-arrival
runtest t1; R0=$(restarts); start_peak; log "T1 large-join 4p/100 co-arrival"
run_senders 4 260; for i in 1; do ap "${TEST}-l${i}" "l${i}" 0 100 240; done
wait_done 360; stop_peak; score t1 large-join 4 100 "$R0"

# 2. Large join group, 10 presenters, 400 co-arrival
runtest t2; R0=$(restarts); start_peak; log "T2 large-join 10p/400 co-arrival"
run_senders 10 300; for i in 1 2 3 4; do ap "${TEST}-l${i}" "l${i}" 0 100 280; done
wait_done 420; stop_peak; score t2 large-join 10 400 "$R0"

# 3. Large join group, 20 presenters, 400 co-arrival
runtest t3; R0=$(restarts); start_peak; log "T3 large-join 20p/400 co-arrival"
run_senders 20 300; for i in 1 2 3 4; do ap "${TEST}-l${i}" "l${i}" 0 100 280; done
wait_done 420; stop_peak; score t3 large-join 20 400 "$R0"

# 4. Large join/DEPART, 10 presenters: 300 join, depart 150, join 150
runtest t4; R0=$(restarts); start_peak; log "T4 join/depart 10p/300->depart150->join150"
run_senders 10 360; for i in 1 2 3; do ap "${TEST}-l${i}" "l${i}" 0 100 320; done
sleep 90; log "  depart 150 (delete l2,l3 partial)"; "${KCTL[@]}" delete pod ${TEST}-l2 ${TEST}-l3 --ignore-not-found --wait=false >/dev/null 2>&1
sleep 30; log "  join 150 (l4,l5)"; for i in 4 5; do ap "${TEST}-l${i}" "l${i}" 0 75 200; done
wait_done 420; stop_peak; score t4 join-depart 10 300 "$R0"

# 5. Large join/DEPART, 20 presenters: 400 join, depart 200, join 200
runtest t5; R0=$(restarts); start_peak; log "T5 join/depart 20p/400->depart200->join200"
run_senders 20 380; for i in 1 2 3 4; do ap "${TEST}-l${i}" "l${i}" 0 100 340; done
sleep 100; log "  depart 200"; "${KCTL[@]}" delete pod ${TEST}-l3 ${TEST}-l4 --ignore-not-found --wait=false >/dev/null 2>&1
sleep 30; log "  join 200 (l5,l6)"; for i in 5 6; do ap "${TEST}-l${i}" "l${i}" 0 100 200; done
wait_done 440; stop_peak; score t5 join-depart 20 400 "$R0"

# 6. SLOW joiners, 4 presenters: trickle 50 every 30s to 200
runtest t6; R0=$(restarts); start_peak; log "T6 slow-join 4p/trickle to 200"
run_senders 4 360; for i in 1 2 3 4; do ap "${TEST}-l${i}" "l${i}" 0 50 $((300-i*30)); sleep 30; done
wait_done 420; stop_peak; score t6 slow-join 4 200 "$R0"

# 7. SLOW joiners, 10 presenters: trickle 50 every 25s to 400
runtest t7; R0=$(restarts); start_peak; log "T7 slow-join 10p/trickle to 400"
run_senders 10 420; for i in 1 2 3 4 5 6 7 8; do ap "${TEST}-l${i}" "l${i}" 0 50 $((360-i*25)); sleep 25; done
wait_done 460; stop_peak; score t7 slow-join 10 400 "$R0"

# 8. JOIN/REJOIN, 10 presenters: 200 join; cycle: 100 leave+rejoin x2
runtest t8; R0=$(restarts); start_peak; log "T8 join/rejoin 10p/200 x2 cycles"
run_senders 10 420; for i in 1 2; do ap "${TEST}-l${i}" "l${i}" 0 100 380; done
for cyc in 1 2; do sleep 80; log "  rejoin cycle $cyc"; "${KCTL[@]}" delete pod ${TEST}-l2 --ignore-not-found --wait=true >/dev/null 2>&1; sleep 5; ap "${TEST}-l2" "l2c${cyc}" 0 100 200; done
wait_done 460; stop_peak; score t8 join-rejoin 10 200 "$R0"

# 9. JOIN/REJOIN, 20 presenters: 300 join; 150 leave+rejoin x2
runtest t9; R0=$(restarts); start_peak; log "T9 join/rejoin 20p/300 x2"
run_senders 20 440; for i in 1 2 3; do ap "${TEST}-l${i}" "l${i}" 0 100 400; done
for cyc in 1 2; do sleep 80; log "  rejoin cycle $cyc"; "${KCTL[@]}" delete pod ${TEST}-l2 ${TEST}-l3 --ignore-not-found --wait=true >/dev/null 2>&1; sleep 5; for i in 2 3; do ap "${TEST}-l${i}" "l${i}c${cyc}" 0 100 200; done; done
wait_done 480; stop_peak; score t9 join-rejoin 20 300 "$R0"

# 10. STEPPED ramp 10->400, 10 presenters: +~50 every 20s
runtest t10; R0=$(restarts); start_peak; log "T10 step 10p/ramp 10->400"
run_senders 10 420; ap "${TEST}-l0" "l0" 0 10 380; sleep 20
for i in 1 2 3 4 5 6 7 8; do ap "${TEST}-l${i}" "l${i}" 0 50 $((360-i*20)); sleep 20; done
wait_done 460; stop_peak; score t10 step-ramp 10 400 "$R0"

log "=== MATRIX DONE ==="
column -t -s, "${RES}" | tee -a "${RUN_LOG}"
