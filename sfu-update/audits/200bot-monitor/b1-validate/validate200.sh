#!/usr/bin/env bash
# B1 validation: the FAILING late-join shape (slow-join to 400 + 20 presenters).
# Matrix baseline for this shape: ~12-50% decode (t7/t10). B1 (multi-thread fan-out)
# should let LATE joiners decode (processing choke removed). NOTE: local cluster has
# ~unlimited bandwidth, so this validates the PROCESSING fix, NOT the prod NIC ceiling
# (that's vc-stee/B13). Full real-decode + crc.
set -uo pipefail
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="b1val-$(date +%s)"; RUN="${DIR}/run.log"; : > "${RUN}"
SEL=app.kubernetes.io/name=videocall-b1val
log(){ echo "[b1val] $(date '+%H:%M:%S') $*" | tee -a "${RUN}"; }
ap(){ local n="$1" sh="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${n}|g" -e "s|__SHARD__|${sh}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-b1val|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
cpu_m(){ local v; v=$("${KCTL[@]}" top pod -l app.kubernetes.io/name=rustlemania-webtransport --no-headers 2>/dev/null | awk 'NR==1{gsub(/m/,"",$2);print $2+0}'); echo "${v:-0}"; }
trap '"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
rm -f "${DIR}"/b1val-l*.log "${DIR}"/b1val-snd*.log 2>/dev/null; "${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
log "ROOM=${ROOM}: 20 presenters + slow-join 50/25s to 200 (v1 target, 10 presenters) (the failing shape)"
for i in $(seq -w 1 10); do ap "b1val-snd${i}" "s${i}" 1 0 460; done
for i in 1 2 3 4; do ap "b1val-l${i}" "l${i}" 0 50 $((400-i*25)); log "  wave $i (+50 => $((i*50))) cpu=$(cpu_m)m"; sleep 25; done
log "all 400 joined; soaking"
# wait for completion
dl=$(( $(date +%s) + 420 )); while [ $(date +%s) -lt $dl ]; do
  p=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "${p}" -eq 0 ] && break; sleep 10; done
for p in $("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E 'l[0-9]'); do
  "${KCTL[@]}" logs "$p" > "${DIR}/${p}.log" 2>&1 || true; done
python3 - "${DIR}" <<'PY' | tee -a "${RUN}"
import json,glob,sys,os
d=sys.argv[1]; v=a=c=n=lv=la=0
for f in sorted(glob.glob(os.path.join(d,"b1val-l*.log"))):
    L=open(f,errors='replace').read().splitlines()
    s=next((i for i in range(len(L)-1,-1,-1) if L[i].rstrip()=='{'),None)
    if s is None: continue
    try: o=json.loads("\n".join(L[s:]))
    except: continue
    t=o.get('listener_totals') or {}; v+=t.get('video_frames_decoded',0) or 0; a+=t.get('audio_frames_decoded',0) or 0; c+=t.get('crc_mismatches',0) or 0
    for b in o.get('per_bot',[]):
        n+=1
        if (b.get('video_frames_decoded') or 0)>0: lv+=1
        if (b.get('audio_frames_decoded') or 0)>0: la+=1
print(f"B1VAL RESULT: listeners={n} with_video={lv} with_audio={la} video_frames={v} audio_frames={a} crc_mismatches={c}")
print(f"  (matrix baseline this shape ~12%% video/audio; B1 should lift late joiners)")
PY
log "b1val done"
