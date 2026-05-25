#!/usr/bin/env bash
# Accuracy diagnostic: CO-ARRIVAL (all at T=0), modest load, long duration — best case
# for 100% reach. Full decode + crc. Per-listener breakdown to explain any shortfall.
set -uo pipefail
CTX=k3d-videocall-local; KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="accuracy-$(date +%s)"; RUN="${DIR}/run.log"; : > "${RUN}"
SEL=app.kubernetes.io/name=videocall-acc
PRES=${PRES:-5}; LPODS=${LPODS:-2}; PER=${PER:-100}; DUR=${DUR:-240}
log(){ echo "[acc] $(date '+%H:%M:%S') $*" | tee -a "${RUN}"; }
ap(){ local n="$1" sh="$2" s="$3" l="$4" d="$5" m; m="$(mktemp --suffix=.yaml)"
  sed -e "s|__NAME__|${n}|g" -e "s|__SHARD__|${sh}|g" -e "s|__ROOM__|${ROOM}|g" \
      -e "s|__SENDERS__|${s}|g" -e "s|__LISTENERS__|${l}|g" -e "s|__DURATION__|${d}|g" \
      "${TMPL}" | sed 's|videocall-staircase|videocall-acc|g' > "${m}"
  "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"; }
rm -f "${DIR}"/acc-*.log
"${KCTL[@]}" delete pod -l "${SEL}" --ignore-not-found --wait=true >/dev/null 2>&1
TOTAL=$((LPODS*PER))
log "CO-ARRIVAL: ${PRES} presenters + ${TOTAL} listeners (${LPODS}x${PER}) all @ T=0, dur ${DUR}s, full decode+crc"
for i in $(seq -w 1 ${PRES}); do ap "acc-snd${i}" "s${i}" 1 0 $((DUR+40)); done
for i in $(seq 1 ${LPODS}); do ap "acc-l${i}" "l${i}" 0 ${PER} ${DUR}; done
log "launched; waiting for completion"
dl=$(( $(date +%s) + DUR + 120 ))
while [ $(date +%s) -lt $dl ]; do
  p=$("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -cv -E '^(Succeeded|Failed)$' || true)
  [ "${p}" -eq 0 ] && break; sleep 15; done
log "collecting"
for p in $("${KCTL[@]}" get pods -l "${SEL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E 'acc-l'); do
  "${KCTL[@]}" logs "$p" > "${DIR}/${p}.log" 2>&1 || true; done
python3 - "${DIR}" "${TOTAL}" <<'PY' | tee -a "${RUN}"
import json,glob,sys,os
d,total=sys.argv[1],int(sys.argv[2])
both=aud_only=vid_only=neither=n=0; vmiss=[]; nmiss=[]
for f in sorted(glob.glob(os.path.join(d,"acc-l*.log"))):
    L=open(f,errors='replace').read().splitlines()
    s=next((i for i in range(len(L)-1,-1,-1) if L[i].rstrip()=='{'),None)
    if s is None: continue
    try: o=json.loads("\n".join(L[s:]))
    except: continue
    for b in o.get('per_bot',[]):
        n+=1; v=b.get('video_frames_decoded') or 0; a=b.get('audio_frames_decoded') or 0
        bid=b.get('bot_id') or b.get('id') or b.get('session_id') or '?'
        if v>0 and a>0: both+=1
        elif a>0 and v==0: aud_only+=1; vmiss.append(bid)
        elif v>0 and a==0: vid_only+=1
        else: neither+=1; nmiss.append(bid)
print(f"\n=== ACCURACY ({n}/{total} listeners summarized) ===")
print(f"  both video+audio : {both}  ({100*both//max(n,1)}%)")
print(f"  audio only (v=0) : {aud_only}   <- video keyframe/startup (Defect-2)")
print(f"  video only (a=0) : {vid_only}")
print(f"  NEITHER (0/0)    : {neither}   <- never received media (connect/join/subscribe)")
print(f"  MISSING summary  : {total-n}   <- bot never reported (connect/startup failure)")
if vmiss[:8]: print("  sample video-missing bot ids:", vmiss[:8])
if nmiss[:8]: print("  sample neither bot ids:", nmiss[:8])
PY
log "diag done"
