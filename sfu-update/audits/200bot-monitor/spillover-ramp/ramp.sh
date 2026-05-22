#!/usr/bin/env bash
# Spillover capacity ramp — 500-listener increments, multi-pod, exercises the
# redirect + spillover path (vc-xnp redirect-uni-stream + vc-85p admit-to-spill).
#
# Each 500-listener batch = 5 pods x 100 real-decode listeners. All into ONE
# room, so the owner crosses the 180 soft cap on batch 1 and spillover must
# engage. Logs SFU pod load spread each batch (proof spillover distributes).
#
# STOP on: SFU restart, pod OOM/Fail/CrashLoop, pods stuck Pending, SFU panic.
# Usage: ramp.sh [REPLICAS] [MAX_BATCHES]   defaults: 3 12  (12*500 = 6000)
set -uo pipefail

REPLICAS="${1:-3}"
MAX_BATCHES="${2:-12}"
PODS_PER_BATCH="${PODS_PER_BATCH:-5}"
BATCH_GAP="${BATCH_GAP:-90}"
PENDING_GRACE="${PENDING_GRACE:-75}"
DURATION="${DURATION:-9000}"
CTX=k3d-videocall-local
KCTL=(kubectl --context "${CTX}" -n default)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMPL="${DIR}/shard-pod.tmpl.yaml"
ROOM="spillramp-$(date +%s)"
RUN_LOG="${DIR}/ramp.log"; TOP_LOG="${DIR}/ramp-top.log"
: > "${RUN_LOG}"; : > "${TOP_LOG}"
SELECTOR="app.kubernetes.io/name=videocall-spillramp"

log() { echo "[spillramp] $(date '+%H:%M:%S') $*" | tee -a "${RUN_LOG}"; }
apply_pod() {
    local name="$1" shard="$2" senders="$3" listeners="$4" m
    m="$(mktemp --suffix=.yaml)"
    sed -e "s|__NAME__|${name}|g" -e "s|__SHARD__|${shard}|g" -e "s|__ROOM__|${ROOM}|g" \
        -e "s|__SENDERS__|${senders}|g" -e "s|__LISTENERS__|${listeners}|g" -e "s|__DURATION__|${DURATION}|g" \
        "${TMPL}" | sed 's|videocall-staircase|videocall-spillramp|g' > "${m}"
    "${KCTL[@]}" apply -f "${m}" >/dev/null 2>&1; rm -f "${m}"
}
sfu_restarts() { local v; v=$("${KCTL[@]}" get pods -l app.kubernetes.io/name=rustlemania-webtransport -o jsonpath='{range .items[*]}{.status.containerStatuses[*].restartCount}{"\n"}{end}' 2>/dev/null | awk 'BEGIN{s=0}{for(i=1;i<=NF;i++)s+=$i}END{print s+0}'); echo "${v:-0}"; }
sfu_panics()   { local v; v=$("${KCTL[@]}" logs -l app.kubernetes.io/name=rustlemania-webtransport --tail=800 2>/dev/null | grep -c -iE 'panic|fatal runtime' 2>/dev/null); echo "${v:-0}"; }
ramp_pod_problems() {
    "${KCTL[@]}" get pods -l "${SELECTOR}" -o json 2>/dev/null | python3 - 2>/dev/null <<'PY'
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(""); sys.exit(0)
p=[]
for x in d.get('items',[]):
    n=x.get('metadata',{}).get('name','?'); ph=x.get('status',{}).get('phase','')
    if ph in('Failed','Unknown'): p.append(f"{n}:{ph}")
    for cs in x.get('status',{}).get('containerStatuses',[]) or []:
        t=(cs.get('state',{}) or {}).get('terminated',{}) or {}
        if t.get('reason') in('OOMKilled','Error'): p.append(f"{n}:{t.get('reason')}")
        w=(cs.get('state',{}) or {}).get('waiting',{}) or {}
        if w.get('reason') in('CrashLoopBackOff','ImagePullBackOff','ErrImagePull'): p.append(f"{n}:{w.get('reason')}")
print("|".join(p))
PY
    return 0
}
pending_pods() { "${KCTL[@]}" get pods -l "${SELECTOR}" -o jsonpath='{range .items[?(@.status.phase=="Pending")]}{.metadata.name}{" "}{end}' 2>/dev/null; return 0; }
sfu_pod_distribution() { "${KCTL[@]}" top pods --no-headers 2>/dev/null | grep 'rustlemania-webtransport' | awk '{print $1"="$2"/"$3}' | paste -sd' ' -; }
cleanup() { "${KCTL[@]}" delete pod -l "${SELECTOR}" --ignore-not-found --wait=false >/dev/null 2>&1; }
trap cleanup EXIT

log "ROOM=${ROOM} REPLICAS=${REPLICAS} step=$((PODS_PER_BATCH*100)) MAX=$((MAX_BATCHES*PODS_PER_BATCH*100)) gap=${BATCH_GAP}s"
log "scaling SFU to ${REPLICAS}"; "${KCTL[@]}" scale statefulset/rustlemania-webtransport --replicas="${REPLICAS}" >/dev/null 2>&1
"${KCTL[@]}" rollout status statefulset/rustlemania-webtransport --timeout=240s >/dev/null 2>&1
"${KCTL[@]}" delete pod -l "${SELECTOR}" --ignore-not-found --wait=true >/dev/null 2>&1
R0=$(sfu_restarts); P0=$(sfu_panics)
log "baseline: SFU restarts=${R0} panics=${P0}"
log "launching 10 senders (persistent)"; apply_pod spillramp-s s 10 0; sleep 30

CEILING=""
for ((b=1;b<=MAX_BATCHES;b++)); do
    total=$(( b*PODS_PER_BATCH*100 ))
    log "=== batch ${b}: +$((PODS_PER_BATCH*100)) listeners (target total ${total}) ==="
    for ((p=1;p<=PODS_PER_BATCH;p++)); do apply_pod "spillramp-b${b}p${p}" "b${b}p${p}" 0 100; done
    sleep "${BATCH_GAP}"
    echo "----- $(date '+%H:%M:%S') batch=${b} total=${total} -----" >> "${TOP_LOG}"
    "${KCTL[@]}" top pods --no-headers 2>/dev/null | grep -E 'spillramp|rustlemania-webtransport' >> "${TOP_LOG}" 2>/dev/null
    R=$(sfu_restarts); P=$(sfu_panics); probs=$(ramp_pod_problems); pend=$(pending_pods)
    log "  batch ${b}: SFU restarts=${R}(+$((R-R0))) panics=${P}(+$((P-P0)))"
    log "  SFU pod load (spillover spread): $(sfu_pod_distribution)"
    reason=""
    (( R>R0 )) && reason="SFU restarted (+$((R-R0)))"
    (( P>P0 )) && reason="${reason:+$reason; }SFU panic (+$((P-P0)))"
    [ -n "${probs}" ] && reason="${reason:+$reason; }pod problems: ${probs}"
    if [ -n "${pend}" ]; then sleep "${PENDING_GRACE}"; pend2=$(pending_pods); [ -n "${pend2}" ] && reason="${reason:+$reason; }stuck Pending: ${pend2}"; fi
    if [ -n "${reason}" ]; then CEILING="${total}"; log "!!! ERRORS at batch ${b} (${total} listeners): ${reason}"; break; fi
    log "  batch ${b} clean (${total} listeners healthy)."
done
[ -z "${CEILING}" ] && log "reached MAX ($(( MAX_BATCHES*PODS_PER_BATCH*100 )) listeners) with NO errors" || log "CEILING: ${CEILING} listeners"
log "final SFU pod load: $(sfu_pod_distribution)"
log "ramp done"
