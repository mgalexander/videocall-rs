#!/usr/bin/env bash
# materialize-ops.sh — sibling of materialize.sh, targeting the
# videocall_ops rig (prefix vco-). Reads sfu-update/ops-convoy-manifest.yaml
# and writes state to sfu-update/.materialize-state.ops.json. Synchronous
# export is to /gt/videocall_ops/.beads/issues.jsonl.
#
# cd's to /gt/videocall_ops so bd resolves its database against the ops
# rig's .beads/, NOT the dev rig's (we hit cross-rig contamination during
# the videocall bootstrap when this wasn't done — see
# sfu-update/scripts/bd-sync.sh comments).
#
# Usage (from inside gastown-sandbox or via docker exec):
#   docker exec gastown-sandbox \
#       bash /mnt/llms/videocall/sfu-update/scripts/materialize-ops.sh
set -euo pipefail

cd /gt/videocall_ops
exec python3 /mnt/llms/videocall/sfu-update/scripts/materialize.py \
    --manifest /mnt/llms/videocall/sfu-update/ops-convoy-manifest.yaml \
    --state    /mnt/llms/videocall/sfu-update/.materialize-state.ops.json \
    --jsonl    /gt/videocall_ops/.beads/issues.jsonl \
    "$@"
