#!/usr/bin/env bash
# materialize.sh — thin wrapper around materialize.py.
#
# Idempotent gastown materialisation for the SFU refactor.
# Run from inside gastown-sandbox so `bd` and `gt` are on PATH:
#
#   docker exec -w /mnt/llms/videocall gastown-sandbox \
#       bash sfu-update/scripts/materialize.sh
#
# See sfu-update/convoy-manifest.yaml for the bead/convoy spec.
# State (key -> bd id mapping) lives at sfu-update/.materialize-state.json.
set -euo pipefail

# bd resolves its database by walking up from cwd to find .beads/. The
# rig's beads live at /gt/videocall/.beads/, so we cd there before
# invoking the Python script. The script accepts --manifest / --state /
# --jsonl args; default values target the videocall rig.
#
# To run the same script against the videocall_ops rig, use
# sfu-update/scripts/materialize-ops.sh (sibling wrapper).
cd /gt/videocall
exec python3 /mnt/llms/videocall/sfu-update/scripts/materialize.py "$@"
