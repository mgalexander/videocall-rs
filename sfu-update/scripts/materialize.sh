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

# bd resolves its database by walking up from cwd to find .beads/.
# The rig's beads live at /gt/videocall/.beads/, NOT at /mnt/llms/videocall/.beads/,
# so we run from the rig path. The manifest is referenced by absolute path
# inside materialize.py, so the cwd doesn't affect manifest discovery.
cd /gt/videocall
exec python3 /mnt/llms/videocall/sfu-update/scripts/materialize.py "$@"
