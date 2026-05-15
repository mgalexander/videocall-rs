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

cd "$(dirname "$0")/../.."
exec python3 sfu-update/scripts/materialize.py "$@"
