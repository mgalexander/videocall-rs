#!/usr/bin/env bash
# bd-sync.sh — run a state-mutating `bd` command and immediately flush
# the result to .beads/issues.jsonl so the next bd command can't auto-
# import stale state and clobber it.
#
# Context: gastown's bd has auto-export.interval=1s in this rig but the
# export is timer-based and not synchronous with writes. A `bd close`
# followed quickly by `bd list` (or any other bd command) sees the
# pre-close state because the JSONL was not yet refreshed. The auto-
# import on the next session then overwrites the in-memory close with
# the stale JSONL. Calling `bd export --all` synchronously after the
# write fixes it. This wrapper makes that one step.
#
# Usage (run inside gastown-sandbox from /gt/videocall):
#   bash sfu-update/scripts/bd-sync.sh close vc-c4e.20 --reason "..."
#   bash sfu-update/scripts/bd-sync.sh update vc-c4e.21 --status open
#
# Or invoke from host via:
#   docker exec -w /gt/videocall gastown-sandbox \
#       bash /mnt/llms/videocall/sfu-update/scripts/bd-sync.sh <bd-args>
#
# See also: sfu-update/ops-log.md "fix bd state inconsistency permanently".

set -euo pipefail

# Always operate from the videocall rig directory. Without this, bd's
# auto-discovery may resolve a parent .beads/ (e.g. /gt/.beads — the HQ
# town beads), and `bd export --all` will then dump HQ data INTO this
# rig's issues.jsonl. We hit exactly that in the recovery iteration and
# had to regenerate JSONL from Dolt.
RIG_DIR="/gt/videocall"
JSONL="${RIG_DIR}/.beads/issues.jsonl"

if [[ ! -d "$RIG_DIR" ]]; then
    echo "bd-sync: rig dir $RIG_DIR not found inside container" >&2
    exit 2
fi
if [[ $# -lt 1 ]]; then
    echo "usage: bd-sync.sh <bd-subcommand> [args...]" >&2
    echo "  Wraps a bd command and forces 'bd export --all' after." >&2
    echo "  Always runs with cwd=$RIG_DIR to keep auto-discovery on the rig db." >&2
    exit 2
fi

cd "$RIG_DIR"

bd "$@"
status=$?

# Always flush, even on bd-error — useful for partial-batch recovery
# when one of N closes failed but others succeeded.
bd export --all -o "$JSONL" >/dev/null
echo "  ✓ flushed bd state to $JSONL" >&2

exit $status
