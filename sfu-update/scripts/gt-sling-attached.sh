#!/usr/bin/env bash
# gt-sling-attached.sh — sling a bead to a polecat AND make sure it actually
# lands on the polecat's hook before returning.
#
# Context: `gt sling` and `gt convoy launch` produce a wisp for the polecat,
# but the wisp frequently fails to land on the polecat's hook. The polecat
# session spawns, primes, sees an empty hook, and self-defers. The recovery
# is always:
#
#     gt hook <bead> <rig>/polecats/<polecat>
#     gt session restart <rig>/<polecat>
#
# This wrapper bundles that recovery into the launch path so we don't have
# to babysit every sling. Authoritative fix belongs upstream in gastown
# (sling-wisp formula / pool-init identity registration). Until that lands,
# use this wrapper instead of bare `gt sling`.
#
# See: vco-ow8.8 (this bead), sfu-update/ops-log.md (incident history).
#
# Usage:
#   sfu-update/scripts/gt-sling-attached.sh <bead-id> <rig>/<polecat-name>
#
# Example:
#   sfu-update/scripts/gt-sling-attached.sh vco-ow8.9 videocall_ops/jasper

set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: gt-sling-attached.sh <bead-id> <rig>/<polecat-name>

  Attaches <bead-id> to the polecat's hook and restarts the polecat session
  so it re-primes and picks up the work. Verifies the hook is non-empty
  before returning success.

  Examples:
    gt-sling-attached.sh vco-ow8.9  videocall_ops/jasper
    gt-sling-attached.sh vc-c4e.12  videocall/furiosa
USAGE
}

if [[ $# -ne 2 ]]; then
    usage
    exit 2
fi

BEAD="$1"
TARGET="$2"  # <rig>/<polecat-name>

# Parse rig/name. Reject anything that isn't exactly "<rig>/<name>" so we
# don't accidentally accept a pre-canonicalized "<rig>/polecats/<name>".
if [[ "$TARGET" != */* ]] || [[ "$TARGET" == */*/* ]]; then
    echo "gt-sling-attached: target must be '<rig>/<polecat-name>', got '$TARGET'" >&2
    exit 2
fi
RIG="${TARGET%%/*}"
NAME="${TARGET##*/}"
HOOK_ADDR="${RIG}/polecats/${NAME}"   # form gt hook expects
SESSION_ADDR="${RIG}/${NAME}"          # form gt session restart expects

echo "→ attaching $BEAD to $HOOK_ADDR" >&2
gt hook "$BEAD" "$HOOK_ADDR"

echo "→ restarting session $SESSION_ADDR" >&2
gt session restart "$SESSION_ADDR"

# Give the restarted session a beat to prime before we inspect the hook.
sleep 2

echo "→ verifying hook for $HOOK_ADDR" >&2
HOOK_OUT="$(gt hook show "$HOOK_ADDR" 2>&1)"
echo "$HOOK_OUT" >&2

# `gt hook show` prints one line like:
#   videocall_ops/polecats/jasper: vco-ow8.9 'title' [hooked]
# An empty hook prints something containing "no hook" / "empty" / no bead ID.
# We require the requested bead id to appear on the line for success.
if ! grep -qF "$BEAD" <<<"$HOOK_OUT"; then
    echo "gt-sling-attached: hook verification FAILED — $BEAD did not land on $HOOK_ADDR" >&2
    echo "                   recover manually with:" >&2
    echo "                     gt hook $BEAD $HOOK_ADDR" >&2
    echo "                     gt session restart $SESSION_ADDR" >&2
    exit 1
fi

echo "✓ $BEAD attached to $HOOK_ADDR and session restarted" >&2
