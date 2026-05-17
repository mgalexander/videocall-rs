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
# Branch freshening (vco-49p):
#   Before restarting the polecat's session, this script forces the polecat's
#   worktree onto a fresh branch named `polecat/<polecat>/<bead-id>@<suffix>`
#   rooted at `origin/<base-branch>` (default: experimental-sfu). Without
#   this, polecats inherit whatever branch they were last on, commit new
#   work on the OLD branch name, and the Refinery's push to upstream rejects
#   non-FF (see vco-ow8.11, vco-2sm incidents).
#
#   If the worktree has uncommitted tracked changes, the sling FAILS — we
#   refuse to silently overwrite work-in-progress. Untracked files (e.g.
#   .beads/, .runtime/) are left alone.
#
#   Pass --no-branch-switch to skip the branch reset (rare; intended for
#   continuing on a pre-existing polecat branch).
#
# See: vco-ow8.8 (original wrapper), vco-49p (branch freshening),
#      sfu-update/ops-log.md (incident history).
#
# Usage:
#   sfu-update/scripts/gt-sling-attached.sh [flags] <bead-id> <rig>/<polecat-name>
#
# Flags:
#   --no-branch-switch    Don't reset the polecat's worktree onto a fresh branch.
#   --base <branch>       Base branch the fresh polecat branch is rooted at.
#                         Default: experimental-sfu (override via $GT_SLING_BASE_BRANCH).
#   -h, --help            Show this usage.
#
# Example:
#   sfu-update/scripts/gt-sling-attached.sh vco-ow8.9 videocall_ops/jasper

set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: gt-sling-attached.sh [flags] <bead-id> <rig>/<polecat-name>

  Attaches <bead-id> to the polecat's hook, resets the polecat's worktree
  onto a fresh branch rooted at origin/<base-branch>, and restarts the
  polecat session so it re-primes and picks up the work. Verifies the
  hook is non-empty before returning success.

  Flags:
    --no-branch-switch    Skip the branch reset (continue on existing branch).
    --base <branch>       Base branch (default: experimental-sfu).
    -h, --help            Show this help.

  Examples:
    gt-sling-attached.sh vco-ow8.9  videocall_ops/jasper
    gt-sling-attached.sh vc-c4e.12  videocall/furiosa
    gt-sling-attached.sh --no-branch-switch vco-99 videocall_ops/jasper
USAGE
}

SWITCH_BRANCH=1
BASE_BRANCH="${GT_SLING_BASE_BRANCH:-experimental-sfu}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-branch-switch)
            SWITCH_BRANCH=0
            shift
            ;;
        --base)
            if [[ $# -lt 2 ]]; then
                echo "gt-sling-attached: --base requires an argument" >&2
                exit 2
            fi
            BASE_BRANCH="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            break
            ;;
        -*)
            echo "gt-sling-attached: unknown flag: $1" >&2
            usage
            exit 2
            ;;
        *)
            break
            ;;
    esac
done

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

# Validate the polecat worktree *before* mutating the hook. If the worktree
# is dirty (or missing), we abort early — that way an aborted sling never
# leaves the hook reassigned with no matching session restart.
NEW_BRANCH=""
if (( SWITCH_BRANCH )); then
    TOWN_ROOT="${GT_TOWN_ROOT:-/gt}"
    WORKTREE="${TOWN_ROOT}/${RIG}/polecats/${NAME}/${RIG}"

    if [[ ! -e "$WORKTREE/.git" ]]; then
        echo "gt-sling-attached: polecat worktree is not a git working tree: $WORKTREE" >&2
        echo "                   (pass --no-branch-switch to skip the branch reset)" >&2
        exit 1
    fi

    # Refuse to clobber uncommitted tracked work. Untracked files (.beads/,
    # .runtime/, etc.) are safe to leave in place — `git checkout -B` won't
    # touch them.
    if ! git -C "$WORKTREE" diff --quiet \
       || ! git -C "$WORKTREE" diff --cached --quiet; then
        echo "gt-sling-attached: polecat worktree has uncommitted tracked changes:" >&2
        echo "                   $WORKTREE" >&2
        git -C "$WORKTREE" status --porcelain -uno >&2 || true
        echo "                   commit, stash, or discard those before re-slinging," >&2
        echo "                   or pass --no-branch-switch to keep the current branch." >&2
        exit 1
    fi

    # Polecat branch convention: polecat/<polecat>/<bead-id>@<suffix>. The
    # suffix makes each sling produce a distinct ref so re-slinging the same
    # bead doesn't collide with a previous branch the Refinery has already
    # consumed.
    SUFFIX="mp$(head -c100 /dev/urandom | tr -dc 'a-z0-9' | head -c6)"
    NEW_BRANCH="polecat/${NAME}/${BEAD}@${SUFFIX}"
fi

echo "→ attaching $BEAD to $HOOK_ADDR" >&2
gt hook "$BEAD" "$HOOK_ADDR"

if (( SWITCH_BRANCH )); then
    # Make sure we're rooted at the *current* upstream tip, not a stale local copy.
    echo "→ fetching origin/$BASE_BRANCH into $WORKTREE" >&2
    git -C "$WORKTREE" fetch --quiet origin "$BASE_BRANCH"

    echo "→ resetting $WORKTREE onto fresh branch $NEW_BRANCH (base: origin/$BASE_BRANCH)" >&2
    git -C "$WORKTREE" checkout -B "$NEW_BRANCH" "origin/$BASE_BRANCH" >&2
else
    echo "→ --no-branch-switch: leaving polecat worktree branch as-is" >&2
fi

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
