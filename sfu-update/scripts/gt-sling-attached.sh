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

# ----- Pre-flight: enforce rig invariants ------------------------------------
#
# Repeatedly observed wedge: a polecat ends up checked out on the base branch
# (experimental-sfu) instead of its own polecat/<name>/<bead> branch. The
# polecat then auto-saves WIP, which advances the bare repo's base-branch
# ref. Subsequent Refinery pushes from OTHER polecats then fail non-FF
# because the bare repo's view of the base branch has diverged from upstream.
#
# Incident log: nux-on-experimental-sfu, recurring ~every few cycles even
# AFTER vco-49p added per-sling fresh-branch checkout for the TARGET polecat.
# vco-49p doesn't backfill polecats that were already in a bad state.
#
# This pre-flight is INTENTIONALLY conservative (per overseer direction to
# "add an extra validation check even if it is slower in the near term"):
#   1. For every polecat worktree in the rig (not just the sling target),
#      check whether it's on the base branch. If yes, detach it, parking
#      the orphaned commit under refs/heads/parked/auto-<name>-<sha7>.
#   2. After any fix, re-sync the bare repo's base-branch ref to
#      origin/<base-branch> so the next Refinery push goes clean.
#
# Cost: one `git branch --show-current` per polecat in the rig + a fetch +
# an update-ref if any fix was needed. <1 second in practice.
#
# If pre-flight CANNOT fix the state (e.g., a polecat has uncommitted dirty
# work that we'd lose by detaching), it aborts the sling with a clear error
# and tells the operator what to inspect. We do NOT silently overwrite work.
preflight_rig_invariants() {
    local rig="$1"
    local base="$2"
    local town_root="${GT_TOWN_ROOT:-/gt}"
    local pdir="${town_root}/${rig}/polecats"
    local bare="${town_root}/${rig}/.repo.git"

    if [[ ! -d "$bare" ]]; then
        return 0
    fi

    local fixed=0
    if [[ -d "$pdir" ]]; then
        for pwt in "$pdir"/*/"$rig"; do
            [[ -d "$pwt" ]] || continue
            local cur
            cur=$(git -C "$pwt" branch --show-current 2>/dev/null || echo "")
            if [[ "$cur" == "$base" ]]; then
                local pname
                pname=$(basename "$(dirname "$pwt")")

                # Refuse if the worktree has uncommitted tracked changes —
                # we'd lose them on the detach. Operator must resolve.
                if ! git -C "$pwt" diff --quiet \
                   || ! git -C "$pwt" diff --cached --quiet; then
                    echo "preflight: ABORT — polecat ${pname} is on ${base} AND has uncommitted tracked changes." >&2
                    echo "           Inspect: git -C ${pwt} status" >&2
                    echo "           Resolve manually (commit/stash/discard), then re-run the sling." >&2
                    exit 1
                fi

                local head_sha
                head_sha=$(git -C "$pwt" rev-parse HEAD 2>/dev/null || echo "")
                echo "preflight: polecat ${pname} is on ${base}; detaching (head was ${head_sha:0:7})" >&2

                # Park the orphaned head under refs/heads/parked/ so nothing
                # is unreachable. Best-effort; if the ref already exists or
                # update-ref fails, we still detach the worktree.
                if [[ -n "$head_sha" ]]; then
                    git -C "$bare" update-ref "refs/heads/parked/auto-${pname}-${head_sha:0:7}" "$head_sha" 2>/dev/null || true
                fi

                git -C "$pwt" checkout --detach HEAD >&2 || {
                    echo "preflight: failed to detach polecat ${pname} from ${base}; aborting" >&2
                    exit 1
                }
                fixed=1
            fi
        done
    fi

    # After any fix, re-sync bare-repo base ref to upstream so the next push
    # goes clean. Safe to run unconditionally if we fixed anything; skips
    # otherwise.
    if (( fixed )); then
        echo "preflight: re-syncing bare repo ${bare}'s ${base} → origin/${base}" >&2
        git -C "$bare" fetch origin --no-tags --quiet 2>/dev/null || {
            echo "preflight: WARN — could not fetch origin; bare-repo ${base} ref may still be stale" >&2
        }
        if git -C "$bare" rev-parse "refs/remotes/origin/${base}" >/dev/null 2>&1; then
            git -C "$bare" update-ref "refs/heads/${base}" "refs/remotes/origin/${base}" 2>/dev/null || true
            echo "preflight: bare repo ${base} now at $(git -C "$bare" log --oneline -1 "$base")" >&2
        fi
    fi
}

preflight_rig_invariants "$RIG" "$BASE_BRANCH"

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

# ----- Post-flight invariant check -------------------------------------------
#
# Verify the target polecat's worktree did not silently end up back on the
# base branch (which would be the failure mode the pre-flight catches at
# the START of the NEXT sling — but by then a WIP auto-save may have
# already advanced the bare-repo ref). Catching it here, immediately after
# the session restart, gives the operator a chance to recover before any
# damage is done.
if (( SWITCH_BRANCH )); then
    POST_BRANCH=$(git -C "$WORKTREE" branch --show-current 2>/dev/null || echo "")
    if [[ "$POST_BRANCH" == "$BASE_BRANCH" ]]; then
        echo "gt-sling-attached: POST-FLIGHT FAILED — polecat worktree is on '$BASE_BRANCH' after sling" >&2
        echo "                   (expected '$NEW_BRANCH'). gt session restart may have reset it." >&2
        echo "                   recover manually: git -C $WORKTREE checkout -B $NEW_BRANCH origin/$BASE_BRANCH" >&2
        exit 1
    fi
    if [[ "$POST_BRANCH" != "$NEW_BRANCH" ]]; then
        echo "gt-sling-attached: POST-FLIGHT WARN — polecat worktree is on '$POST_BRANCH'" >&2
        echo "                   (expected '$NEW_BRANCH'). Continuing — branch isn't $BASE_BRANCH so" >&2
        echo "                   the wedge risk is contained, but verify the polecat session." >&2
    fi
fi

echo "✓ $BEAD attached to $HOOK_ADDR and session restarted" >&2
