# ADR-0006: Refinery Push Contract (C-8)

- **Status:** Accepted
- **Date:** 2026-05-15
- **Deciders:** overseer (malexander)
- **Related:** [`SCALE-UP.md`](../SCALE-UP.md), [`FANOUT.md`](../FANOUT.md), [`ops-log.md`](../ops-log.md), [`GAP-ANALYSIS.md`](../GAP-ANALYSIS.md) §C-8.

## Context

`gt rig add videocall file:///mnt/llms/videocall …` makes the user's working clone (`/mnt/llms/videocall/`) the rig's git upstream. When a polecat finishes work and runs `gt done`, the Refinery merges the polecat's branch into `experimental-sfu` inside the rig's bare repo at `/gt/videocall/.repo.git/` — and then attempts to push the merged result back to the upstream (`/mnt/llms/videocall/`).

This push is refused by git's default `receive.denyCurrentBranch` because the upstream is a non-bare clone with `experimental-sfu` currently checked out. Observed during P0 Wave 1 (bootstrap): merge queue entry `vc-wisp-kzk` sat in status `ready` indefinitely because the Refinery could not push.

The user's standing rule (see [[feedback-local-only-push]]): *"keeping the work local in worktrees until merged and manually approved to push."* Two interpretations:
1. The Refinery's "push to upstream" *is* a push that needs approval.
2. The Refinery's push is internal to the local environment and only `git push origin` to GitHub is the gated step.

The wording is ambiguous, but the spirit — review polecat work before it lands in the user's clone — favors interpretation 1.

## Decision

**Convoys default to `--merge=local`. The Refinery does NOT push to the upstream (`/mnt/llms/videocall/`). The user manually fetches and merges from the rig.**

Concretely:

1. **Convoy materialisation default.** Every convoy created via `gt convoy create` / `gt convoy stage` / `materialize.py` uses merge strategy `local`. This is set on the convoy's metadata; the auto-convoy created by `gt sling` for single-issue dispatch also defaults to `local`.
2. **Refinery behaviour.** The Refinery still merges polecat branches into `experimental-sfu` inside the rig's bare repo (`/gt/videocall/.repo.git/`) when CI gates pass. It then **stops**. No upstream push attempted.
3. **User sync path.** The user's local clone (`/mnt/llms/videocall/`) has a `rig` remote pointing at `/mnt/llms/gas-town/town/videocall/.repo.git/` (added during bootstrap). To pull polecat-landed work:
   ```bash
   git fetch rig
   git diff experimental-sfu rig/experimental-sfu     # review
   git merge --ff-only rig/experimental-sfu           # land if FF
   ```
   This is the manual approval gate.
4. **Auto-mode classifier reinforcement.** The Claude Code auto-mode classifier already blocks attempts to FF-merge polecat branches into the user's clone (observed during bootstrap). That behaviour is correct and consistent with this ADR.
5. **GitHub remote push (`origin`).** Untouched by this ADR. `git push origin experimental-sfu` requires explicit per-push approval from the overseer, no exceptions.

## Consequences

**Pro:**
- The Refinery can never silently land polecat work into the user's clone, eliminating the bootstrap-observed wedge.
- The "local-only, manually approved" contract is enforceable in code (convoy default) and in habit (the manual `git fetch rig` step).
- No need to configure `receive.denyCurrentBranch updateInstead` in the user's clone (which would weaken the gate by auto-applying changes when the working tree is clean).

**Con:**
- Users must remember to `git fetch rig && git merge --ff-only rig/experimental-sfu` after each phase close. Easy to forget. Mitigation: SCALE-UP.md's "Convoy launch protocol" gains a step "fetch + merge from rig before materialising the next phase."
- Wave-to-wave dependencies inside a phase still work fine because polecats clone from `/gt/videocall/.repo.git`, not from `/mnt/llms/videocall`. So polecat N+1 sees polecat N's commit immediately after Refinery merges.
- Cross-phase: P1 polecats see P0 polecats' work in the rig's bare repo, **not** in the user's clone. This is fine as long as the materialiser-script-generated beads reference paths and file states from the rig perspective, not from the user's clone.

## Implementation

- [ ] (Documentation) `SCALE-UP.md` gains a "Convoy launch protocol" subsection mentioning the `git fetch rig` step. *(done in S0)*
- [ ] (Manifest) `convoy-manifest.yaml` documents the merge strategy default near the top.
- [ ] (Default) Add `default_merge: local` field to the manifest schema; have `materialize.py` set this on convoy creation when `gt convoy create` supports a flag (TBD — check `gt convoy create --help`).
- [ ] (Convoy P0 retro) The existing P0 convoy `hq-cv-i4w2x` was launched with the default (merge=mr). Update it via `gt convoy set` (or recreate) to use `merge=local`. Not blocking; the merge queue entry is harmless.

## Rejected alternatives

**Alternative A: `git config receive.denyCurrentBranch updateInstead` on the user's clone.** Pro: Refinery can push without rejection, working tree updates automatically. Con: weakens the manual-approval gate; the user's clone changes without their action. **Rejected** as too automatic.

**Alternative B: Make the rig's upstream a separate bare repo (e.g., `/mnt/llms/videocall.git`) and let the user pull from there.** Pro: clean separation; Refinery can push freely; the user's working clone is unaffected. Con: adds a third clone (user's working clone + rig's bare + new exchange bare); operational complexity. **Rejected** as overkill.

**Alternative C: Refinery pushes to a `pending/experimental-sfu` branch on the user's clone, leaves `experimental-sfu` untouched.** Pro: review happens by `git diff experimental-sfu..pending/experimental-sfu`. Con: requires custom Refinery behaviour; the `rig` remote already does this naturally without code changes. **Rejected** as redundant with Alternative B's logic, but using a different mechanism.

## Status

Accepted 2026-05-15. Applies to all convoys from S0 forward.
