# ADR-0006: Refinery Push Contract (C-8)

- **Status:** Accepted (revised 2026-05-16; supersedes the 2026-05-15 revision of this ADR)
- **Date:** 2026-05-16 (original 2026-05-15)
- **Deciders:** overseer (malexander)
- **Related:** [`SCALE-UP.md`](../SCALE-UP.md), [`FANOUT.md`](../FANOUT.md), [`ops-log.md`](../ops-log.md), [`GAP-ANALYSIS.md`](../GAP-ANALYSIS.md) §C-8, bead `vc-c4e.24`.

## Context

`gt rig add videocall file:///mnt/llms/videocall …` makes the user's working clone (`/mnt/llms/videocall/`) the rig's git upstream. When a polecat finishes work and runs `gt done`, the Refinery merges the polecat's branch into `experimental-sfu` inside the rig's bare repo at `/gt/videocall/.repo.git/` and then pushes the merged result back to the upstream (`/mnt/llms/videocall/`).

`/mnt/llms/videocall/` has `receive.denyCurrentBranch=updateInstead` set (verified 2026-05-16 with `git config -l`; configured by `gt rig add` during bootstrap, see ops-log 2026-05-15). With `updateInstead`, git accepts a push to the currently checked-out branch *as long as the working tree and index are clean*; the push fast-forwards the branch ref AND updates the working tree in lockstep. The Refinery's normal `git push <upstream> experimental-sfu` therefore succeeds, and HEAD of `/mnt/llms/videocall/` advances automatically.

This was the actual behaviour observed during P0 Wave 1 (bootstrap) once the first push was attempted with a clean working tree, and again during the `videocall_ops` Track 2 launch on 2026-05-16 when obsidian's `vco-ow8.1` commit landed at HEAD of `/mnt/llms/videocall/` on `experimental-sfu` automatically.

The user's standing rule (see [[feedback-local-only-push]]): *"keeping the work local in worktrees until merged and manually approved to push."* The rule's intent is to gate **pushes that leave the local environment** (i.e. `git push origin` to GitHub). The Refinery's push from the rig's bare repo to the user's local clone is a *local* operation — both endpoints live on the same machine, under the same `file://` mount, and the user can review the result before any GitHub push. The 2026-05-15 revision of this ADR over-read the rule and tried to insert a manual `git fetch rig` step between Refinery and the user's clone. The user's subsequent direction — "let obsidian's work move forward without pause" during the 2026-05-16 Track 2 launch — confirmed that auto-advance is desired, and that the manual review gate lives at `git push origin`, not at the rig-to-clone hop.

## Decision

**The Refinery pushes merged work to the upstream (`/mnt/llms/videocall/`). The manual approval gate is `git push origin` (to GitHub), not `git fetch rig`.**

Concretely:

1. **Refinery behaviour.** When a polecat closes via `gt done` and the Refinery merges its branch into `experimental-sfu` inside the rig's bare repo (`/gt/videocall/.repo.git/`), the Refinery then performs `git push <upstream> experimental-sfu` against the configured `file://` upstream. With `receive.denyCurrentBranch=updateInstead` set on the upstream, this advances both the branch ref and the working tree of `/mnt/llms/videocall/` (provided that working tree is clean — see Operational hygiene below).
2. **Configuration.** Every rig added via `gt rig add` against a non-bare local upstream **must** have `receive.denyCurrentBranch=updateInstead` set on that upstream. `gt rig add` already does this at rig-creation time (verified for both `videocall` and `videocall_ops` rigs). If you ever stand up a rig and the Refinery's push is being rejected with `refusing to update checked out branch`, this is the setting to fix.
3. **Convoy materialisation.** Convoys may use either `--merge=local` or `--merge=mr` (the gt default). The merge-mode choice no longer changes the push contract; either way the Refinery pushes to upstream after the local merge step. Existing convoys keep whatever merge mode they were created with.
4. **GitHub remote push (`origin`) — the actual gate.** Untouched by this ADR and unchanged in spirit. `git push origin experimental-sfu` (or any branch to the GitHub remote) requires explicit per-push approval from the overseer, no exceptions. This is the only push that leaves the local environment and is therefore the only one subject to the manual-approval rule. The Claude Code auto-mode classifier is configured to block `git push origin` without confirmation; that behaviour is correct and load-bearing.
5. **Per-phase review path.** If pre-merge review of a polecat's work is desired before it advances `experimental-sfu` on the user's clone, the review happens against the polecat's branch in the rig's bare repo (`rig-ops/polecat/<name>/<bead>@…` or the equivalent ref), NOT by intercepting the rig-to-clone push. This keeps the convoy daemon unblocked while preserving the user's ability to inspect work in flight.

## Consequences

**Pro:**
- Matches observed system behaviour — no surprise gap between contract and reality.
- The convoy daemon advances without per-phase human gating, which was the user's explicit direction during the 2026-05-16 Track 2 launch.
- The manual-approval rule is concentrated at the one place that matters (the GitHub `origin` push), which is easier to enforce and easier to audit.
- No need for the user to run `git fetch rig && git merge --ff-only rig/experimental-sfu` after every phase. The 2026-05-15 revision's main "Con" (easy to forget) is eliminated.

**Con:**
- The user's local clone's `experimental-sfu` advances without per-push review on their machine. Mitigation: pre-merge review can be done against the polecat's branch in the rig bare repo; post-merge review is `git log experimental-sfu` on the user's clone before any `git push origin`.
- If the user has uncommitted changes in `/mnt/llms/videocall/`, the Refinery's push will be **refused** by `updateInstead` (this is the safety net built into the setting). The push attempt becomes a wedge until the working tree is clean. See Operational hygiene.
- A misbehaving polecat could land bad work on the user's clone before the user notices. Mitigation: Refinery's CI gates are the primary line of defence; the user's `git push origin` review is the secondary one. If a polecat lands bad code on `experimental-sfu`, it stays in `/mnt/llms/videocall/` but never reaches GitHub without explicit approval.

## Operational hygiene

- Keep `/mnt/llms/videocall/` working tree **clean** when convoys are running. Stash or commit local edits before launching a wave. If `updateInstead` rejects a push because the index isn't clean, the symptom is a stalled MR in the Refinery queue.
- If a Refinery push wedges, check `git status` on `/mnt/llms/videocall/` first; the most likely cause is a dirty working tree on the upstream.
- Verify `receive.denyCurrentBranch=updateInstead` on any new rig's upstream after `gt rig add` finishes. One-liner: `git -C <upstream> config receive.denyCurrentBranch` should print `updateInstead`.

## Implementation

- [x] `gt rig add` already configures `receive.denyCurrentBranch=updateInstead` on the upstream at rig-creation time (verified for `videocall` and `videocall_ops` 2026-05-16).
- [x] Refinery's push-to-upstream behaviour matches this ADR (observed during videocall_ops Track 2 launch, 2026-05-16, with obsidian's vco-ow8.1).
- [ ] (Documentation) Update `PLAN.md` "Standing guardrails" to clarify that the local-only-push rule applies to `git push origin` (GitHub), not the rig-to-clone hop. *(done alongside this ADR revision)*
- [ ] (Documentation) Update `PLAN.md` step B7's "Refinery merges into `experimental-sfu` locally; **no push**" line to reflect the auto-push contract. *(done alongside this ADR revision)*
- [ ] (Documentation) `convoy-manifest.yaml`'s `s0-4-refinery-push-adr` summary still describes the old contract. Either rewrite that summary or leave it as a frozen historical record of what the bead's *original* deliverable said; this ADR (the deliverable file itself) is the authoritative current contract. *(left frozen as historical record, since the bead is closed)*

## Rejected alternatives

**Alternative A: "Manual-fetch" model — set `receive.denyCurrentBranch=refuse` on the user's clone; user runs `git fetch rig && git merge --ff-only rig/experimental-sfu` after each phase.** Pro: explicit per-phase review gate on the user's local branch. Con: the user has been happy with auto-merge during multi-phase convoy runs; adds a step that's easy to forget; doesn't actually buy more safety than reviewing before `git push origin`, since both endpoints are local. **Rejected** as ceremony without commensurate benefit. (This was the 2026-05-15 revision's Decision; superseded here.)

**Alternative B: Make the rig's upstream a separate bare repo (e.g., `/mnt/llms/videocall.git`) and let the user pull from there.** Pro: clean separation; Refinery can push freely; the user's working clone is unaffected. Con: adds a third clone (user's working clone + rig's bare + new exchange bare); operational complexity; deviates from the `gt rig add` convention used by every other rig in the town. **Rejected** as over-engineering for a single-user local setup.

**Alternative C: Refinery pushes to a `pending/experimental-sfu` branch on the user's clone, leaves `experimental-sfu` untouched.** Pro: review happens by `git diff experimental-sfu..pending/experimental-sfu`. Con: requires custom Refinery behaviour; the user's actual need is post-merge review before GitHub push, which is satisfied by reading `git log experimental-sfu` directly. **Rejected** as solving the wrong problem.

## Status

Accepted (revised) 2026-05-16. Applies to all convoys from this point forward. The 2026-05-15 revision (which mandated manual `git fetch rig`) is **superseded** but preserved in git history; do not resurrect the manual-fetch contract without a new ADR.
