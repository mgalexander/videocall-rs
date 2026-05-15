# sfu-update Operations Log

Chronological log of bootstrap and convoy-execution events. Append-only.

Format: `YYYY-MM-DD HH:MM TZ  ACTOR  EVENT  DETAIL`

---

## 2026-05-15 Bootstrap

- baseline disk: `/dev/mapper/data-root 908G / 596G used / 266G avail / 70% used` — well under 80% soft alert and 85% halt threshold.
- baseline docker: `Images 107.7GB (73.43GB reclaimable), Build cache 4.77GB, Local volumes 1.738GB`.
- B0 verified `gastown-sandbox` running (up ~2 days hosting `lps-`, `imap-` rigs). Existing bind mounts: labs-pim-server, labs-imap-server, /gt → /mnt/llms/gas-town/town.
- B1 added `/mnt/llms/videocall:/mnt/llms/videocall` bind mount to `/mnt/llms/gas-town/docker-compose.override.yml`; `docker compose up -d gastown` recreated the container cleanly.
- Verified: `docker exec gastown-sandbox ls /mnt/llms/videocall` returns repo tree; `gt --version` returns `gt version dev`; `bd` found at `/usr/local/bin/bd`.
- Created branch `experimental-sfu` from `c01a773 (helm: fix webtransport version URL to use LB service (#827))`.
- Bootstrap commit `183fc53 chore(sfu): bootstrap sfu-update planning tree and convoy manifest` on experimental-sfu.
- Enrolled rig with `gt rig add videocall file:///mnt/llms/videocall --prefix vc --branch experimental-sfu`. Rig at `/gt/videocall/` with mayor/rig and refinery/rig clones on experimental-sfu. routes.jsonl gets `{"prefix":"vc-","path":"videocall"}`. rigs.json adds videocall entry with git_url `file:///mnt/llms/videocall`.
- Ran `bd bootstrap` (rig db needed manual init after `gt rig add`'s prefix-set warning).
- Mayor primed and started; durable handoff mail sent with full PLAN.md body (pinned, permanent, priority 1).
- Materialised: epic `vc-c4e`, 14 beads `vc-c4e.{1..12, 14, 16}` (3 dupes `.13/.15/.17` closed as the regex bug iteration), 22 blocking edges, convoy `hq-cv-upyye` (original) plus staged convoy `hq-cv-i4w2x` (4 waves, 9 dispatchable tasks — 5 decision-type ADRs are non-slingable, by design).
- Launched `hq-cv-i4w2x`. Wave 1 = vc-c4e.1.
- Provisioned 2-polecat pool via `gt polecat pool-init videocall --size 2` → furiosa + nux (Mad Max theme).
- `gt scheduler run` dispatched vc-c4e.1 to furiosa. Session started; daemon started (PID 27401).
- Furiosa working in `/gt/videocall/polecats/furiosa/videocall/` on branch `polecat/furiosa/vc-c4e.1@mp6w5df2` (forked from 183fc53).

## Known issues / one-off corrections during bootstrap

- `gt rig add` printed: `Warning: Could not set issue_prefix on rig database`. Resolved by `bd bootstrap` from `/gt/videocall`.
- `gt rig add` printed: `Warning: Town root is on branch 'experimental-sfu'` — false alarm; gt was reading the cwd's branch, not the actual town root at `/gt`.
- `gt init` from `/mnt/llms/videocall` created mayor/, refinery/, polecats/, witness/, crew/ — orphaned because the rig actually lives under `/gt/videocall/`. Removed; `.git/info/exclude` cleaned of the agent-directory entries.
- materialize.py regex initially matched any `vc-*` substring, picking up auto-import log noise. Rewrote to anchor on `Created issue:` / `Created convoy:`. 3 duplicate beads `vc-c4e.{13,15,17}` were closed via `bd close --reason "duplicate from regex bug"`.
- `bd` reports `Warning: auto-export: git add failed` on every command because `.beads/` is gitignored — non-fatal, expected (bead state stays out of code commits).
- `SetAgentState attempt N failed, retrying ... issue not found` warnings during `gt polecat pool-init` and session start. Non-fatal so far (furiosa is `working`), but worth watching.
- Furiosa's first session ran `gt prime --hook`, saw an empty hook (the scheduler sling produced a wisp but never landed the bead on the polecat's hook), and self-deferred with `gt done --status DEFERRED`. Root cause: identity beads weren't created (the `SetAgentState` warnings during pool-init were not "non-fatal" — they meant the polecat identities never landed).
- Recovery: `gt polecat identity add videocall furiosa` + `gt polecat identity add videocall nux` → created identity beads `vc-videocall-polecat-furiosa` and `vc-videocall-polecat-nux`. Then `gt hook vc-c4e.1 videocall/polecats/furiosa` attached the bead directly. Then `gt session restart videocall/furiosa` cycled furiosa's tmux session. Furiosa primed again, saw the hook, and started actual work (thinking → executing).
- **Lesson for future phases:** after `gt polecat pool-init`, verify identity beads exist via `gt polecat identity list videocall`. If empty, create them by hand BEFORE slinging.

## 2026-05-15 Pre-execution audit

- Pre-execution gap analysis + adversarial security review filed at `sfu-update/GAP-ANALYSIS.md`. Findings: 4 P0-class security issues, 5 P1, 6 P2, 5 P3, 12 consistency items.
- The most consequential findings (must address): S-P0-1 (routing header forgery enables active-speaker hijack), S-P0-2 (new packet types lack origin discipline — enables redirect/spoof attacks), S-P0-3 (no admission cap until P3 leaves a 7–13 day window where 1000-session DoS is possible), S-P0-4 (NATS subject ACLs not audited).
- Recommends a new convoy `S0` (security pre-flight) materialised in parallel with P0; the quick-wins section lists five items each ≤1 hour that would substantially harden the experiment before P1 opens.
- Scheduler remains paused. No new beads materialised from this audit yet — that step waits for overseer review.
