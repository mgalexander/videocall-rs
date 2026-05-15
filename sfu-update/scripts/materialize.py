#!/usr/bin/env python3
"""
materialize.py — idempotently create the SFU refactor beads & convoys.

Reads sfu-update/convoy-manifest.yaml (the source of truth).
Maintains sfu-update/.materialize-state.json mapping manifest keys to
concrete bd ids. Re-runnable; never creates duplicates.

Run inside gastown-sandbox from /mnt/llms/videocall (the rig root):
    docker exec -w /mnt/llms/videocall gastown-sandbox \\
        python3 sfu-update/scripts/materialize.py
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

REPO = Path("/mnt/llms/videocall")
MANIFEST = REPO / "sfu-update" / "convoy-manifest.yaml"
STATE = REPO / "sfu-update" / ".materialize-state.json"

# Match bd's actual create output: "✓ Created issue: vc-abc — Title".
# Anchoring on "Created issue:" avoids false matches in auto-import log noise.
CREATED_ISSUE_RE = re.compile(r"Created issue:\s+([a-z]{1,8}(?:-cv)?-[a-z0-9.]{2,16})")
# Convoys print "Created convoy 🚚 hq-cv-xxxxx" (emoji separator, no colon).
# Be liberal in what we accept after "Created convoy": optional colon,
# optional whitespace, optional emoji, then the id.
CREATED_CONVOY_RE = re.compile(r"Created convoy[:\s]+[^\sa-z0-9]*\s*([a-z]{1,8}-cv-[a-z0-9.]{2,16})")


def run(cmd: list[str], check: bool = True) -> str:
    print(f"  $ {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    if check and proc.returncode != 0:
        raise SystemExit(f"command failed ({proc.returncode}):\n{out}")
    return out.strip()


def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {"epics": {}, "beads": {}, "convoys": {}, "edges": []}


def save_state(state: dict) -> None:
    STATE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def extract_id(out: str, prefix: str, *, is_convoy: bool = False) -> str:
    """Pull a bead/convoy id from bd/gt CLI output anchored on 'Created issue:' / 'Created convoy:'."""
    regex = CREATED_CONVOY_RE if is_convoy else CREATED_ISSUE_RE
    m = regex.search(out)
    if not m:
        raise SystemExit(
            f"could not extract {prefix}-* id from output:\n{out}"
        )
    found = m.group(1)
    # Convoys live under the town hq prefix (e.g. hq-cv-*), not the rig prefix.
    if not is_convoy and not found.startswith(f"{prefix}-"):
        raise SystemExit(f"id {found!r} does not match prefix {prefix!r}; output:\n{out}")
    return found


def create_bead(
    *,
    title: str,
    bead_type: str,
    summary: str,
    parent_id: str | None,
    prefix: str,
) -> str:
    args = [
        "bd",
        "create",
        title,
        "--type",
        bead_type,
        "--description",
        summary,
    ]
    if parent_id:
        args += ["--parent", parent_id]
    out = run(args)
    return extract_id(out, prefix, is_convoy=False)


def add_blocks_dep(blocker_id: str, blocked_id: str) -> None:
    """blocker blocks blocked  ==  bd dep <blocker> --blocks <blocked>"""
    run(["bd", "dep", blocker_id, "--blocks", blocked_id])


def create_convoy(title: str, tracked_ids: list[str], prefix: str) -> str:
    args = ["gt", "convoy", "create", title] + tracked_ids
    out = run(args)
    return extract_id(out, prefix, is_convoy=True)


def main() -> int:
    if not MANIFEST.exists():
        raise SystemExit(f"manifest not found: {MANIFEST}")
    manifest = yaml.safe_load(MANIFEST.read_text())
    prefix = manifest.get("prefix", "vc")

    state = load_state()
    state.setdefault("epics", {})
    state.setdefault("beads", {})
    state.setdefault("convoys", {})
    state.setdefault("edges", [])

    # 1. Epics first (no parent).
    print("== epics ==")
    for epic in manifest.get("epics", []):
        key = epic["key"]
        if key in state["epics"]:
            print(f"  EXISTS  {key}  {state['epics'][key]}")
            continue
        new_id = create_bead(
            title=epic["title"],
            bead_type=epic.get("type", "epic"),
            summary=epic.get("summary", ""),
            parent_id=None,
            prefix=prefix,
        )
        state["epics"][key] = new_id
        save_state(state)
        print(f"  CREATED {key}  {new_id}")

    # 2. Beads.
    print("== beads ==")
    for bead in manifest.get("beads", []):
        key = bead["key"]
        if key in state["beads"]:
            print(f"  EXISTS  {key}  {state['beads'][key]}")
            continue
        parent_epic_key = bead.get("parent_epic")
        parent_id = state["epics"].get(parent_epic_key) if parent_epic_key else None
        new_id = create_bead(
            title=bead["title"],
            bead_type=bead["type"],
            summary=bead.get("summary", ""),
            parent_id=parent_id,
            prefix=prefix,
        )
        state["beads"][key] = new_id
        save_state(state)
        print(f"  CREATED {key}  {new_id}")

    # 3. blocked_by edges.
    print("== edges ==")
    edge_set = {tuple(e) for e in state["edges"]}
    for bead in manifest.get("beads", []):
        blocked_key = bead["key"]
        blocked_id = state["beads"].get(blocked_key)
        if not blocked_id:
            print(f"  SKIP    blocked={blocked_key} has no id yet")
            continue
        for blocker_key in bead.get("blocked_by", []) or []:
            blocker_id = state["beads"].get(blocker_key)
            if not blocker_id:
                print(f"  SKIP    blocker={blocker_key} not yet materialised")
                continue
            edge = (blocker_id, blocked_id)
            if edge in edge_set:
                print(f"  EXISTS  {blocker_key}({blocker_id}) -blocks-> {blocked_key}({blocked_id})")
                continue
            add_blocks_dep(blocker_id, blocked_id)
            edge_set.add(edge)
            state["edges"] = sorted(list(edge_set))
            save_state(state)
            print(f"  ADDED   {blocker_key}({blocker_id}) -blocks-> {blocked_key}({blocked_id})")

    # 4. Convoys, each tracking its declared beads.
    print("== convoys ==")
    for convoy in manifest.get("convoys", []):
        key = convoy["key"]
        if key in state["convoys"]:
            print(f"  EXISTS  {key}  {state['convoys'][key]}")
            continue
        tracked = [state["beads"][k] for k in convoy.get("tracks", []) if k in state["beads"]]
        if not tracked:
            print(f"  SKIP    convoy {key} has no tracked beads in state yet")
            continue
        new_id = create_convoy(convoy["title"], tracked, prefix)
        state["convoys"][key] = new_id
        save_state(state)
        print(f"  CREATED convoy {key}  {new_id}  tracks={len(tracked)} beads")

    # bd's auto-export is timer-based (~1s after export.interval was tuned;
    # default 60s). Without a synchronous flush, state changes written in this
    # script will not appear in .beads/issues.jsonl before the next bd command
    # auto-imports the old JSONL and clobbers them. Force a synchronous export
    # so the JSONL captures everything we just did.
    jsonl = "/gt/videocall/.beads/issues.jsonl"
    print("\n== sync export ==")
    run(["bd", "export", "--all", "-o", jsonl])

    print("\n== summary ==")
    print(f"  epics   : {len(state['epics'])}")
    print(f"  beads   : {len(state['beads'])}")
    print(f"  convoys : {len(state['convoys'])}")
    print(f"  edges   : {len(state['edges'])}")
    print(f"  state   : {STATE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
