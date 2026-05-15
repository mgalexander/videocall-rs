# ADR-0007: DAG Source of Truth (C-10)

- **Status:** Accepted
- **Date:** 2026-05-15
- **Deciders:** overseer (malexander)
- **Related:** [`PLAN.md`](../PLAN.md) "Gastown DAG per Phase" section, [`convoy-manifest.yaml`](../convoy-manifest.yaml), [`scripts/materialize.py`](../scripts/materialize.py), [`GAP-ANALYSIS.md`](../GAP-ANALYSIS.md) §C-10.

## Context

The SFU refactor's bead/convoy DAG is currently described in two places:
1. **`PLAN.md` "Gastown DAG per Phase"** — human-readable tables, one section per phase, with bead descriptions, blocking edges, and computed waves.
2. **`convoy-manifest.yaml`** — machine-readable, consumed by `scripts/materialize.py` to invoke `bd create` and `bd dep` against the gastown rig.

Polecats execute from whatever ends up in `bd`, which means `convoy-manifest.yaml` (via `materialize.py`) is what actually drives work dispatch. Humans review against `PLAN.md`. If the two drift — a bead added to YAML but not to the prose, or a `blocked_by` edge changed in prose but not in YAML — the actual work order diverges from what was reviewed.

This was identified in `GAP-ANALYSIS.md` as C-10 ("PLAN.md and convoy-manifest.yaml drift risk") with three options: (a) YAML canonical + generated prose, (b) prose canonical + generated YAML, (c) CI check enforcing structural agreement.

## Decision

**`convoy-manifest.yaml` is the canonical source of truth for the DAG. `PLAN.md`'s "Gastown DAG per Phase" section is *long-form documentation*, not the executable spec.**

Concretely:

1. **What's canonical in YAML.** Every bead, every `blocked_by`/`blocks` edge, every convoy and its `tracks` list, every parent-child link. If it isn't in `convoy-manifest.yaml`, polecats can't see it.
2. **What's canonical in PLAN.md.** The *rationale* for each bead (why does p2-7 exist? what's the design intent?), per-phase scope statements, capacity numbers, ADR cross-references. PLAN.md is what a human reads to understand the *plan*; YAML is what the *system* reads to execute.
3. **Acceptable redundancy.** PLAN.md may list the bead set per phase in tables. That listing is **documentation**, not executable spec. When the YAML changes (bead added/removed/re-edged), update PLAN.md prose **in the same commit**. The reverse is *not* required (you may improve PLAN.md prose without touching YAML), but adding/removing beads without touching YAML is a no-op for the system.
4. **CI gate (light).** A `make plan-lint` (or `cargo run -p plan-lint` once that exists) parses `convoy-manifest.yaml` and asserts every `key:` listed in PLAN.md's bead tables appears in the YAML. Inverse not enforced. This gate runs as part of `cargo fmt --check + cargo clippy` (so part of every phase-close gate). Implementation deferred until the count of beads makes it worthwhile (currently 14 + 6 = 20; the threshold for tooling is ~50).
5. **Process when phases change DAG mid-flight.** The user (or Mayor) edits `convoy-manifest.yaml`, re-runs `materialize.sh` (idempotent), then `gt convoy stage <P_N>` to revalidate. PLAN.md is updated in the same commit for documentation.
6. **Bead description text.** Each bead's `summary:` field in `convoy-manifest.yaml` is what polecats see via `bd show` and `gt prime`. It must be self-contained enough for a polecat to execute the bead **without reading PLAN.md** — because polecats often don't. PLAN.md provides cross-bead context, but bead summaries must work standalone.

## Consequences

**Pro:**
- One source for the system to read; no synchronisation logic.
- YAML is the format that scales — adding beads in YAML is cheap; tooling can validate structure (cycle detection via `bd dep cycles`, type validation via JSON-schema, etc.).
- PLAN.md becomes a stable *narrative* artifact; humans read it for context, not for execution detail.
- Drift becomes obvious quickly: if the YAML has 20 beads and the PLAN.md prose lists 14, the reviewer notices on the next commit.

**Con:**
- PLAN.md can fall behind silently if reviewers don't enforce the "update prose in same commit" rule. Mitigation: PR-review checklist (the user is currently the sole reviewer; informal).
- A polecat that reads PLAN.md but not its bead summary may misunderstand scope. Mitigation: bead summaries must be self-contained (rule #6 above). Witness/Mayor can flag obviously-undescribed beads.
- The `key:` namespace (e.g., `p0-1`, `s-p0-3`, `gap-c8-…`) is human-managed in YAML. Collisions or renames need care. Mitigation: `materialize.py` keys the state file on `key:`, so renaming a key after materialisation creates a new bead and orphans the old one. Don't rename keys post-materialisation; close the old bead and create a new one.

## Implementation

- [ ] (Documentation, this commit) PLAN.md gains a one-line note at the top of "Gastown DAG per Phase": *"Canonical DAG is `convoy-manifest.yaml`. This section is long-form documentation; in case of disagreement, the YAML wins."*
- [ ] (Documentation) `convoy-manifest.yaml` gains a top-level comment: *"Source of truth for the SFU refactor DAG per ADR-0007. PLAN.md derives prose from this."*
- [ ] (Future tooling) `make plan-lint` validator. Track as a P2-or-later bead, not blocking S0.

## Rejected alternatives

**Alternative A: PLAN.md prose is canonical; generate YAML from it.** Pro: human-first authoring. Con: parsing markdown tables reliably is brittle; round-tripping with comments and per-bead context is messy. **Rejected.**

**Alternative B: Both canonical with a CI sync check.** Pro: no priority. Con: double-edit burden; reviewers must touch both files; drift is more frequent than the audit catches. **Rejected.**

**Alternative C: Eliminate the PLAN.md DAG section entirely — let YAML stand alone.** Pro: strictly DRY. Con: PLAN.md becomes harder to read top-to-bottom; reviewers lose phase narrative. **Rejected** — the narrative value is real.

## Status

Accepted 2026-05-15. Effective immediately. The DAG section in PLAN.md is hereby downgraded to "long-form documentation." Discrepancies between PLAN.md and `convoy-manifest.yaml` are resolved in favor of the YAML.
