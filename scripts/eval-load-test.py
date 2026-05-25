#!/usr/bin/env python3
"""Evaluate a videocall bot-orchestrator JSON report against CI thresholds.

Usage:
    cat report.json | scripts/eval-load-test.py \
        --max-loss-pct 0.5 \
        [--require-all-connected | --no-require-all-connected] \
        [--out-json /path/to/verdict.json]

Reads the orchestrator's summary JSON (produced by `bot --orchestrate`) from
stdin and emits a one-line human summary on stderr plus PASS/FAIL on stdout.
Exit code is 0 on pass, 1 on fail. Designed to be the merge-gate / release-gate
verdict step in `.github/workflows/load-test.yaml`.

Loss metric definition
----------------------
We compute:

    loss_pct = listener_totals.drops
               / (listener_totals.packets_received + listener_totals.drops)
               * 100.0

over *listeners only*. Listeners are the subscribing bots actually measuring
receive-side loss; senders' `drops` counter isn't a meaningful proxy because
senders don't receive media. Senders still contribute to the gate via the
`--require-all-connected` check.

Important caveat: the bot's `drops` counter (see bot/src/stats.rs) is
"failed-to-drain inbound unistreams" — every accepted but un-readable stream
increments it. That is the best receive-side loss proxy the bot exposes today,
but it does NOT distinguish audio frames from video frames, and it does NOT
attribute loss to RTP-level drops vs transport-level errors. Treat the
threshold as an aggregate health gate, not a codec-aware loss budget. Follow-up
bead title: "bot: add codec-aware loss tracking (audio vs video)".

Crashed-bot definition
----------------------
A bot is treated as "crashed" if its per-bot snapshot has `connected == false`.
The bot doesn't separately track post-connect disconnects, so a bot that fails
to dial the SFU at all and a bot that crashed mid-run are both folded into the
same bucket. This is the practical interpretation for the gate: any bot that
never reached steady state is a failure.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def _bool_arg(parser: argparse.ArgumentParser, name: str, default: bool, help_text: str) -> None:
    """Add a --foo / --no-foo pair (Python stdlib equivalent of argparse BooleanOptionalAction).

    We avoid BooleanOptionalAction so this script stays compatible with the
    older Python on the GH Actions ubuntu-latest image without pinning a
    setup-python version. (BooleanOptionalAction is 3.9+; ubuntu-latest is
    fine but the explicit pair is clearer in `--help` output anyway.)
    """
    group = parser.add_mutually_exclusive_group()
    group.add_argument(f"--{name}", dest=name.replace("-", "_"), action="store_true", help=help_text)
    group.add_argument(
        f"--no-{name}",
        dest=name.replace("-", "_"),
        action="store_false",
        help=f"disable: {help_text}",
    )
    parser.set_defaults(**{name.replace("-", "_"): default})


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Evaluate videocall bot orchestrator JSON against CI thresholds."
    )
    p.add_argument(
        "--max-loss-pct",
        type=float,
        required=True,
        help="Fail if listener packet-loss rate exceeds this percentage (e.g. 0.5 for 0.5%%).",
    )
    _bool_arg(
        p,
        "require-all-connected",
        default=True,
        help_text="Fail if any bot finished with connected=false (default: on).",
    )
    p.add_argument(
        "--out-json",
        type=str,
        default=None,
        help="Optional path to write a machine-readable verdict JSON.",
    )
    return p.parse_args(argv)


def compute_loss_pct(listener_totals: dict[str, Any]) -> float:
    """Receive-side packet-loss percentage across all listener bots.

    Returns 0.0 if there's no traffic to measure (no listeners or both
    counters are zero). That's safe: an empty run will still trip the
    --require-all-connected check before this number matters.
    """
    received = int(listener_totals.get("packets_received", 0) or 0)
    drops = int(listener_totals.get("drops", 0) or 0)
    denom = received + drops
    if denom == 0:
        return 0.0
    return (drops / denom) * 100.0


def summarize(report: dict[str, Any]) -> dict[str, Any]:
    senders = int(report.get("senders", 0) or 0)
    listeners = int(report.get("listeners", 0) or 0)
    per_bot = report.get("per_bot", []) or []
    listener_totals = report.get("listener_totals", {}) or {}
    sender_totals = report.get("sender_totals", {}) or {}
    totals = report.get("totals", {}) or {}

    total_bots = len(per_bot) if per_bot else senders + listeners
    crashed_bots = [b for b in per_bot if not bool(b.get("connected", False))]
    connected_bots = total_bots - len(crashed_bots)
    loss_pct = compute_loss_pct(listener_totals)

    return {
        "senders": senders,
        "listeners": listeners,
        "total_bots": total_bots,
        "crashed_bots": len(crashed_bots),
        "crashed_user_ids": [b.get("user_id", "?") for b in crashed_bots],
        "connected_bots": connected_bots,
        "loss_pct": loss_pct,
        "totals": totals,
        "sender_totals": sender_totals,
        "listener_totals": listener_totals,
    }


def evaluate(summary: dict[str, Any], max_loss_pct: float, require_all_connected: bool) -> tuple[bool, list[str]]:
    """Return (pass, reasons). Reasons are non-empty iff pass is False."""
    reasons: list[str] = []
    if summary["loss_pct"] > max_loss_pct:
        reasons.append(
            f"loss_pct {summary['loss_pct']:.3f}% exceeds threshold {max_loss_pct:.3f}%"
        )
    if require_all_connected and summary["crashed_bots"] > 0:
        crashed_preview = ",".join(summary["crashed_user_ids"][:5])
        more = "" if summary["crashed_bots"] <= 5 else f" (+{summary['crashed_bots'] - 5} more)"
        reasons.append(
            f"{summary['crashed_bots']}/{summary['total_bots']} bots crashed: {crashed_preview}{more}"
        )
    return (len(reasons) == 0, reasons)


def render_human_line(summary: dict[str, Any]) -> str:
    s_tot = summary["sender_totals"] or {}
    l_tot = summary["listener_totals"] or {}
    return (
        f"loss={summary['loss_pct']:.3f}% "
        f"crashed={summary['crashed_bots']}/{summary['total_bots']} "
        f"connected={summary['connected_bots']}/{summary['total_bots']} | "
        f"senders: pkts={int(s_tot.get('packets_received', 0) or 0)} "
        f"bytes={int(s_tot.get('bytes_received', 0) or 0)} "
        f"drops={int(s_tot.get('drops', 0) or 0)} | "
        f"listeners: pkts={int(l_tot.get('packets_received', 0) or 0)} "
        f"bytes={int(l_tot.get('bytes_received', 0) or 0)} "
        f"drops={int(l_tot.get('drops', 0) or 0)}"
    )


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    raw = sys.stdin.read()
    if not raw.strip():
        print("FAIL: empty input on stdin", file=sys.stdout)
        print("eval-load-test: no JSON received on stdin", file=sys.stderr)
        return 1

    try:
        report = json.loads(raw)
    except json.JSONDecodeError as e:
        print("FAIL: stdin is not valid JSON", file=sys.stdout)
        print(f"eval-load-test: JSON decode error: {e}", file=sys.stderr)
        return 1

    if not isinstance(report, dict):
        print("FAIL: top-level JSON value is not an object", file=sys.stdout)
        return 1

    summary = summarize(report)
    passed, reasons = evaluate(summary, args.max_loss_pct, args.require_all_connected)

    print(render_human_line(summary), file=sys.stderr)

    verdict = {
        "pass": passed,
        "loss_pct": summary["loss_pct"],
        "crashed_bots": summary["crashed_bots"],
        "total_bots": summary["total_bots"],
        "thresholds": {
            "max_loss_pct": args.max_loss_pct,
            "require_all_connected": args.require_all_connected,
        },
        "reasons": reasons,
    }
    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(verdict, f, indent=2, sort_keys=True)
            f.write("\n")

    if passed:
        print("PASS")
        return 0
    print(f"FAIL: {'; '.join(reasons)}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
