#!/usr/bin/env python3
import argparse
import json
import sqlite3
import subprocess
import sys
from typing import Optional, Tuple

from btc5m_settlement_worker import infer_winner_from_market_payload
from fetch_polymarket_market_to_db import fetch_market_by_slug


def get_cycle(conn: sqlite3.Connection, cycle_id: Optional[str], slug: Optional[str]) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    if cycle_id:
        row = conn.execute(
            "SELECT cycle_id, slug, condition_id, status, mode, created_at FROM strategy_cycles WHERE cycle_id=?",
            (cycle_id,),
        ).fetchone()
        if not row:
            raise SystemExit(f"cycle_id not found: {cycle_id}")
        return row
    if slug:
        row = conn.execute(
            "SELECT cycle_id, slug, condition_id, status, mode, created_at FROM strategy_cycles WHERE slug=? ORDER BY created_at DESC LIMIT 1",
            (slug,),
        ).fetchone()
        if not row:
            raise SystemExit(f"slug not found in strategy_cycles: {slug}")
        return row
    raise SystemExit("Provide --cycle-id or --slug")


def get_fills(conn: sqlite3.Connection, cycle_id: str) -> Tuple[float, float]:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN outcome_name='Up' THEN fill_size END),0) AS up_filled,
          COALESCE(SUM(CASE WHEN outcome_name='Down' THEN fill_size END),0) AS down_filled
        FROM strategy_fills
        WHERE cycle_id=?
        """,
        (cycle_id,),
    ).fetchone()
    return float(row["up_filled"] or 0.0), float(row["down_filled"] or 0.0)


def tail(s: str, n: int = 4000) -> str:
    if not s:
        return ""
    return s[-n:]


def main() -> None:
    p = argparse.ArgumentParser(description="Debug Polymarket claim/redeem for a single cycle (uses relayer helper only)")
    p.add_argument("--db", default="polymarket_collector.db")
    p.add_argument("--cycle-id")
    p.add_argument("--slug")
    p.add_argument("--mode", choices=["winner-only", "both"], default="winner-only")
    p.add_argument("--up-shares", type=float, default=None, help="Override up shares")
    p.add_argument("--down-shares", type=float, default=None, help="Override down shares")
    p.add_argument("--route", choices=["ctf", "neg-risk"], default=None, help="Force relayer route")
    p.add_argument("--execute", action="store_true", help="Actually execute claim via relayer")
    p.add_argument("--wait", action="store_true", help="Wait for relayer tx final state")
    p.add_argument("--runner", default="./run_relayer_redeem.sh")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        cycle = get_cycle(conn, args.cycle_id, args.slug)
        cycle_id = str(cycle["cycle_id"])
        slug = str(cycle["slug"])
        condition_id = str(cycle["condition_id"])
        up_filled, down_filled = get_fills(conn, cycle_id)
    finally:
        conn.close()

    market = fetch_market_by_slug(slug)
    winner, closed, info = infer_winner_from_market_payload(market)
    end_date = market.get("endDate") or info.get("endDate")

    up_shares = up_filled
    down_shares = down_filled
    if args.mode == "winner-only":
        if winner == "Up":
            down_shares = 0.0
        elif winner == "Down":
            up_shares = 0.0
    if args.up_shares is not None:
        up_shares = args.up_shares
    if args.down_shares is not None:
        down_shares = args.down_shares

    print(
        "[claim-debug]",
        f"cycle_id={cycle_id}",
        f"slug={slug}",
        f"status={cycle['status']}",
        f"condition_id={condition_id}",
        f"winner={winner}",
        f"closed={closed}",
        f"endDate={end_date}",
        f"up_filled={up_filled}",
        f"down_filled={down_filled}",
        f"mode={args.mode}",
        f"up_shares_arg={up_shares}",
        f"down_shares_arg={down_shares}",
        flush=True,
    )

    cmd = [
        args.runner,
        "--condition-id",
        condition_id,
        "--up-shares",
        str(up_shares),
        "--down-shares",
        str(down_shares),
    ]
    if args.route:
        cmd += ["--route", args.route]
    if args.execute:
        cmd.append("--execute")
    if args.wait:
        cmd.append("--wait")

    print("[claim-debug-cmd]", " ".join(cmd), flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    print(f"[claim-debug-result] rc={proc.returncode}", flush=True)
    if proc.stdout:
        print("[claim-debug-stdout]")
        print(tail(proc.stdout))
    if proc.stderr:
        print("[claim-debug-stderr]")
        print(tail(proc.stderr))

    # Helpful exit for shell automation
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
