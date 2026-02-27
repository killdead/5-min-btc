#!/usr/bin/env python3
import argparse
import sqlite3
from typing import Optional

from btc5m_trading_bot import TradingBotDB


def find_cycle(db: TradingBotDB, cycle_id: Optional[str], slug: Optional[str]) -> sqlite3.Row:
    if cycle_id:
        row = db.conn.execute("SELECT * FROM strategy_cycles WHERE cycle_id=?", (cycle_id,)).fetchone()
        if not row:
            raise SystemExit(f"cycle_id not found: {cycle_id}")
        return row
    if not slug:
        raise SystemExit("Provide --cycle-id or --slug")
    row = db.conn.execute(
        "SELECT * FROM strategy_cycles WHERE slug=? ORDER BY created_at DESC LIMIT 1",
        (slug,),
    ).fetchone()
    if not row:
        raise SystemExit(f"slug not found: {slug}")
    return row


def main() -> None:
    p = argparse.ArgumentParser(description="Record a manual claim/redeem in the strategy DB")
    p.add_argument("--db", default="polymarket_collector.db")
    p.add_argument("--cycle-id")
    p.add_argument("--slug")
    p.add_argument("--amount", type=float, required=True, help="USDC amount claimed manually")
    p.add_argument("--winner", choices=["Up", "Down"], help="Optional winner outcome")
    p.add_argument("--note", default="manual claim recorded from Polymarket UI")
    args = p.parse_args()

    db = TradingBotDB(args.db)
    try:
        cycle = find_cycle(db, args.cycle_id, args.slug)
        cycle_id = str(cycle["cycle_id"])
        slug = str(cycle["slug"])
        fills = db.cycle_fills_by_outcome(cycle_id)
        up_filled = float(fills.get("Up", 0.0))
        down_filled = float(fills.get("Down", 0.0))

        # Capital ledger: treat manual claim as redeem inflow so capital stats unlock correctly.
        db.insert_capital_event(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "event_type": "redeem_estimate",
                "amount_usdc": float(args.amount),
                "note": args.note,
                "raw_json": {"manual": True},
            }
        )
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "manual_claim",
                "market_closed": None,
                "winner_outcome": args.winner,
                "redeem_value_est": float(args.amount),
                "up_filled": up_filled,
                "down_filled": down_filled,
                "status": "settled_manual_claim",
                "note": args.note,
                "raw_json": {"manual": True, "winner": args.winner},
            }
        )
        db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "target_price": float(cycle["target_price"] or 0.49),
                "target_size": float(cycle["target_size"] or 5.0),
                "status": "settled_manual_claim",
                "mode": str(cycle["mode"] or "live"),
                "note": args.note,
                "raw_json": {"manual_claim": True, "amount": float(args.amount), "winner": args.winner},
            }
        )
        print(
            "[manual-claim]",
            f"cycle_id={cycle_id}",
            f"slug={slug}",
            f"amount={args.amount}",
            f"winner={args.winner}",
            "status=settled_manual_claim",
            flush=True,
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
