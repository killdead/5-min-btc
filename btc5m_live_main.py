#!/usr/bin/env python3
import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import math
from typing import Optional
from datetime import datetime


def py_bin() -> str:
    conda_py = "/Users/killdead/opt/anaconda3/bin/python3"
    return conda_py if os.path.exists(conda_py) else sys.executable


def wait_for_db_cycle(db_path: str, slug: str, timeout_s: float) -> Optional[str]:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT cycle_id, status FROM strategy_cycles WHERE slug=? ORDER BY created_at DESC LIMIT 1",
                (slug,),
            ).fetchone()
            conn.close()
            if row:
                print(f"[main] cycle detected cycle_id={row['cycle_id']} status={row['status']}", flush=True)
                return str(row["cycle_id"])
        except Exception:
            pass
        time.sleep(1.0)
    return None


def latest_settlement_status(db_path: str, cycle_id: str) -> Optional[sqlite3.Row]:
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT settlement_mode, status, settled_at, note
            FROM strategy_settlements
            WHERE cycle_id=?
            ORDER BY settled_at DESC
            LIMIT 1
            """,
            (cycle_id,),
        ).fetchone()
        conn.close()
        return row
    except Exception:
        return None


def latest_cycle_created_after(db_path: str, min_created_at_epoch: float) -> Optional[sqlite3.Row]:
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT cycle_id, slug, status, created_at
            FROM strategy_cycles
            WHERE strftime('%s', created_at) >= ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (int(min_created_at_epoch) - 5,),
        ).fetchone()
        conn.close()
        return row
    except Exception:
        return None


def latest_cycle_row(db_path: str) -> Optional[sqlite3.Row]:
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT cycle_id, slug, status, created_at
            FROM strategy_cycles
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        conn.close()
        return row
    except Exception:
        return None


def parse_created_at_epoch(value: Optional[str]) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return 0.0


def cycle_row(db_path: str, cycle_id: str) -> Optional[sqlite3.Row]:
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT cycle_id, slug, status, created_at FROM strategy_cycles WHERE cycle_id=?",
            (cycle_id,),
        ).fetchone()
        conn.close()
        return row
    except Exception:
        return None


TERMINAL_SETTLEMENT_STATUSES = (
    "live_redeem_confirmed",
    "live_redeem_disabled",
    "live_redeem_missing_client",
    "live_redeem_missing_creds",
)


def unresolved_live_cycle_count(db_path: str, lookback_minutes: float) -> int:
    """
    Count live cycles that are not terminal yet and still require operational attention.
    """
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM strategy_cycles c
            WHERE c.mode = 'live'
              AND c.slug NOT LIKE 'btc-updown-5m-MOCK%'
              AND strftime('%s', c.created_at) >= strftime('%s', 'now') - ?
              AND (c.end_ts IS NULL OR c.end_ts >= strftime('%s','now') - 60)
              AND c.status NOT LIKE 'skipped_%'
              AND c.status <> 'no_fill'
              AND c.status NOT LIKE 'settled_%'
              AND NOT EXISTS (
                SELECT 1
                FROM strategy_settlements s
                WHERE s.cycle_id = c.cycle_id
                  AND s.settlement_mode = 'live_redeem'
                  AND (s.status LIKE 'settled_%' OR s.status IN ({",".join("?" * len(TERMINAL_SETTLEMENT_STATUSES))}))
              )
            """,
            (int(max(1.0, lookback_minutes) * 60), *TERMINAL_SETTLEMENT_STATUSES),
        ).fetchone()
        conn.close()
        return int(row[0] if row else 0)
    except Exception:
        return 0


def _safe_obj_to_dict(obj):
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        return [{"raw": x} if not isinstance(x, dict) else x for x in obj]
    if hasattr(obj, "json"):
        try:
            j = obj.json()
            if isinstance(j, (dict, list)):
                return j
            if isinstance(j, str):
                s = j.strip()
                if s.startswith("{") or s.startswith("["):
                    return json.loads(s)
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        try:
            return obj.__dict__
        except Exception:
            pass
    return {"raw": str(obj)}


def live_open_order_cycles_estimate() -> Optional[int]:
    """
    Estimate active market cycles from real CLOB open orders.
    Returns None if unavailable.
    """
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON
        from py_clob_client.clob_types import ApiCreds, OpenOrderParams
    except Exception:
        return None
    try:
        host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLY_CHAIN_ID", str(POLYGON)))
        private_key = os.getenv("POLY_PRIVATE_KEY")
        funder = os.getenv("POLY_FUNDER")
        signature_type = os.getenv("POLY_SIGNATURE_TYPE")
        if not private_key:
            return None
        client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=private_key,
            funder=funder,
            signature_type=int(signature_type) if signature_type else None,
        )
        api_key = os.getenv("POLY_API_KEY")
        api_secret = os.getenv("POLY_API_SECRET")
        api_passphrase = os.getenv("POLY_API_PASSPHRASE")
        if api_key and api_secret and api_passphrase:
            client.set_api_creds(ApiCreds(api_key, api_secret, api_passphrase))
        else:
            creds = client.create_or_derive_api_creds()
            client.set_api_creds(ApiCreds(creds.api_key, creds.api_secret, creds.api_passphrase))

        resp = client.get_orders(OpenOrderParams())
        payload = _safe_obj_to_dict(resp)
        items = []
        if isinstance(payload, list):
            items = [x for x in payload if isinstance(x, dict)]
        elif isinstance(payload, dict):
            for k in ("data", "orders", "results"):
                if isinstance(payload.get(k), list):
                    items = [x for x in payload.get(k) if isinstance(x, dict)]
                    break
            if not items and isinstance(payload.get("raw"), str):
                raw = payload.get("raw", "").strip()
                if raw.startswith("[") and raw.endswith("]"):
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        items = [x for x in parsed if isinstance(x, dict)]

        terminal_statuses = {"filled", "matched", "canceled", "cancelled", "expired", "rejected"}
        orders = []
        for d in items:
            obj = d.get("order") if isinstance(d.get("order"), dict) else d
            has_order_id = any(obj.get(k) for k in ("id", "orderID", "orderId", "order_id"))
            if not has_order_id:
                continue
            status = str(obj.get("status") or d.get("status") or "").lower()
            if status and status in terminal_statuses:
                continue
            orders.append(obj)
        n = len(orders)
        return int(math.ceil(n / 2.0))
    except Exception:
        return None


def main() -> None:
    p = argparse.ArgumentParser(description="Single-command live BTC5m pipeline (trade + claim worker)")
    p.add_argument("--db", default="polymarket_collector.db")
    p.add_argument("--slug", help="Specific market slug")
    p.add_argument("--probe-next", action="store_true", help="Let trading bot probe next BTC 5m market")
    p.add_argument("--max-active-cycles", type=int, default=2, help="Max unresolved live cycles to keep in flight")
    p.add_argument("--active-lookback-minutes", type=float, default=30.0, help="Only count cycles created in this recent window for capacity gate")
    p.add_argument("--loop-seconds", type=float, default=5.0, help="Main loop sleep")
    p.add_argument("--target-price", type=float, default=0.49)
    p.add_argument("--size-shares", type=float, default=5.0)
    p.add_argument("--live-poll-seconds", type=float, default=1.0)
    p.add_argument("--no-live-monitor", action="store_true", help="Pass --no-live-monitor to trading bot")
    p.add_argument("--settlement-poll-seconds", type=float, default=30.0)
    p.add_argument("--probe-retry-seconds", type=float, default=15.0, help="Retry cadence when --probe-next finds no tradable market")
    args = p.parse_args()

    if not args.slug and not args.probe_next:
        p.error("Provide --slug or use --probe-next")

    python = py_bin()
    env = os.environ.copy()

    worker_cmd_base = [
        python,
        "btc5m_settlement_worker.py",
        "--db",
        args.db,
        "--mode",
        "live-redeem",
        "--loop",
        "--poll-seconds",
        str(args.settlement_poll_seconds),
    ]
    worker = None

    bot_cmd = [
        python,
        "btc5m_trading_bot.py",
        "--db",
        args.db,
        "--mode",
        "live",
        "--target-price",
        str(args.target_price),
        "--size-shares",
        str(args.size_shares),
        "--live-confirm",
        "--live-poll-seconds",
        str(args.live_poll_seconds),
    ]
    if args.no_live_monitor:
        bot_cmd.append("--no-live-monitor")
    slug = args.slug
    if slug:
        bot_cmd.extend(["--slug", slug])
    else:
        bot_cmd.append("--probe-next")

    try:
        # Always run one settlement worker for backlog + newly traded cycles.
        worker_cmd = worker_cmd_base
        print(f"[main] starting settlement worker: {' '.join(worker_cmd)}", flush=True)
        worker = subprocess.Popen(worker_cmd, env=env)

        # Single explicit slug: one-shot run.
        if slug:
            print(f"[main] starting trading bot: {' '.join(bot_cmd)}", flush=True)
            rc = subprocess.call(bot_cmd, env=env)
            print(f"[main] trading bot exit rc={rc}", flush=True)
            if rc != 0:
                raise SystemExit(rc)
            return

        # Continuous pipeline mode: keep up to N active cycles in flight.
        last_capacity_log = 0.0
        while True:
            clob_n = live_open_order_cycles_estimate()
            db_n = unresolved_live_cycle_count(args.db, args.active_lookback_minutes)
            if clob_n is None:
                active_n = db_n
                active_src = "db_unresolved_fallback"
            else:
                # Prefer real exchange state for capacity gating.
                # DB unresolved cycles are informational and may include stale non-redeemable history.
                active_n = int(clob_n)
                active_src = f"clob_open_orders(clob={int(clob_n)},db={int(db_n)})"
            if active_n >= max(1, int(args.max_active_cycles)):
                now = time.time()
                if now - last_capacity_log >= 10.0:
                    print(
                        "[main]",
                        f"capacity full active_cycles={active_n}/{int(args.max_active_cycles)} src={active_src}; waiting",
                        flush=True,
                    )
                    last_capacity_log = now
                time.sleep(max(1.0, float(args.loop_seconds)))
                continue

            bot_launch_ts = time.time()
            before = latest_cycle_row(args.db)
            before_id = str(before["cycle_id"]) if before else None
            print(
                "[main]",
                f"launch trading bot active_cycles={active_n}/{int(args.max_active_cycles)} src={active_src}",
                flush=True,
            )
            print(f"[main] starting trading bot: {' '.join(bot_cmd)}", flush=True)
            bot_rc = subprocess.call(bot_cmd, env=env)
            print(f"[main] trading bot exit rc={bot_rc}", flush=True)
            if bot_rc != 0:
                time.sleep(max(2.0, float(args.probe_retry_seconds)))
                continue

            row = latest_cycle_row(args.db)
            if row and str(row["cycle_id"]) != (before_id or "") and parse_created_at_epoch(row["created_at"]) >= bot_launch_ts - 2:
                print(
                    "[main] latest probe cycle",
                    f"cycle_id={row['cycle_id']}",
                    f"slug={row['slug']}",
                    f"status={row['status']}",
                    flush=True,
                )
                time.sleep(max(1.0, float(args.loop_seconds)))
            else:
                print(f"[main] no tradable market yet, retrying in {args.probe_retry_seconds}s", flush=True)
                time.sleep(max(1.0, float(args.probe_retry_seconds)))
    finally:
        if worker is not None and worker.poll() is None:
            print("[main] stopping settlement worker", flush=True)
            try:
                worker.send_signal(signal.SIGINT)
                worker.wait(timeout=5)
            except Exception:
                worker.kill()


if __name__ == "__main__":
    main()
