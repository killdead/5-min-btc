#!/usr/bin/env python3
import argparse
import json
import os
import random
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

from fetch_polymarket_market_to_db import (
    ensure_schema,
    fetch_market_by_slug,
    fetch_orderbook,
    http_get_json,
    parse_json_maybe,
    to_float,
    utc_now_iso,
)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams, OpenOrderParams, OrderArgs, OrderType
from py_clob_client.constants import POLYGON


GAMMA_BASE = "https://gamma-api.polymarket.com"


def parse_iso_ts(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return None


def round_to_tick_down(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    p = Decimal(str(price))
    t = Decimal(str(tick))
    units = (p / t).quantize(Decimal("1"), rounding=ROUND_DOWN)
    return float(units * t)


def choose_initial_maker_buy_price(
    *,
    max_price: float,
    best_bid: Optional[float],
    best_ask: Optional[float],
    tick_size: float,
) -> Tuple[Optional[float], str]:
    """
    Autonomous initial BUY maker price:
    join current best bid (capped), not the max price.
    """
    if tick_size <= 0:
        tick_size = 0.01

    if best_bid is None and best_ask is None:
        return round_to_tick_down(max_price - tick_size, tick_size), "fallback_no_book"

    if best_ask is None:
        if best_bid is None:
            return round_to_tick_down(max_price - tick_size, tick_size), "fallback_no_bid_no_ask"
        px = min(max_price, best_bid)
        px = round_to_tick_down(px, tick_size)
        if best_bid > max_price:
            return px, "capped_at_target_no_ask"
        return px, "join_best_bid_no_ask"

    if best_bid is None:
        # No bid visible: shade below ask and cap by max.
        px = min(max_price, best_ask - tick_size)
        px = round_to_tick_down(px, tick_size)
        return (px if px > 0 else None), "no_bid_place_below_ask"

    desired = min(max_price, best_bid)
    # Must remain strictly below ask to stay maker.
    if desired >= best_ask:
        desired = best_ask - tick_size
    desired = round_to_tick_down(desired, tick_size)
    if desired <= 0:
        return None, "skip_no_valid_maker_price"
    if desired < min(max_price, best_bid):
        if best_bid > max_price:
            return desired, "capped_at_target"
        return desired, "below_ask_to_stay_maker"
    if best_bid > max_price:
        return desired, "capped_at_target"
    return desired, "join_best_bid"


def choose_reprice_maker_buy(
    *,
    current_price: Optional[float],
    max_price: float,
    best_bid: Optional[float],
    best_ask: Optional[float],
    tick_size: float,
) -> Tuple[Optional[float], str]:
    """
    Reprice upward when best bid improves, capped by max_price, staying maker.
    """
    if current_price is None:
        return choose_initial_maker_buy_price(
            max_price=max_price, best_bid=best_bid, best_ask=best_ask, tick_size=tick_size
        )
    if best_bid is None:
        return current_price, "hold_no_bid"

    desired = min(max_price, best_bid)
    if best_ask is not None and desired >= best_ask:
        desired = best_ask - tick_size
    desired = round_to_tick_down(desired, tick_size)
    if desired <= 0:
        return current_price, "hold_invalid_reprice"
    if desired > current_price:
        if best_bid > max_price:
            return desired, "reprice_up_capped_at_target"
        return desired, "reprice_up_join_best_bid"
    return current_price, "hold_no_change"


def discover_btc5m_by_slug_probe(
    now_ts: float,
    *,
    lead_seconds: float = 300.0,
    lookback_seconds: int = 900,
    lookahead_seconds: int = 1800,
) -> List[str]:
    # Deterministic slug probe centered near intended trigger window (now + lead).
    target_ts = int(now_ts + max(0.0, float(lead_seconds)))
    first = target_ts - (target_ts % 300)
    out = []
    for ts in range(first - int(lookback_seconds), first + int(lookahead_seconds) + 1, 300):
        slug = f"btc-updown-5m-{ts}"
        try:
            m = http_get_json(f"{GAMMA_BASE}/markets/slug/{slug}")
        except Exception:
            continue
        if isinstance(m, dict) and m.get("slug") == slug and not bool(m.get("closed")):
            out.append(slug)
    return sorted(set(out))


def btc5m_slug_ts(slug: str) -> Optional[int]:
    try:
        if not slug.startswith("btc-updown-5m-"):
            return None
        return int(slug.rsplit("-", 1)[1])
    except Exception:
        return None


def reorder_slugs_from_reference(slugs: List[str], reference_ts: Optional[int]) -> List[str]:
    if not slugs:
        return slugs
    pairs = [(s, btc5m_slug_ts(s)) for s in slugs]
    # keep unknowns last deterministically
    known = [(s, ts) for s, ts in pairs if ts is not None]
    unknown = [s for s, ts in pairs if ts is None]
    known.sort(key=lambda x: x[1])
    if reference_ts is None:
        return [s for s, _ in known] + unknown
    newer = [s for s, ts in known if ts >= reference_ts]
    older = [s for s, ts in known if ts < reference_ts]
    return newer + older + unknown


@dataclass
class MarketContext:
    slug: str
    condition_id: str
    question: str
    event_start_ts: float
    end_ts: float
    up_token_id: str
    down_token_id: str
    tick_size: float
    order_min_size: float


class TradingBotDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        ensure_schema(self.conn)
        self._ensure_bot_schema()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    def _ensure_bot_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS strategy_cycles (
                cycle_id TEXT PRIMARY KEY,
                slug TEXT NOT NULL,
                condition_id TEXT,
                event_start_ts REAL,
                end_ts REAL,
                trigger_time_ts REAL,
                target_price REAL NOT NULL,
                target_size REAL NOT NULL,
                status TEXT NOT NULL,
                mode TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                note TEXT,
                raw_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_cycles_slug ON strategy_cycles(slug);

            CREATE TABLE IF NOT EXISTS strategy_orders (
                local_order_id TEXT PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                condition_id TEXT,
                outcome_name TEXT NOT NULL,
                token_id TEXT NOT NULL,
                side TEXT NOT NULL,
                target_price REAL NOT NULL,
                chosen_price REAL,
                size REAL NOT NULL,
                post_only INTEGER NOT NULL,
                tif TEXT,
                status TEXT NOT NULL,
                placement_reason TEXT,
                book_best_bid REAL,
                book_best_ask REAL,
                book_bid_sz_l1 REAL,
                book_ask_sz_l1 REAL,
                tick_size REAL,
                placed_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                exchange_order_id TEXT,
                raw_json TEXT,
                FOREIGN KEY (cycle_id) REFERENCES strategy_cycles(cycle_id)
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_orders_cycle ON strategy_orders(cycle_id);

            CREATE TABLE IF NOT EXISTS strategy_fills (
                fill_id TEXT PRIMARY KEY,
                local_order_id TEXT NOT NULL,
                cycle_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                outcome_name TEXT NOT NULL,
                side TEXT NOT NULL,
                fill_price REAL NOT NULL,
                fill_size REAL NOT NULL,
                maker INTEGER,
                fee_or_rebate REAL,
                fill_time TEXT NOT NULL,
                source TEXT NOT NULL,
                raw_json TEXT,
                FOREIGN KEY (local_order_id) REFERENCES strategy_orders(local_order_id),
                FOREIGN KEY (cycle_id) REFERENCES strategy_cycles(cycle_id)
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_fills_cycle ON strategy_fills(cycle_id);

            CREATE TABLE IF NOT EXISTS strategy_order_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                local_order_id TEXT NOT NULL,
                cycle_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                event_time TEXT NOT NULL,
                old_price REAL,
                new_price REAL,
                best_bid REAL,
                best_ask REAL,
                reason TEXT,
                raw_json TEXT,
                FOREIGN KEY (local_order_id) REFERENCES strategy_orders(local_order_id),
                FOREIGN KEY (cycle_id) REFERENCES strategy_cycles(cycle_id)
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_order_updates_cycle ON strategy_order_updates(cycle_id);

            CREATE TABLE IF NOT EXISTS strategy_capital_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                event_time TEXT NOT NULL,
                event_type TEXT NOT NULL,
                outcome_name TEXT,
                local_order_id TEXT,
                amount_usdc REAL NOT NULL,
                note TEXT,
                raw_json TEXT,
                FOREIGN KEY (cycle_id) REFERENCES strategy_cycles(cycle_id)
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_capital_ledger_cycle ON strategy_capital_ledger(cycle_id);

            CREATE TABLE IF NOT EXISTS strategy_settlements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                condition_id TEXT,
                settled_at TEXT NOT NULL,
                settlement_mode TEXT NOT NULL,
                market_closed INTEGER,
                winner_outcome TEXT,
                redeem_value_est REAL,
                up_filled REAL,
                down_filled REAL,
                status TEXT NOT NULL,
                note TEXT,
                raw_json TEXT,
                UNIQUE(cycle_id, settlement_mode),
                FOREIGN KEY (cycle_id) REFERENCES strategy_cycles(cycle_id)
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_settlements_slug ON strategy_settlements(slug);

            CREATE TABLE IF NOT EXISTS bot_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                note TEXT
            );
            """
        )
        self.conn.commit()

    def start_run(self, mode: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO bot_runs (started_at, mode, status) VALUES (?, ?, ?)",
            (utc_now_iso(), mode, "running"),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, note: str = "") -> None:
        self.conn.execute(
            "UPDATE bot_runs SET finished_at=?, status=?, note=? WHERE id=?",
            (utc_now_iso(), status, note, run_id),
        )
        self.conn.commit()

    def upsert_cycle(self, row: Dict[str, Any]) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO strategy_cycles (
                cycle_id, slug, condition_id, event_start_ts, end_ts, trigger_time_ts,
                target_price, target_size, status, mode, created_at, updated_at, note, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cycle_id) DO UPDATE SET
                status=excluded.status,
                updated_at=excluded.updated_at,
                note=excluded.note,
                raw_json=excluded.raw_json
            """,
            (
                row["cycle_id"],
                row["slug"],
                row.get("condition_id"),
                row.get("event_start_ts"),
                row.get("end_ts"),
                row.get("trigger_time_ts"),
                row["target_price"],
                row["target_size"],
                row["status"],
                row["mode"],
                row.get("created_at", now),
                now,
                row.get("note"),
                json.dumps(row.get("raw_json", {}), ensure_ascii=True),
            ),
        )
        self.conn.commit()

    def insert_order(self, row: Dict[str, Any]) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """
            INSERT OR REPLACE INTO strategy_orders (
                local_order_id, cycle_id, slug, condition_id, outcome_name, token_id, side,
                target_price, chosen_price, size, post_only, tif, status, placement_reason,
                book_best_bid, book_best_ask, book_bid_sz_l1, book_ask_sz_l1, tick_size,
                placed_at, updated_at, exchange_order_id, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["local_order_id"],
                row["cycle_id"],
                row["slug"],
                row.get("condition_id"),
                row["outcome_name"],
                row["token_id"],
                row["side"],
                row["target_price"],
                row.get("chosen_price"),
                row["size"],
                1 if row.get("post_only", True) else 0,
                row.get("tif"),
                row["status"],
                row.get("placement_reason"),
                row.get("book_best_bid"),
                row.get("book_best_ask"),
                row.get("book_bid_sz_l1"),
                row.get("book_ask_sz_l1"),
                row.get("tick_size"),
                row.get("placed_at", now),
                now,
                row.get("exchange_order_id"),
                json.dumps(row.get("raw_json", {}), ensure_ascii=True),
            ),
        )
        self.conn.commit()

    def update_order_status(self, local_order_id: str, status: str, note_json: Optional[Dict[str, Any]] = None) -> None:
        self.conn.execute(
            "UPDATE strategy_orders SET status=?, updated_at=?, raw_json=COALESCE(?, raw_json) WHERE local_order_id=?",
            (status, utc_now_iso(), json.dumps(note_json, ensure_ascii=True) if note_json else None, local_order_id),
        )
        self.conn.commit()

    def update_order_live_submission(self, local_order_id: str, status: str, exchange_order_id: Optional[str], note_json: Optional[Dict[str, Any]] = None) -> None:
        self.conn.execute(
            "UPDATE strategy_orders SET status=?, exchange_order_id=?, updated_at=?, raw_json=COALESCE(?, raw_json) WHERE local_order_id=?",
            (
                status,
                exchange_order_id,
                utc_now_iso(),
                json.dumps(note_json, ensure_ascii=True) if note_json else None,
                local_order_id,
            ),
        )
        self.conn.commit()

    def insert_fill(self, row: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO strategy_fills (
                fill_id, local_order_id, cycle_id, slug, outcome_name, side,
                fill_price, fill_size, maker, fee_or_rebate, fill_time, source, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["fill_id"],
                row["local_order_id"],
                row["cycle_id"],
                row["slug"],
                row["outcome_name"],
                row["side"],
                row["fill_price"],
                row["fill_size"],
                1 if row.get("maker") else 0 if row.get("maker") is not None else None,
                row.get("fee_or_rebate"),
                row["fill_time"],
                row["source"],
                json.dumps(row.get("raw_json", {}), ensure_ascii=True),
            ),
        )
        self.conn.commit()

    def insert_order_update(self, row: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO strategy_order_updates (
                local_order_id, cycle_id, slug, event_time, old_price, new_price,
                best_bid, best_ask, reason, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["local_order_id"],
                row["cycle_id"],
                row["slug"],
                row.get("event_time", utc_now_iso()),
                row.get("old_price"),
                row.get("new_price"),
                row.get("best_bid"),
                row.get("best_ask"),
                row.get("reason"),
                json.dumps(row.get("raw_json", {}), ensure_ascii=True),
            ),
        )
        self.conn.commit()

    def insert_capital_event(self, row: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO strategy_capital_ledger (
                cycle_id, slug, event_time, event_type, outcome_name, local_order_id,
                amount_usdc, note, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["cycle_id"],
                row["slug"],
                row.get("event_time", utc_now_iso()),
                row["event_type"],
                row.get("outcome_name"),
                row.get("local_order_id"),
                float(row["amount_usdc"]),
                row.get("note"),
                json.dumps(row.get("raw_json", {}), ensure_ascii=True),
            ),
        )
        self.conn.commit()

    def insert_settlement(self, row: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO strategy_settlements (
                cycle_id, slug, condition_id, settled_at, settlement_mode, market_closed,
                winner_outcome, redeem_value_est, up_filled, down_filled, status, note, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["cycle_id"],
                row["slug"],
                row.get("condition_id"),
                row.get("settled_at", utc_now_iso()),
                row["settlement_mode"],
                row.get("market_closed"),
                row.get("winner_outcome"),
                row.get("redeem_value_est"),
                row.get("up_filled"),
                row.get("down_filled"),
                row["status"],
                row.get("note"),
                json.dumps(row.get("raw_json", {}), ensure_ascii=True),
            ),
        )
        self.conn.commit()

    def cycle_order_fill_summary(self, cycle_id: str) -> sqlite3.Row:
        return self.conn.execute(
            """
            WITH fills AS (
              SELECT o.outcome_name, SUM(f.fill_size) AS filled
              FROM strategy_orders o
              LEFT JOIN strategy_fills f ON f.local_order_id = o.local_order_id
              WHERE o.cycle_id = ?
              GROUP BY o.outcome_name
            )
            SELECT
              (SELECT status FROM strategy_cycles WHERE cycle_id = ?) AS cycle_status,
              COALESCE((SELECT filled FROM fills WHERE outcome_name='Up'), 0) AS up_filled,
              COALESCE((SELECT filled FROM fills WHERE outcome_name='Down'), 0) AS down_filled
            """,
            (cycle_id, cycle_id),
        ).fetchone()

    def cycle_orders(self, cycle_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM strategy_orders WHERE cycle_id = ? ORDER BY outcome_name",
            (cycle_id,),
        ).fetchall()

    def cycle_fills_by_outcome(self, cycle_id: str) -> Dict[str, float]:
        rows = self.conn.execute(
            "SELECT outcome_name, COALESCE(SUM(fill_size),0) AS qty FROM strategy_fills WHERE cycle_id=? GROUP BY outcome_name",
            (cycle_id,),
        ).fetchall()
        return {str(r["outcome_name"]): float(r["qty"] or 0.0) for r in rows}

    def cycle_capital_summary(self, cycle_id: str) -> sqlite3.Row:
        return self.conn.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN event_type='reserve' THEN amount_usdc END),0) AS reserve,
              COALESCE(SUM(CASE WHEN event_type='reserve_release' THEN amount_usdc END),0) AS reserve_release,
              COALESCE(SUM(CASE WHEN event_type='fill_cost' THEN amount_usdc END),0) AS fill_cost,
              COALESCE(SUM(CASE WHEN event_type='redeem_estimate' THEN amount_usdc END),0) AS redeem_estimate
            FROM strategy_capital_ledger
            WHERE cycle_id = ?
            """,
            (cycle_id,),
        ).fetchone()

    def has_nonterminal_cycle_for_slug(self, slug: str) -> Optional[sqlite3.Row]:
        """
        Returns latest non-terminal cycle for slug if present.
        We block duplicate trading on the same market while a previous cycle is still unresolved.
        """
        terminal_prefixes = ("settled", "skipped_")
        terminal_exact = ("no_fill",)
        rows = self.conn.execute(
            """
            SELECT cycle_id, slug, status, created_at, updated_at
            FROM strategy_cycles
            WHERE slug = ?
            ORDER BY created_at DESC
            """,
            (slug,),
        ).fetchall()
        for r in rows:
            status = str(r["status"] or "")
            if status in terminal_exact or any(status.startswith(p) for p in terminal_prefixes):
                continue
            return r
        return None


class BTC5MTradingBot:
    def __init__(
        self,
        db_path: str,
        mode: str,
        target_price: float,
        size_shares: float,
        lead_seconds: float,
        mock_fill: str,
        offline_mock: bool,
        live_confirm: bool = False,
    ):
        self.db = TradingBotDB(db_path)
        self.mode = mode
        self.target_price = target_price
        self.size_shares = size_shares
        self.lead_seconds = lead_seconds
        self.mock_fill = mock_fill
        self.offline_mock = offline_mock
        self.live_confirm = live_confirm
        self.run_id = self.db.start_run(mode)
        self.paper_poll_seconds = 1.0
        self.live_client: Optional[ClobClient] = None
        self.live_poll_seconds: float = 2.0
        self.live_max_seconds: float = 0.0
        self.live_monitor_enabled: bool = True
        self.live_block_if_open_orders: bool = True

    def _is_market_ready_for_strategy(
        self,
        ctx: "MarketContext",
        chosen: Dict[str, Dict[str, Any]],
        now_ts: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """
        Deterministic PASS/FAIL filter for 'next market to trade'.
        Goal: skip markets that exist in Gamma but are not actually tradable yet
        (missing/degenerate CLOB books), and only trade within the intended time window.
        """
        now_ts = time.time() if now_ts is None else now_ts

        # Operate from T-lead to market end. If too early, don't pick it yet.
        trigger_ts = ctx.event_start_ts - float(self.lead_seconds)
        if now_ts < trigger_ts:
            return False, "too_early_for_lead_window"
        # Strategy requirement: place orders pre-open (before price-to-beat window starts).
        # Do not enter once the 5m market has already started.
        if now_ts >= ctx.event_start_ts:
            return False, "market_already_started"
        if now_ts > ctx.end_ts:
            return False, "market_already_ended"

        pair_bid_sum = 0.0
        pair_ask_sum = 0.0
        # Strategy wants both sides trading around ~0.49/0.49 (pre-open volatility near 50%).
        # Use hard PASS/FAIL bounds, not a score.
        min_side_bid = max(0.0, float(self.target_price) - 0.09)   # 0.40 with target=0.49
        max_side_bid = min(1.0, float(self.target_price) + 0.11)   # 0.60
        min_side_ask = max(0.0, float(self.target_price) - 0.09)   # 0.40
        max_side_ask = min(1.0, float(self.target_price) + 0.16)   # 0.65
        min_chosen_px = max(0.0, float(self.target_price) - 0.09)  # avoid nonsense 0.08 bids
        for outcome in ("Up", "Down"):
            c = chosen.get(outcome, {})
            bb = c.get("best_bid")
            ba = c.get("best_ask")
            px = c.get("chosen_price")
            if bb is None or ba is None:
                return False, f"{outcome.lower()}_missing_bbo"
            if px is None:
                return False, f"{outcome.lower()}_no_valid_maker_price"
            if float(px) < min_chosen_px:
                return False, f"{outcome.lower()}_chosen_price_too_low"
            if ba <= bb:
                return False, f"{outcome.lower()}_crossed_or_invalid_book"
            # Degenerate books (common in not-yet-live markets)
            if bb >= 0.95 or ba <= 0.05:
                return False, f"{outcome.lower()}_degenerate_extreme"
            if ba - bb > 0.20:
                return False, f"{outcome.lower()}_spread_too_wide"
            if not (min_side_bid <= float(bb) <= max_side_bid):
                return False, f"{outcome.lower()}_bid_not_near_50"
            if not (min_side_ask <= float(ba) <= max_side_ask):
                return False, f"{outcome.lower()}_ask_not_near_50"
            pair_bid_sum += float(bb)
            pair_ask_sum += float(ba)

        # Pair consistency sanity checks around binary complement pricing.
        if not (0.92 <= pair_bid_sum <= 1.05):
            return False, "pair_bid_sum_out_of_range"
        if not (0.96 <= pair_ask_sum <= 1.10):
            return False, "pair_ask_sum_out_of_range"
        return True, "ready"

    def close(self, status: str, note: str = "") -> None:
        self.db.finish_run(self.run_id, status, note)
        self.db.close()

    def _init_live_client(self) -> ClobClient:
        if self.live_client is not None:
            return self.live_client
        host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLY_CHAIN_ID", str(POLYGON)))
        private_key = os.getenv("POLY_PRIVATE_KEY")
        if not private_key:
            raise RuntimeError("Missing POLY_PRIVATE_KEY env var for live mode")
        funder = os.getenv("POLY_FUNDER")
        signature_type = os.getenv("POLY_SIGNATURE_TYPE")
        api_key = os.getenv("POLY_API_KEY")
        api_secret = os.getenv("POLY_API_SECRET")
        api_passphrase = os.getenv("POLY_API_PASSPHRASE")

        client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=private_key,
            funder=funder,
            signature_type=int(signature_type) if signature_type else None,
        )
        if api_key and api_secret and api_passphrase:
            client.set_api_creds(ApiCreds(api_key, api_secret, api_passphrase))
        else:
            creds = client.create_or_derive_api_creds()
            client.set_api_creds(creds)
            print("[live] derived API creds from signer", flush=True)
        self.live_client = client
        return client

    def _get_live_collateral_balance_allowance(self) -> Optional[Dict[str, Any]]:
        """
        Best-effort collateral balance/allowance fetch from CLOB API.
        This is available capital info, not full net worth (positions MTM not included).
        """
        try:
            client = self._init_live_client()
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            resp = client.get_balance_allowance(params)
            payload = resp.json() if hasattr(resp, "json") else resp
            return payload if isinstance(payload, dict) else {"raw": payload}
        except Exception as exc:
            return {"error": str(exc)}

    def _db_capital_totals(self) -> Dict[str, float]:
        row = self.db.conn.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN event_type='reserve' THEN amount_usdc END),0) AS reserve_amt,
              COALESCE(SUM(CASE WHEN event_type='fill_cost' THEN amount_usdc END),0) AS fill_cost_amt,
              COALESCE(SUM(CASE WHEN event_type='reserve_release' THEN amount_usdc END),0) AS reserve_release_amt,
              COALESCE(SUM(CASE WHEN event_type='redeem_estimate' THEN amount_usdc END),0) AS redeem_est_amt
            FROM strategy_capital_ledger
            """
        ).fetchone()
        reserve_amt = float(row["reserve_amt"] or 0.0)
        fill_cost_amt = float(row["fill_cost_amt"] or 0.0)
        reserve_release_amt = float(row["reserve_release_amt"] or 0.0)
        redeem_est_amt = float(row["redeem_est_amt"] or 0.0)
        # Approx locked = fills not yet redeemed (paper estimate); reserve is temporary and may be stale before fills/cancels.
        est_locked_positions = max(0.0, fill_cost_amt - redeem_est_amt)
        return {
            "reserve_amt": reserve_amt,
            "fill_cost_amt": fill_cost_amt,
            "reserve_release_amt": reserve_release_amt,
            "redeem_est_amt": redeem_est_amt,
            "est_locked_positions": est_locked_positions,
        }

    def _get_live_open_orders(self, client: ClobClient) -> List[Dict[str, Any]]:
        """
        Fetch current open orders from CLOB (best effort, schema-agnostic parsing).
        """
        try:
            resp = client.get_orders(OpenOrderParams())
        except Exception as exc:
            raise RuntimeError(f"get_orders failed: {exc}")
        payload = self._safe_obj_to_dict(resp)
        # Some SDK versions can return a wrapper like {"raw": "[]"} from our generic parser.
        if isinstance(payload, dict) and isinstance(payload.get("raw"), str):
            raw_str = payload.get("raw", "").strip()
            if raw_str.startswith("[") and raw_str.endswith("]"):
                try:
                    parsed = json.loads(raw_str)
                    payload = parsed
                except Exception:
                    pass
        items: List[Any]
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                items = data
            else:
                # If this is not an order object, treat as empty rather than false-positive "1 open order".
                if any(k in payload for k in ("id", "orderID", "asset_id", "market", "status")):
                    items = [payload]
                else:
                    items = []
        else:
            items = []
        out: List[Dict[str, Any]] = []
        terminal_statuses = {"filled", "matched", "canceled", "cancelled", "expired", "rejected"}
        for x in items:
            d = self._safe_obj_to_dict(x)
            if not isinstance(d, dict):
                continue
            # Drop wrappers / parser artifacts (e.g. {"raw":"[]"})
            if set(d.keys()) == {"raw"}:
                continue
            # Only count records that actually look like orders.
            has_order_id = any(
                d.get(k) for k in ("id", "orderID", "orderId", "order_id")
            )
            if not has_order_id:
                continue
            status = str(d.get("status") or "").lower()
            if status and status in terminal_statuses:
                continue
            out.append(d)
        return out

    @staticmethod
    def _safe_obj_to_dict(x: Any) -> Any:
        if x is None:
            return None
        if isinstance(x, list):
            return x
        if isinstance(x, dict):
            return x
        try:
            if hasattr(x, "json"):
                j = x.json()
                if isinstance(j, str):
                    try:
                        return json.loads(j)
                    except Exception:
                        return {"raw": j}
                return j
        except Exception:
            pass
        try:
            d = x.__dict__
            if isinstance(d, dict):
                return d
        except Exception:
            pass
        return {"raw": str(x)}

    @staticmethod
    def _extract_order_state(payload: Any) -> Dict[str, Any]:
        d = BTC5MTradingBot._safe_obj_to_dict(payload)
        if not isinstance(d, dict):
            return {"raw": d}
        obj = d.get("order") if isinstance(d.get("order"), dict) else d
        status = obj.get("status") or d.get("status")
        size = obj.get("size") or obj.get("original_size") or d.get("size")
        matched = (
            obj.get("size_matched")
            or obj.get("matched_size")
            or obj.get("filledSize")
            or obj.get("filled_size")
            or d.get("size_matched")
            or d.get("matched_size")
            or d.get("filled_size")
        )
        price = obj.get("price") or d.get("price")
        avg_price = (
            obj.get("avg_price")
            or obj.get("average_price")
            or obj.get("avgPrice")
            or d.get("avg_price")
            or d.get("average_price")
            or d.get("avgPrice")
        )
        return {
            "status": status,
            "size": to_float(size),
            "matched_size": to_float(matched) or 0.0,
            "price": to_float(price),
            "avg_price": to_float(avg_price),
            "raw": d,
        }

    def _build_market_context(self, slug: str) -> MarketContext:
        if self.offline_mock:
            now = time.time()
            start_ts = now + 300
            end_ts = start_ts + 300
            return MarketContext(
                slug=slug,
                condition_id=f"mock-{slug}",
                question="MOCK BTC 5m market",
                event_start_ts=start_ts,
                end_ts=end_ts,
                up_token_id=f"{slug}-UP",
                down_token_id=f"{slug}-DOWN",
                tick_size=0.01,
                order_min_size=5.0,
            )
        m = fetch_market_by_slug(slug)
        ev0 = (m.get("events") or [{}])[0] if m.get("events") else {}
        event_start_ts = parse_iso_ts(m.get("eventStartTime") or ev0.get("startTime"))
        end_ts = parse_iso_ts(m.get("endDate") or ev0.get("endDate"))
        token_ids = [str(x) for x in parse_json_maybe(m.get("clobTokenIds"), [])]
        outcomes = [str(x) for x in parse_json_maybe(m.get("outcomes"), [])]
        mapping = {outcomes[i].lower(): token_ids[i] for i in range(min(len(outcomes), len(token_ids)))}
        if not event_start_ts or not end_ts:
            raise RuntimeError(f"Market {slug} missing eventStartTime/endDate")
        if "up" not in mapping or "down" not in mapping:
            raise RuntimeError(f"Market {slug} missing Up/Down token IDs")

        # Pull one book to get tick/min-size. Fallback defaults if unavailable.
        tick_size = 0.01
        order_min_size = 5.0
        try:
            b = fetch_orderbook(mapping["up"])
            tick_size = to_float(b.get("tick_size")) or tick_size
            order_min_size = to_float(b.get("min_order_size")) or order_min_size
        except Exception:
            pass

        return MarketContext(
            slug=slug,
            condition_id=str(m.get("conditionId") or ""),
            question=str(m.get("question") or ""),
            event_start_ts=event_start_ts,
            end_ts=end_ts,
            up_token_id=mapping["up"],
            down_token_id=mapping["down"],
            tick_size=tick_size,
            order_min_size=order_min_size,
        )

    def _choose_prices_from_books(self, ctx: MarketContext) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        # 1) Read books first for both outcomes.
        for outcome, token_id in (("Up", ctx.up_token_id), ("Down", ctx.down_token_id)):
            if self.offline_mock:
                # Mock books around 0.50 to test price selection logic.
                if outcome == "Up":
                    book = {
                        "tick_size": "0.01",
                        "min_order_size": "5",
                        "bids": [{"price": "0.48", "size": "120"}],
                        "asks": [{"price": "0.50", "size": "90"}],
                    }
                else:
                    book = {
                        "tick_size": "0.01",
                        "min_order_size": "5",
                        "bids": [{"price": "0.49", "size": "70"}],
                        "asks": [{"price": "0.49", "size": "80"}],
                    }
            else:
                book = fetch_orderbook(token_id)
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid_levels = [(to_float(x.get("price")), to_float(x.get("size"))) for x in bids]
            bid_levels = [(p, s) for p, s in bid_levels if p is not None]
            if bid_levels:
                bid_levels.sort(key=lambda x: x[0], reverse=True)
                best_bid, best_bid_sz = bid_levels[0]
            else:
                best_bid, best_bid_sz = None, None
            best_ask = to_float(asks[-1].get("price")) if False else None  # placeholder to avoid wrong assumption
            # CLOB asks ordering is not guaranteed; compute min ask.
            ask_levels = [(to_float(x.get("price")), to_float(x.get("size"))) for x in asks]
            ask_levels = [(p, s) for p, s in ask_levels if p is not None]
            if ask_levels:
                ask_levels.sort(key=lambda x: x[0])
                best_ask = ask_levels[0][0]
                best_ask_sz = ask_levels[0][1]
            else:
                best_ask_sz = None

            out[outcome] = {
                "token_id": token_id,
                "best_bid": best_bid,
                "best_bid_sz_l1": best_bid_sz,
                "best_ask": best_ask,
                "best_ask_sz_l1": best_ask_sz,
                "chosen_price": None,
                "reason": "uncomputed",
                "effective_max_price": self.target_price,
                "raw_book": {"tick_size": book.get("tick_size"), "min_order_size": book.get("min_order_size")},
            }

        # 2) Pair-aware decision:
        # If best_bid_up + best_bid_down < 1.00, join the nearest top-of-book levels
        # (often 0.49 + 0.50) to increase fill priority and maker volume/rebate.
        up_bid = out["Up"].get("best_bid")
        down_bid = out["Down"].get("best_bid")
        pair_bid_sum = None
        pair_join_mode = False
        if up_bid is not None and down_bid is not None:
            pair_bid_sum = float(up_bid) + float(down_bid)
            pair_join_mode = pair_bid_sum < 1.0

        for outcome in ("Up", "Down"):
            c = out[outcome]
            best_bid = c.get("best_bid")
            best_ask = c.get("best_ask")
            if pair_join_mode and best_bid is not None:
                desired = float(best_bid)
                reason = "pair_sum_lt_1_join_best_bid"
                if best_ask is not None and desired >= float(best_ask):
                    desired = float(best_ask) - ctx.tick_size
                    reason = "pair_sum_lt_1_below_ask_to_stay_maker"
                desired = round_to_tick_down(desired, ctx.tick_size)
                c["chosen_price"] = desired if desired > 0 else None
                c["reason"] = reason
                # Allow up to top-book in pair mode (not hard-capped to target=0.49).
                c["effective_max_price"] = 1.0
            else:
                chosen_price, reason = choose_initial_maker_buy_price(
                    max_price=self.target_price,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    tick_size=ctx.tick_size,
                )
                c["chosen_price"] = chosen_price
                c["reason"] = reason
                c["effective_max_price"] = self.target_price

            if pair_bid_sum is not None:
                c["pair_bid_sum"] = pair_bid_sum
                c["pair_join_mode"] = pair_join_mode

        return out

    def _fetch_books_state(self, ctx: MarketContext) -> Dict[str, Dict[str, Any]]:
        out = {}
        for outcome, token_id in (("Up", ctx.up_token_id), ("Down", ctx.down_token_id)):
            if self.offline_mock:
                # Simpler mock source for paper loop; repricing path is driven by current bids/asks updates in loop.
                if outcome == "Up":
                    bid = round(random.choice([0.47, 0.48, 0.49]), 2)
                    ask = round(min(0.99, bid + random.choice([0.01, 0.02])), 2)
                else:
                    bid = round(random.choice([0.47, 0.48, 0.49]), 2)
                    ask = round(min(0.99, bid + random.choice([0.0, 0.01])), 2)
                book = {
                    "bids": [{"price": f"{bid:.2f}", "size": f"{random.uniform(5,300):.2f}"}],
                    "asks": [{"price": f"{ask:.2f}", "size": f"{random.uniform(5,300):.2f}"}],
                }
            else:
                book = fetch_orderbook(token_id)
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid_levels = [(to_float(x.get("price")), to_float(x.get("size"))) for x in bids]
            bid_levels = [(p, s) for p, s in bid_levels if p is not None]
            if bid_levels:
                bid_levels.sort(key=lambda x: x[0], reverse=True)
                best_bid, best_bid_sz = bid_levels[0]
            else:
                best_bid, best_bid_sz = None, None
            ask_levels = [(to_float(x.get("price")), to_float(x.get("size"))) for x in asks]
            ask_levels = [(p, s) for p, s in ask_levels if p is not None]
            if ask_levels:
                ask_levels.sort(key=lambda x: x[0])
                best_ask, best_ask_sz = ask_levels[0]
            else:
                best_ask, best_ask_sz = None, None
            out[outcome] = {
                "token_id": token_id,
                "best_bid": best_bid,
                "best_bid_sz_l1": best_bid_sz,
                "best_ask": best_ask,
                "best_ask_sz_l1": best_ask_sz,
            }
        return out

    def _insert_cycle_and_orders(self, ctx: MarketContext, chosen: Dict[str, Dict[str, Any]]) -> str:
        cycle_id = str(uuid.uuid4())
        self.db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": ctx.slug,
                "condition_id": ctx.condition_id,
                "event_start_ts": ctx.event_start_ts,
                "end_ts": ctx.end_ts,
                "trigger_time_ts": ctx.event_start_ts - self.lead_seconds,
                "target_price": self.target_price,
                "target_size": self.size_shares,
                "status": "orders_placed" if self.mode == "dry-run" else "placing_orders",
                "mode": self.mode,
                "note": ctx.question,
                "raw_json": {"question": ctx.question},
            }
        )
        for outcome in ("Up", "Down"):
            c = chosen[outcome]
            local_order_id = f"{cycle_id}:{outcome.lower()}"
            self.db.insert_order(
                {
                    "local_order_id": local_order_id,
                    "cycle_id": cycle_id,
                    "slug": ctx.slug,
                    "condition_id": ctx.condition_id,
                    "outcome_name": outcome,
                    "token_id": c["token_id"],
                    "side": "BUY",
                    "target_price": self.target_price,
                    "chosen_price": c["chosen_price"],
                    "size": self.size_shares,
                    "post_only": True,
                    "tif": "GTD",
                    "status": "mock_placed" if self.mode == "dry-run" else "submitted",
                    "placement_reason": c["reason"],
                    "book_best_bid": c["best_bid"],
                    "book_best_ask": c["best_ask"],
                    "book_bid_sz_l1": c["best_bid_sz_l1"],
                    "book_ask_sz_l1": c["best_ask_sz_l1"],
                    "tick_size": ctx.tick_size,
                    "raw_json": c["raw_book"],
                }
            )
            if c.get("chosen_price") is not None:
                self.db.insert_capital_event(
                    {
                        "cycle_id": cycle_id,
                        "slug": ctx.slug,
                        "event_type": "reserve",
                        "outcome_name": outcome,
                        "local_order_id": local_order_id,
                        "amount_usdc": float(c["chosen_price"]) * float(self.size_shares),
                        "note": "reserve_est_at_placement",
                    }
                )
        return cycle_id

    def _simulate_offline_repricing(self, cycle_id: str, slug: str, chosen: Dict[str, Dict[str, Any]], scenario_seed: int = 0) -> Dict[str, Dict[str, Any]]:
        """
        Offline mock repricing simulation driven by synthetic best bid/ask paths.
        Updates chosen prices and logs order updates into DB.
        """
        rng = random.Random(scenario_seed)
        state = {
            "Up": dict(chosen["Up"]),
            "Down": dict(chosen["Down"]),
        }
        # Synthetic paths around 0.50 to exercise repricing. Up likely starts 0.48 -> may become 0.49.
        for step in range(8):
            for outcome in ("Up", "Down"):
                cur = state[outcome]
                base_bid = cur.get("best_bid") if cur.get("best_bid") is not None else 0.47
                # Random walk by one tick with bounded range.
                move = rng.choice([-0.01, 0.0, 0.01])
                best_bid = max(0.01, min(0.49, round((base_bid + move), 2)))
                # Keep ask >= bid; sometimes tight at same level edge case.
                spread_ticks = rng.choice([0, 1, 2])
                best_ask = round(best_bid + (0.01 * spread_ticks), 2)
                if best_ask <= 0:
                    best_ask = 0.01

                old_price = cur.get("chosen_price")
                new_price, reason = choose_reprice_maker_buy(
                    current_price=old_price,
                    max_price=float(cur.get("effective_max_price", self.target_price)),
                    best_bid=best_bid,
                    best_ask=best_ask,
                    tick_size=0.01,
                )
                cur["best_bid"] = best_bid
                cur["best_ask"] = best_ask
                cur["best_bid_sz_l1"] = round(rng.uniform(5, 500), 2)
                cur["best_ask_sz_l1"] = round(rng.uniform(5, 500), 2)
                if new_price != old_price:
                    cur["chosen_price"] = new_price
                    local_order_id = f"{cycle_id}:{outcome.lower()}"
                    self.db.insert_order_update(
                        {
                            "local_order_id": local_order_id,
                            "cycle_id": cycle_id,
                            "slug": slug,
                            "old_price": old_price,
                            "new_price": new_price,
                            "best_bid": best_bid,
                            "best_ask": best_ask,
                            "reason": reason,
                            "raw_json": {"step": step, "outcome": outcome},
                        }
                    )
                    self.db.insert_order(
                        {
                            "local_order_id": local_order_id,
                            "cycle_id": cycle_id,
                            "slug": slug,
                            "condition_id": None,
                            "outcome_name": outcome,
                            "token_id": cur["token_id"],
                            "side": "BUY",
                            "target_price": self.target_price,
                            "chosen_price": new_price,
                            "size": self.size_shares,
                            "post_only": True,
                            "tif": "GTD",
                            "status": "mock_repriced",
                            "placement_reason": reason,
                            "book_best_bid": best_bid,
                            "book_best_ask": best_ask,
                            "book_bid_sz_l1": cur["best_bid_sz_l1"],
                            "book_ask_sz_l1": cur["best_ask_sz_l1"],
                            "tick_size": 0.01,
                            "raw_json": {"repriced_step": step},
                        }
                    )
        return state

    def _mock_fill_cycle(self, cycle_id: str, ctx: MarketContext, chosen: Dict[str, Dict[str, Any]]) -> None:
        """
        Mock state transitions for logic testing only.
        """
        if self.mock_fill == "none":
            self.db.upsert_cycle(
                {
                    "cycle_id": cycle_id,
                    "slug": ctx.slug,
                    "condition_id": ctx.condition_id,
                    "target_price": self.target_price,
                    "target_size": self.size_shares,
                    "status": "no_fill",
                    "mode": self.mode,
                    "note": "mock none",
                }
            )
            return

        def add_fill(outcome: str, frac: float = 1.0) -> None:
            local_order_id = f"{cycle_id}:{outcome.lower()}"
            fill_size = round(self.size_shares * frac, 6)
            fill_price = chosen[outcome]["chosen_price"] if chosen[outcome]["chosen_price"] is not None else self.target_price
            self.db.insert_fill(
                {
                    "fill_id": str(uuid.uuid4()),
                    "local_order_id": local_order_id,
                    "cycle_id": cycle_id,
                    "slug": ctx.slug,
                    "outcome_name": outcome,
                    "side": "BUY",
                    "fill_price": fill_price,
                    "fill_size": fill_size,
                    "maker": True,
                    "fee_or_rebate": None,
                    "fill_time": utc_now_iso(),
                    "source": "mock",
                    "raw_json": {"mock_fill": self.mock_fill},
                }
            )
            self.db.insert_capital_event(
                {
                    "cycle_id": cycle_id,
                    "slug": ctx.slug,
                    "event_type": "fill_cost",
                    "outcome_name": outcome,
                    "local_order_id": local_order_id,
                    "amount_usdc": float(fill_price) * float(fill_size),
                    "note": "mock fill cost",
                }
            )
            self.db.update_order_status(local_order_id, "mock_filled")

        if self.mock_fill == "both":
            add_fill("Up")
            add_fill("Down")
            new_status = "both_filled_wait_resolution"
        elif self.mock_fill == "up_only":
            add_fill("Up")
            self.db.update_order_status(f"{cycle_id}:down", "mock_open_unfilled")
            new_status = "up_only_wait_resolution"
        elif self.mock_fill == "down_only":
            add_fill("Down")
            self.db.update_order_status(f"{cycle_id}:up", "mock_open_unfilled")
            new_status = "down_only_wait_resolution"
        elif self.mock_fill == "partial_both":
            add_fill("Up", 0.5)
            add_fill("Down", 0.5)
            new_status = "partial_both_wait_resolution"
        else:
            new_status = "orders_placed"

        self.db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": ctx.slug,
                "condition_id": ctx.condition_id,
                "target_price": self.target_price,
                "target_size": self.size_shares,
                "status": new_status,
                "mode": self.mode,
                "note": f"mock={self.mock_fill}",
            }
        )
        self._reconcile_cycle_unfilled_reserve(cycle_id, ctx.slug)

    def run_mock_batch(self, n: int, seed: int = 42) -> None:
        rng = random.Random(seed)
        statuses = {}
        repricing_updates = 0
        for i in range(n):
            slug = f"btc-updown-5m-MOCKBATCH-{i:03d}"
            # Rotate mock outcomes to exercise state transitions.
            mock_fill = rng.choices(
                ["both", "up_only", "down_only", "none", "partial_both"],
                weights=[45, 20, 20, 10, 5],
                k=1,
            )[0]
            old_mock_fill = self.mock_fill
            self.mock_fill = mock_fill
            cycle_id = self.run_cycle_for_slug(slug)
            row = self.db.conn.execute(
                "SELECT status FROM strategy_cycles WHERE cycle_id = ?", (cycle_id,)
            ).fetchone()
            status = row["status"] if row else "unknown"
            statuses[status] = statuses.get(status, 0) + 1
            rc = self.db.conn.execute(
                "SELECT COUNT(*) AS c FROM strategy_order_updates WHERE cycle_id = ?",
                (cycle_id,),
            ).fetchone()
            repricing_updates += int(rc["c"] if rc else 0)
            self.mock_fill = old_mock_fill

        print("[mock-batch-summary]", f"n={n}", f"repricing_updates={repricing_updates}", flush=True)
        for k in sorted(statuses):
            print("[mock-batch-status]", f"status={k}", f"count={statuses[k]}", flush=True)

    def _paper_fill_if_touched(self, cycle_id: str, slug: str, outcome: str, order_state: Dict[str, Any], book: Dict[str, Any]) -> bool:
        if order_state.get("filled"):
            return False
        px = order_state.get("chosen_price")
        if px is None:
            return False
        best_bid = book.get("best_bid")
        if best_bid is None:
            return False
        # Maker BUY fill proxy: market bid reaches/touches our bid level (queue not modeled).
        if best_bid >= px:
            fill_id = str(uuid.uuid4())
            self.db.insert_fill(
                {
                    "fill_id": fill_id,
                    "local_order_id": order_state["local_order_id"],
                    "cycle_id": cycle_id,
                    "slug": slug,
                    "outcome_name": outcome,
                    "side": "BUY",
                    "fill_price": px,
                    "fill_size": self.size_shares,
                    "maker": True,
                    "fee_or_rebate": None,
                    "fill_time": utc_now_iso(),
                    "source": "paper_live_touch_proxy",
                    "raw_json": {"best_bid": best_bid, "best_ask": book.get("best_ask")},
                }
            )
            self.db.insert_capital_event(
                {
                    "cycle_id": cycle_id,
                    "slug": slug,
                    "event_type": "fill_cost",
                    "outcome_name": outcome,
                    "local_order_id": order_state["local_order_id"],
                    "amount_usdc": float(px) * float(self.size_shares),
                    "note": "paper_live_touch_proxy fill cost",
                }
            )
            self.db.update_order_status(order_state["local_order_id"], "paper_filled")
            order_state["filled"] = True
            print(
                "[paper-fill]",
                f"slug={slug}",
                f"cycle_id={cycle_id}",
                f"outcome={outcome}",
                f"px={px}",
                f"best_bid={best_bid}",
                flush=True,
            )
            return True
        return False

    def _reconcile_cycle_unfilled_reserve(self, cycle_id: str, slug: str) -> None:
        fills = self.db.cycle_fills_by_outcome(cycle_id)
        for row in self.db.cycle_orders(cycle_id):
            order_size = float(row["size"] or 0.0)
            filled = float(fills.get(str(row["outcome_name"]), 0.0))
            remaining = max(0.0, order_size - min(order_size, filled))
            px = row["chosen_price"]
            if remaining > 0 and px is not None:
                self.db.insert_capital_event(
                    {
                        "cycle_id": cycle_id,
                        "slug": slug,
                        "event_type": "reserve_release",
                        "outcome_name": row["outcome_name"],
                        "local_order_id": row["local_order_id"],
                        "amount_usdc": float(px) * remaining,
                        "note": "release_unfilled_reserve_est",
                    }
                )

    def _run_paper_live_cycle(self, ctx: MarketContext, cycle_id: str, chosen: Dict[str, Dict[str, Any]], wait_for_trigger: bool, poll_seconds: float) -> None:
        trigger_ts = ctx.event_start_ts - self.lead_seconds
        if wait_for_trigger:
            while time.time() < trigger_ts:
                rem = trigger_ts - time.time()
                print("[paper]", f"slug={ctx.slug}", f"cycle_id={cycle_id}", f"waiting_to_trigger_s={round(rem,1)}", flush=True)
                time.sleep(min(max(rem, 0.5), 5.0))

        order_states: Dict[str, Dict[str, Any]] = {}
        for outcome in ("Up", "Down"):
            order_states[outcome] = {
                "local_order_id": f"{cycle_id}:{outcome.lower()}",
                "chosen_price": chosen[outcome]["chosen_price"],
                "effective_max_price": chosen[outcome].get("effective_max_price", self.target_price),
                "filled": False,
            }

        self.db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": ctx.slug,
                "condition_id": ctx.condition_id,
                "target_price": self.target_price,
                "target_size": self.size_shares,
                "status": "paper_live_orders_working",
                "mode": self.mode,
                "note": "paper live started",
            }
        )

        while time.time() <= ctx.end_ts:
            books = self._fetch_books_state(ctx)
            any_fill = False
            for outcome in ("Up", "Down"):
                st = order_states[outcome]
                if st["filled"]:
                    continue
                b = books[outcome]
                old_px = st.get("chosen_price")
                new_px, reason = choose_reprice_maker_buy(
                    current_price=old_px,
                    max_price=float(st.get("effective_max_price", self.target_price)),
                    best_bid=b.get("best_bid"),
                    best_ask=b.get("best_ask"),
                    tick_size=ctx.tick_size,
                )
                if new_px != old_px and new_px is not None:
                    self.db.insert_order_update(
                        {
                            "local_order_id": st["local_order_id"],
                            "cycle_id": cycle_id,
                            "slug": ctx.slug,
                            "old_price": old_px,
                            "new_price": new_px,
                            "best_bid": b.get("best_bid"),
                            "best_ask": b.get("best_ask"),
                            "reason": reason,
                            "raw_json": {"paper_live": True},
                        }
                    )
                    self.db.insert_order(
                        {
                            "local_order_id": st["local_order_id"],
                            "cycle_id": cycle_id,
                            "slug": ctx.slug,
                            "condition_id": ctx.condition_id,
                            "outcome_name": outcome,
                            "token_id": b["token_id"],
                            "side": "BUY",
                            "target_price": self.target_price,
                            "chosen_price": new_px,
                            "size": self.size_shares,
                            "post_only": True,
                            "tif": "GTD",
                            "status": "paper_repriced",
                            "placement_reason": reason,
                            "book_best_bid": b.get("best_bid"),
                            "book_best_ask": b.get("best_ask"),
                            "book_bid_sz_l1": b.get("best_bid_sz_l1"),
                            "book_ask_sz_l1": b.get("best_ask_sz_l1"),
                            "tick_size": ctx.tick_size,
                            "raw_json": {"paper_live": True},
                        }
                    )
                    st["chosen_price"] = new_px
                any_fill = self._paper_fill_if_touched(cycle_id, ctx.slug, outcome, st, b) or any_fill

            up_f = order_states["Up"]["filled"]
            dn_f = order_states["Down"]["filled"]
            if up_f and dn_f:
                self.db.upsert_cycle(
                    {
                        "cycle_id": cycle_id,
                        "slug": ctx.slug,
                        "condition_id": ctx.condition_id,
                        "target_price": self.target_price,
                        "target_size": self.size_shares,
                        "status": "both_filled_wait_resolution",
                        "mode": self.mode,
                        "note": "paper live both filled",
                    }
                )
                # No reserve release here; both reserves are considered consumed into positions.
                print("[paper]", f"slug={ctx.slug}", f"cycle_id={cycle_id}", "both legs filled; stop monitoring cycle", flush=True)
                return

            if not any_fill:
                print(
                    "[paper]",
                    f"slug={ctx.slug}",
                    f"cycle_id={cycle_id}",
                    f"up_px={order_states['Up']['chosen_price']} up_bid={books['Up']['best_bid']} up_ask={books['Up']['best_ask']}",
                    f"down_px={order_states['Down']['chosen_price']} down_bid={books['Down']['best_bid']} down_ask={books['Down']['best_ask']}",
                    flush=True,
                )
            time.sleep(max(0.2, poll_seconds))

        # End reached with potential single/no fill.
        up_f = order_states["Up"]["filled"]
        dn_f = order_states["Down"]["filled"]
        status = "no_fill"
        if up_f and not dn_f:
            status = "up_only_wait_resolution"
        elif dn_f and not up_f:
            status = "down_only_wait_resolution"
        elif up_f and dn_f:
            status = "both_filled_wait_resolution"
        self.db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": ctx.slug,
                "condition_id": ctx.condition_id,
                "target_price": self.target_price,
                "target_size": self.size_shares,
                "status": status,
                "mode": self.mode,
                "note": "paper live end reached",
            }
        )
        self._reconcile_cycle_unfilled_reserve(cycle_id, ctx.slug)
        print("[paper]", f"slug={ctx.slug}", f"cycle_id={cycle_id}", f"end reached status={status}", flush=True)

    def _extract_order_id_from_response(self, resp: Any) -> Tuple[Optional[str], Any]:
        payload: Any = resp
        try:
            if hasattr(resp, "json"):
                payload = resp.json()
        except Exception:
            payload = resp
        if isinstance(payload, dict):
            for key in ("orderID", "orderId", "id"):
                if key in payload and payload[key]:
                    return str(payload[key]), payload
            if isinstance(payload.get("order"), dict):
                nested = payload["order"]
                for key in ("id", "orderID", "orderId"):
                    if nested.get(key):
                        return str(nested[key]), payload
        return None, payload

    def _run_live_cycle(self, ctx: MarketContext, cycle_id: str, chosen: Dict[str, Dict[str, Any]]) -> None:
        if not self.live_confirm:
            raise RuntimeError("Live mode requires --live-confirm (real orders)")
        client = self._init_live_client()
        try:
            ok_resp = client.get_ok()
            print("[live]", f"clob_ok={ok_resp}", flush=True)
        except Exception as exc:
            raise RuntimeError(f"Live CLOB connectivity/auth failed: {exc}")
        if self.live_block_if_open_orders:
            open_orders = self._get_live_open_orders(client)
            if open_orders:
                # Hard guard: never add new limit orders while any are already open.
                sample = open_orders[:3]
                print(
                    "[live-guard]",
                    f"slug={ctx.slug}",
                    f"cycle_id={cycle_id}",
                    f"reason=open_orders_exist",
                    f"count={len(open_orders)}",
                    f"sample={json.dumps(sample, ensure_ascii=True)[:400]}",
                    flush=True,
                )
                for outcome in ("Up", "Down"):
                    self.db.update_order_status(f"{cycle_id}:{outcome.lower()}", "skipped_open_orders_exist", {"open_orders_count": len(open_orders)})
                self.db.upsert_cycle(
                    {
                        "cycle_id": cycle_id,
                        "slug": ctx.slug,
                        "condition_id": ctx.condition_id,
                        "target_price": self.target_price,
                        "target_size": self.size_shares,
                        "status": "skipped_open_orders_exist",
                        "mode": self.mode,
                        "note": f"open_orders_exist count={len(open_orders)}",
                        "raw_json": {"open_orders_count": len(open_orders), "sample": sample},
                    }
                )
                return
        bal = self._get_live_collateral_balance_allowance()
        totals = self._db_capital_totals()
        print(
            "[live-capital]",
            f"slug={ctx.slug}",
            f"cycle_id={cycle_id}",
            f"collateral_balance_allowance={json.dumps(bal, ensure_ascii=True)[:400]}",
            f"db_est_locked={round(totals['est_locked_positions'],4)}",
            flush=True,
        )

        self.db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": ctx.slug,
                "condition_id": ctx.condition_id,
                "target_price": self.target_price,
                "target_size": self.size_shares,
                "status": "submitting_orders",
                "mode": self.mode,
                "note": "live submit start",
            }
        )

        live_results = {}
        local_by_outcome: Dict[str, str] = {}
        for outcome in ("Up", "Down"):
            c = chosen[outcome]
            local_order_id = f"{cycle_id}:{outcome.lower()}"
            local_by_outcome[outcome] = local_order_id
            px = c.get("chosen_price")
            if px is None:
                self.db.update_order_live_submission(local_order_id, "skipped_no_price", None, {"reason": c.get("reason")})
                live_results[outcome] = {"status": "skipped_no_price"}
                continue
            order_args = OrderArgs(
                token_id=str(c["token_id"]),
                price=float(px),
                size=float(self.size_shares),
                side="BUY",
            )
            try:
                signed = client.create_order(order_args)
                resp = client.post_order(signed, OrderType.GTC)
                exchange_order_id, payload = self._extract_order_id_from_response(resp)
                self.db.update_order_live_submission(
                    local_order_id,
                    "live_submitted",
                    exchange_order_id,
                    {"response": payload},
                )
                live_results[outcome] = {"status": "submitted", "exchange_order_id": exchange_order_id, "response": payload}
                print(
                    "[live-submit]",
                    f"slug={ctx.slug}",
                    f"cycle_id={cycle_id}",
                    f"outcome={outcome}",
                    f"px={px}",
                    f"size={self.size_shares}",
                    f"exchange_order_id={exchange_order_id}",
                    flush=True,
                )
            except Exception as exc:
                self.db.update_order_live_submission(local_order_id, "live_submit_error", None, {"error": str(exc)})
                live_results[outcome] = {"status": "error", "error": str(exc)}
                print(
                    "[live-submit-error]",
                    f"slug={ctx.slug}",
                    f"cycle_id={cycle_id}",
                    f"outcome={outcome}",
                    f"error={exc}",
                    flush=True,
                )

        # No stop/merge/redeem automation here yet. We just place and log.
        both_submitted = all(live_results.get(o, {}).get("status") == "submitted" for o in ("Up", "Down"))
        cycle_status = "live_orders_submitted" if both_submitted else "live_submit_partial_or_error"
        self.db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": ctx.slug,
                "condition_id": ctx.condition_id,
                "target_price": self.target_price,
                "target_size": self.size_shares,
                "status": cycle_status,
                "mode": self.mode,
                "note": json.dumps(live_results, ensure_ascii=True)[:800],
            }
        )
        print("[live]", f"cycle_status={cycle_status}", flush=True)
        if both_submitted and self.live_monitor_enabled:
            self._monitor_live_orders(ctx, cycle_id, chosen, local_by_outcome, live_results, client)

    def _monitor_live_orders(
        self,
        ctx: MarketContext,
        cycle_id: str,
        chosen: Dict[str, Dict[str, Any]],
        local_by_outcome: Dict[str, str],
        live_results: Dict[str, Dict[str, Any]],
        client: ClobClient,
    ) -> None:
        start = time.time()
        deadline = ctx.end_ts if ctx.end_ts else (start + 600)
        if self.live_max_seconds and self.live_max_seconds > 0:
            deadline = min(deadline, start + self.live_max_seconds)
        matched_seen: Dict[str, float] = {"Up": 0.0, "Down": 0.0}
        latest_status: Dict[str, str] = {}
        latest_matched: Dict[str, float] = {"Up": 0.0, "Down": 0.0}
        terminal_statuses = {"canceled", "cancelled", "filled", "matched", "expired", "rejected"}
        print(
            "[live-monitor]",
            f"slug={ctx.slug}",
            f"cycle_id={cycle_id}",
            f"poll_s={self.live_poll_seconds}",
            f"until={datetime.fromtimestamp(deadline, tz=timezone.utc).isoformat()}",
            flush=True,
        )
        while time.time() < deadline:
            all_terminal = True
            any_change = False
            for outcome in ("Up", "Down"):
                res = live_results.get(outcome, {})
                if res.get("status") != "submitted":
                    continue
                ex_id = res.get("exchange_order_id")
                if not ex_id:
                    continue
                local_order_id = local_by_outcome[outcome]
                try:
                    order_resp = client.get_order(ex_id)
                    st = self._extract_order_state(order_resp)
                except Exception as exc:
                    print(
                        "[live-monitor-error]",
                        f"slug={ctx.slug}",
                        f"cycle_id={cycle_id}",
                        f"outcome={outcome}",
                        f"err={exc}",
                        flush=True,
                    )
                    all_terminal = False
                    continue

                status_text = str(st.get("status") or "unknown").lower()
                matched = float(st.get("matched_size") or 0.0)
                latest_status[outcome] = status_text
                latest_matched[outcome] = matched
                delta = round(matched - matched_seen.get(outcome, 0.0), 10)
                if delta > 0:
                    fill_price = st.get("avg_price") or st.get("price") or chosen[outcome].get("chosen_price")
                    fill_id = f"{local_order_id}:live:{matched:.8f}"
                    self.db.insert_fill(
                        {
                            "fill_id": fill_id,
                            "local_order_id": local_order_id,
                            "cycle_id": cycle_id,
                            "slug": ctx.slug,
                            "outcome_name": outcome,
                            "side": "BUY",
                            "fill_price": float(fill_price) if fill_price is not None else float(chosen[outcome]["chosen_price"]),
                            "fill_size": float(delta),
                            "maker": True,
                            "fill_time": utc_now_iso(),
                            "source": "clob_get_order_poll",
                            "raw_json": st.get("raw"),
                        }
                    )
                    self.db.insert_capital_event(
                        {
                            "cycle_id": cycle_id,
                            "slug": ctx.slug,
                            "event_type": "fill_cost",
                            "outcome_name": outcome,
                            "local_order_id": local_order_id,
                            "amount_usdc": float(delta) * float(chosen[outcome]["chosen_price"]),
                            "note": "live fill cost from get_order matched_size delta",
                            "raw_json": {"matched_total": matched, "delta": delta, "exchange_order_id": ex_id},
                        }
                    )
                    matched_seen[outcome] = matched
                    any_change = True
                    print(
                        "[live-fill]",
                        f"slug={ctx.slug}",
                        f"cycle_id={cycle_id}",
                        f"outcome={outcome}",
                        f"delta={delta}",
                        f"matched_total={matched}",
                        f"status={status_text}",
                        flush=True,
                    )

                # Persist order status snapshot
                self.db.update_order_status(local_order_id, f"live_{status_text}", st.get("raw") if isinstance(st.get("raw"), dict) else None)

                # terminal if fully matched OR explicit terminal status
                target_sz = float(self.size_shares)
                is_terminal = status_text in terminal_statuses or matched >= target_sz - 1e-9
                if not is_terminal:
                    all_terminal = False

            # update cycle status summary
            summary = self.db.cycle_order_fill_summary(cycle_id)
            up_filled = float(summary["up_filled"] or 0.0)
            down_filled = float(summary["down_filled"] or 0.0)
            new_cycle_status = None
            if up_filled >= self.size_shares and down_filled >= self.size_shares:
                new_cycle_status = "both_filled_wait_resolution"
            elif up_filled > 0 and down_filled > 0:
                new_cycle_status = "partial_both_wait_resolution"
            elif up_filled > 0:
                new_cycle_status = "up_only_wait_resolution"
            elif down_filled > 0:
                new_cycle_status = "down_only_wait_resolution"
            else:
                new_cycle_status = "live_orders_submitted"
            self.db.upsert_cycle(
                {
                    "cycle_id": cycle_id,
                    "slug": ctx.slug,
                    "condition_id": ctx.condition_id,
                    "target_price": self.target_price,
                    "target_size": self.size_shares,
                    "status": new_cycle_status,
                    "mode": self.mode,
                    "note": f"live monitor up={up_filled} down={down_filled}",
                    "raw_json": {"monitor": True, "up_filled": up_filled, "down_filled": down_filled},
                }
            )
            if any_change:
                print(
                    "[live-monitor]",
                    f"slug={ctx.slug}",
                    f"cycle_id={cycle_id}",
                    f"status={new_cycle_status}",
                    f"up_filled={up_filled}",
                    f"down_filled={down_filled}",
                    flush=True,
                )
            if all_terminal:
                break
            time.sleep(max(0.2, self.live_poll_seconds))

        # Cancel any still-open live orders for this cycle so they don't block the next cycle.
        for outcome in ("Up", "Down"):
            res = live_results.get(outcome, {})
            if res.get("status") != "submitted":
                continue
            ex_id = res.get("exchange_order_id")
            if not ex_id:
                continue
            st = latest_status.get(outcome, "")
            matched = float(latest_matched.get(outcome, 0.0))
            if st in terminal_statuses or matched >= float(self.size_shares) - 1e-9:
                continue
            local_order_id = local_by_outcome[outcome]
            try:
                cancel_resp = client.cancel(ex_id)
                self.db.update_order_status(
                    local_order_id,
                    "live_canceled",
                    {"exchange_order_id": ex_id, "cancel_response": self._safe_obj_to_dict(cancel_resp)},
                )
                print(
                    "[live-cancel]",
                    f"slug={ctx.slug}",
                    f"cycle_id={cycle_id}",
                    f"outcome={outcome}",
                    f"exchange_order_id={ex_id}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    "[live-cancel-error]",
                    f"slug={ctx.slug}",
                    f"cycle_id={cycle_id}",
                    f"outcome={outcome}",
                    f"exchange_order_id={ex_id}",
                    f"err={exc}",
                    flush=True,
                )

        # Release reserves for any unfilled quantity at monitor exit (order may remain open but bot process exits).
        self._reconcile_cycle_unfilled_reserve(cycle_id, ctx.slug)
        summary = self.db.cycle_order_fill_summary(cycle_id)
        print(
            "[live-monitor-end]",
            f"slug={ctx.slug}",
            f"cycle_id={cycle_id}",
            f"up_filled={summary['up_filled']}",
            f"down_filled={summary['down_filled']}",
            f"time_s={round(time.time()-start,1)}",
            flush=True,
        )

    def run_cycle_for_slug(self, slug: str) -> str:
        existing = self.db.has_nonterminal_cycle_for_slug(slug)
        if existing:
            print(
                "[bot-guard]",
                f"slug={slug}",
                f"reason=existing_nonterminal_cycle",
                f"existing_cycle_id={existing['cycle_id']}",
                f"status={existing['status']}",
                flush=True,
            )
            return str(existing["cycle_id"])
        ctx = self._build_market_context(slug)
        chosen = self._choose_prices_from_books(ctx)

        print(
            "[bot]",
            f"slug={slug}",
            f"event_start_utc={datetime.fromtimestamp(ctx.event_start_ts, tz=timezone.utc).isoformat()}",
            f"end_utc={datetime.fromtimestamp(ctx.end_ts, tz=timezone.utc).isoformat()}",
            f"tick={ctx.tick_size}",
            flush=True,
        )
        cycle_id = self._insert_cycle_and_orders(ctx, chosen)
        for outcome in ("Up", "Down"):
            c = chosen[outcome]
            print(
                "[bot-order-plan]",
                f"slug={ctx.slug}",
                f"cycle_id={cycle_id}",
                f"{outcome}",
                f"bb={c['best_bid']}",
                f"ba={c['best_ask']}",
                f"target={self.target_price}",
                f"px={c['chosen_price']}",
                f"why={c['reason']}",
                f"token={c['token_id']}",
                flush=True,
            )
        if self.mode == "dry-run":
            if self.offline_mock:
                chosen = self._simulate_offline_repricing(cycle_id, ctx.slug, chosen, scenario_seed=abs(hash(ctx.slug)) % (2**32))
            self._mock_fill_cycle(cycle_id, ctx, chosen)
            summary = self.db.cycle_order_fill_summary(cycle_id)
            print(
                "[bot-cycle]",
                f"cycle_id={cycle_id}",
                f"status={summary['cycle_status']}",
                f"up_filled={summary['up_filled']}",
                f"down_filled={summary['down_filled']}",
                flush=True,
            )
        elif self.mode == "paper-live":
            self._run_paper_live_cycle(ctx, cycle_id, chosen, wait_for_trigger=True, poll_seconds=self.paper_poll_seconds)
            summary = self.db.cycle_order_fill_summary(cycle_id)
            print(
                "[bot-cycle]",
                f"cycle_id={cycle_id}",
                f"status={summary['cycle_status']}",
                f"up_filled={summary['up_filled']}",
                f"down_filled={summary['down_filled']}",
                flush=True,
            )
        else:
            self._run_live_cycle(ctx, cycle_id, chosen)
        return cycle_id


def main() -> None:
    p = argparse.ArgumentParser(description="BTC 5m trading bot skeleton (dry-run/mock first)")
    p.add_argument("--db", default="polymarket_collector.db", help="SQLite DB path")
    p.add_argument("--slug", help="Specific market slug to test")
    p.add_argument("--target-price", type=float, default=0.49, help="Max BUY price target")
    p.add_argument("--size-shares", type=float, default=5.0, help="Order size per leg in shares")
    p.add_argument("--lead-seconds", type=float, default=300.0, help="Intended placement lead time before eventStartTime")
    p.add_argument("--mode", choices=["dry-run", "paper-live", "live"], default="dry-run")
    p.add_argument(
        "--mock-fill",
        choices=["none", "both", "up_only", "down_only", "partial_both"],
        default="both",
        help="Mock fill pattern for dry-run logic tests",
    )
    p.add_argument("--probe-next", action="store_true", help="If no --slug, probe next BTC 5m slug")
    p.add_argument("--offline-mock", action="store_true", help="No network: use mocked market+books to test logic and DB writes")
    p.add_argument("--mock-batch", type=int, default=0, help="Run N offline mock markets to test logic/statistics")
    p.add_argument("--mock-seed", type=int, default=42, help="Seed for mock batch")
    p.add_argument("--paper-poll-seconds", type=float, default=1.0, help="Polling interval for paper-live mode")
    p.add_argument("--paper-no-wait", action="store_true", help="In paper-live mode, do not wait until T-lead (useful for tests)")
    p.add_argument("--paper-max-seconds", type=float, default=0.0, help="Optional cap for paper-live loop duration (0 = until market end)")
    p.add_argument("--live-confirm", action="store_true", help="Required to allow real order placement in --mode live")
    p.add_argument("--no-live-monitor", action="store_true", help="In live mode, submit orders and exit immediately")
    p.add_argument("--live-poll-seconds", type=float, default=2.0, help="Polling interval for live order monitoring")
    p.add_argument("--live-max-seconds", type=float, default=0.0, help="Optional cap for live monitor duration (0 = until market end)")
    p.add_argument("--allow-existing-open-orders", action="store_true", help="Override safety guard and place live orders even if open orders already exist")
    args = p.parse_args()

    bot = BTC5MTradingBot(
        db_path=args.db,
        mode=args.mode,
        target_price=args.target_price,
        size_shares=args.size_shares,
        lead_seconds=args.lead_seconds,
        mock_fill=args.mock_fill,
        offline_mock=args.offline_mock,
        live_confirm=args.live_confirm,
    )
    bot.paper_poll_seconds = args.paper_poll_seconds
    bot.live_monitor_enabled = not args.no_live_monitor
    bot.live_poll_seconds = args.live_poll_seconds
    bot.live_max_seconds = args.live_max_seconds
    bot.live_block_if_open_orders = not args.allow_existing_open_orders
    try:
        if args.mock_batch:
            if not args.offline_mock:
                p.error("--mock-batch requires --offline-mock")
            bot.run_mock_batch(args.mock_batch, seed=args.mock_seed)
        else:
            slug = args.slug
            already_ran = False
            if not slug:
                if not args.probe_next:
                    p.error("Provide --slug or use --probe-next")
                slugs = discover_btc5m_by_slug_probe(
                    time.time(),
                    lead_seconds=args.lead_seconds,
                    lookback_seconds=900,
                    lookahead_seconds=1800,
                )
                # Process markets sequentially from the most recently traded slug onward.
                ref_ts = None
                try:
                    row = bot.db.conn.execute(
                        """
                        SELECT slug
                        FROM strategy_cycles
                        WHERE slug LIKE 'btc-updown-5m-%'
                        ORDER BY created_at DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    if row:
                        ref_ts = btc5m_slug_ts(str(row["slug"]))
                except Exception:
                    ref_ts = None
                slugs = reorder_slugs_from_reference(slugs, ref_ts)
                if not slugs:
                    raise SystemExit("No BTC 5m markets found via slug probe")
                last_exc = None
                for candidate in slugs:
                    print(f"[bot] probed slug={candidate}", flush=True)
                    try:
                        # In probe mode skip markets that already have a non-terminal cycle,
                        # so we can continue to the next available BTC 5m market.
                        existing = bot.db.has_nonterminal_cycle_for_slug(candidate)
                        if existing is not None:
                            print(
                                "[bot] skipping",
                                f"slug={candidate}",
                                "reason=existing_nonterminal_cycle",
                                f"cycle_id={existing['cycle_id']}",
                                f"status={existing['status']}",
                                flush=True,
                            )
                            continue
                        # Pre-validate market and books before committing to a live run.
                        ctx_probe = bot._build_market_context(candidate)
                        chosen_probe = bot._choose_prices_from_books(ctx_probe)
                        ok, reason = bot._is_market_ready_for_strategy(ctx_probe, chosen_probe)
                        if not ok:
                            print(f"[bot] skipping slug={candidate} reason={reason}", flush=True)
                            continue
                        print(f"[bot] pick slug={candidate} reason={reason}", flush=True)
                        if args.mode == "paper-live" and args.paper_no_wait:
                            original = bot._run_paper_live_cycle
                            def _patched(ctx, cycle_id, chosen, wait_for_trigger=True, poll_seconds=1.0):
                                if args.paper_max_seconds > 0:
                                    ctx = MarketContext(**{**ctx.__dict__, "end_ts": min(ctx.end_ts, time.time() + args.paper_max_seconds)})
                                return original(ctx, cycle_id, chosen, wait_for_trigger=False, poll_seconds=poll_seconds)
                            bot._run_paper_live_cycle = _patched  # type: ignore[method-assign]
                        bot.run_cycle_for_slug(candidate)
                        already_ran = True
                        slug = candidate
                        break
                    except Exception as exc:
                        last_exc = exc
                        msg = str(exc)
                        if "No orderbook exists for the requested token id" in msg or "HTTP 404" in msg:
                            print(f"[bot] skipping slug={candidate} reason=no_orderbook_yet", flush=True)
                            continue
                        raise
                if not slug:
                    # No tradable market in this probe window is normal; let caller retry.
                    print(f"[bot] no tradable probed BTC 5m slug found; last_error={last_exc}", flush=True)
            elif args.mode == "paper-live" and args.paper_no_wait:
                # Monkey patch for test convenience without changing default behavior
                original = bot._run_paper_live_cycle
                def _patched(ctx, cycle_id, chosen, wait_for_trigger=True, poll_seconds=1.0):
                    start = time.time()
                    # Wrap original but force no wait and optional timeout by shrinking end_ts in-memory
                    if args.paper_max_seconds > 0:
                        ctx = MarketContext(**{**ctx.__dict__, "end_ts": min(ctx.end_ts, time.time() + args.paper_max_seconds)})
                    return original(ctx, cycle_id, chosen, wait_for_trigger=False, poll_seconds=poll_seconds)
                bot._run_paper_live_cycle = _patched  # type: ignore[method-assign]
                bot._run_paper_live_cycle = _patched  # type: ignore[method-assign]
            if slug and not already_ran:
                bot.run_cycle_for_slug(slug)
        bot.close("success", "completed")
    except Exception as exc:
        bot.close("error", str(exc))
        raise


if __name__ == "__main__":
    main()
