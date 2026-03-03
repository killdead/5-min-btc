#!/usr/bin/env python3
import argparse
import bisect
import json
import signal
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from websockets.sync.client import connect as ws_connect

from fetch_polymarket_market_to_db import (
    ensure_schema,
    fetch_market_by_slug,
    fetch_orderbook,
    http_get_json,
    insert_event,
    insert_market,
    insert_orderbook_snapshot,
    insert_tokens,
    insert_trades,
    parse_json_maybe,
    to_float,
    utc_now_iso,
    DATA_BASE,
)


WSS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class CollectorDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        self.lock = threading.Lock()
        ensure_schema(self.conn)
        self._ensure_continuous_schema()

    def close(self) -> None:
        with self.lock:
            self.conn.commit()
            self.conn.close()

    def _ensure_continuous_schema(self) -> None:
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS collector_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS market_state_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL,
                    condition_id TEXT,
                    fetched_at TEXT NOT NULL,
                    active INTEGER,
                    closed INTEGER,
                    accepting_orders INTEGER,
                    best_bid REAL,
                    best_ask REAL,
                    last_trade_price REAL,
                    spread REAL,
                    volume REAL,
                    volume_num REAL,
                    volume_clob REAL,
                    liquidity REAL,
                    liquidity_num REAL,
                    liquidity_clob REAL,
                    raw_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_market_state_snapshots_slug_time
                    ON market_state_snapshots(slug, fetched_at DESC);

                CREATE TABLE IF NOT EXISTS ws_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL,
                    condition_id TEXT,
                    event_type TEXT,
                    asset_id TEXT,
                    event_timestamp_ms INTEGER,
                    received_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_ws_messages_slug_time
                    ON ws_messages(slug, received_at DESC);

                CREATE INDEX IF NOT EXISTS idx_ws_messages_event_type
                    ON ws_messages(event_type);

                CREATE TABLE IF NOT EXISTS ws_price_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ws_message_id INTEGER NOT NULL,
                    change_index INTEGER NOT NULL,
                    market TEXT,
                    asset_id TEXT,
                    side TEXT,
                    price REAL,
                    size REAL,
                    level_hash TEXT,
                    best_bid REAL,
                    best_ask REAL,
                    FOREIGN KEY (ws_message_id) REFERENCES ws_messages(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS ws_last_trade_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ws_message_id INTEGER NOT NULL,
                    market TEXT,
                    asset_id TEXT,
                    side TEXT,
                    price REAL,
                    size REAL,
                    fee_rate_bps REAL,
                    FOREIGN KEY (ws_message_id) REFERENCES ws_messages(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS ws_best_bid_ask_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ws_message_id INTEGER NOT NULL,
                    market TEXT,
                    asset_id TEXT,
                    best_bid REAL,
                    best_ask REAL,
                    spread REAL,
                    FOREIGN KEY (ws_message_id) REFERENCES ws_messages(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS ws_tick_size_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ws_message_id INTEGER NOT NULL,
                    market TEXT,
                    asset_id TEXT,
                    old_tick_size REAL,
                    new_tick_size REAL,
                    FOREIGN KEY (ws_message_id) REFERENCES ws_messages(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS ws_book_messages (
                    ws_message_id INTEGER PRIMARY KEY,
                    market TEXT,
                    asset_id TEXT,
                    book_hash TEXT,
                    tick_size REAL,
                    min_order_size REAL,
                    last_trade_price REAL,
                    FOREIGN KEY (ws_message_id) REFERENCES ws_messages(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS ws_book_levels (
                    ws_message_id INTEGER NOT NULL,
                    side TEXT NOT NULL CHECK(side IN ('bid','ask')),
                    level_index INTEGER NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    PRIMARY KEY (ws_message_id, side, level_index),
                    FOREIGN KEY (ws_message_id) REFERENCES ws_messages(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS pair_bbo_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL,
                    condition_id TEXT,
                    event_ts_ms INTEGER,
                    received_at TEXT NOT NULL,
                    phase TEXT,
                    time_to_start_sec REAL,
                    up_token_id TEXT,
                    down_token_id TEXT,
                    up_bid_l1 REAL, up_bid_sz_l1 REAL, up_ask_l1 REAL, up_ask_sz_l1 REAL,
                    up_bid_l2 REAL, up_bid_sz_l2 REAL, up_ask_l2 REAL, up_ask_sz_l2 REAL,
                    up_bid_l3 REAL, up_bid_sz_l3 REAL, up_ask_l3 REAL, up_ask_sz_l3 REAL,
                    down_bid_l1 REAL, down_bid_sz_l1 REAL, down_ask_l1 REAL, down_ask_sz_l1 REAL,
                    down_bid_l2 REAL, down_bid_sz_l2 REAL, down_ask_l2 REAL, down_ask_sz_l2 REAL,
                    down_bid_l3 REAL, down_bid_sz_l3 REAL, down_ask_l3 REAL, down_ask_sz_l3 REAL,
                    sum_bid_l1 REAL,
                    sum_ask_l1 REAL,
                    pair_bid_capacity_l1 REAL,
                    pair_ask_capacity_l1 REAL
                );

                CREATE INDEX IF NOT EXISTS idx_pair_bbo_slug_time
                    ON pair_bbo_snapshots(slug, received_at DESC);

                CREATE TABLE IF NOT EXISTS target_touch_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL,
                    condition_id TEXT,
                    token_id TEXT NOT NULL,
                    outcome_name TEXT,
                    side TEXT NOT NULL CHECK(side IN ('bid','ask')),
                    target_price REAL NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    start_event_ts_ms INTEGER,
                    end_event_ts_ms INTEGER,
                    start_best_price REAL,
                    max_size_at_touch REAL,
                    last_size_at_touch REAL,
                    num_updates INTEGER NOT NULL DEFAULT 0,
                    ws_trade_events_during INTEGER NOT NULL DEFAULT 0,
                    ws_trade_volume_during REAL NOT NULL DEFAULT 0,
                    data_trades_during INTEGER NOT NULL DEFAULT 0,
                    data_trade_volume_during REAL NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'open'
                );

                CREATE INDEX IF NOT EXISTS idx_target_touch_lookup
                    ON target_touch_events(slug, token_id, side, target_price, started_at DESC);

                CREATE TABLE IF NOT EXISTS second_bars_micro (
                    slug TEXT NOT NULL,
                    condition_id TEXT,
                    sec_epoch INTEGER NOT NULL,
                    phase TEXT,
                    time_to_start_sec REAL,
                    ws_messages INTEGER NOT NULL DEFAULT 0,
                    ws_price_changes INTEGER NOT NULL DEFAULT 0,
                    ws_last_trade_events INTEGER NOT NULL DEFAULT 0,
                    ws_last_trade_volume REAL NOT NULL DEFAULT 0,
                    data_trades INTEGER NOT NULL DEFAULT 0,
                    data_trade_volume REAL NOT NULL DEFAULT 0,
                    up_best_bid REAL,
                    up_best_ask REAL,
                    up_bid_sz_l1 REAL,
                    up_ask_sz_l1 REAL,
                    down_best_bid REAL,
                    down_best_ask REAL,
                    down_bid_sz_l1 REAL,
                    down_ask_sz_l1 REAL,
                    sum_bid_l1 REAL,
                    sum_ask_l1 REAL,
                    PRIMARY KEY (slug, sec_epoch)
                );

                CREATE INDEX IF NOT EXISTS idx_second_bars_slug_sec
                    ON second_bars_micro(slug, sec_epoch DESC);

                CREATE TABLE IF NOT EXISTS manual_strategy_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_group_id TEXT,
                    slug TEXT NOT NULL,
                    condition_id TEXT,
                    outcome_name TEXT,
                    token_id TEXT,
                    side TEXT,
                    price REAL,
                    size REAL,
                    placed_at TEXT,
                    filled_at TEXT,
                    canceled_at TEXT,
                    status TEXT,
                    maker INTEGER,
                    fee_or_rebate REAL,
                    note TEXT,
                    raw_json TEXT
                );

                CREATE TABLE IF NOT EXISTS collector_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self.conn.commit()

    def start_run(self, slug: str) -> int:
        with self.lock:
            cur = self.conn.execute(
                "INSERT INTO collector_runs (slug, started_at, status) VALUES (?, ?, ?)",
                (slug, utc_now_iso(), "running"),
            )
            self.conn.commit()
            return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, note: str = "") -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE collector_runs SET finished_at = ?, status = ?, note = ? WHERE id = ?",
                (utc_now_iso(), status, note, run_id),
            )
            self.conn.commit()

    def set_state(self, key: str, value: str) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO collector_state (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, value, utc_now_iso()),
            )
            self.conn.commit()

    def get_state(self, key: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT value FROM collector_state WHERE key = ?", (key,)
            ).fetchone()
            return None if row is None else str(row[0])

    def save_market_refresh(self, market: Dict[str, Any]) -> List[str]:
        fetched_at = utc_now_iso()
        with self.lock:
            insert_market(self.conn, market, fetched_at)
            for ev in market.get("events") or []:
                if isinstance(ev, dict):
                    insert_event(self.conn, market["slug"], ev, fetched_at)
            token_ids = insert_tokens(self.conn, market, fetched_at)
            self.conn.execute(
                """
                INSERT INTO market_state_snapshots (
                    slug, condition_id, fetched_at, active, closed, accepting_orders,
                    best_bid, best_ask, last_trade_price, spread, volume, volume_num, volume_clob,
                    liquidity, liquidity_num, liquidity_clob, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    market.get("slug"),
                    market.get("conditionId"),
                    fetched_at,
                    int(bool(market.get("active"))),
                    int(bool(market.get("closed"))),
                    int(bool(market.get("acceptingOrders"))),
                    to_float(market.get("bestBid")),
                    to_float(market.get("bestAsk")),
                    to_float(market.get("lastTradePrice")),
                    to_float(market.get("spread")),
                    to_float(market.get("volume")),
                    to_float(market.get("volumeNum")),
                    to_float(market.get("volumeClob")),
                    to_float(market.get("liquidity")),
                    to_float(market.get("liquidityNum")),
                    to_float(market.get("liquidityClob")),
                    json.dumps(market, ensure_ascii=True, separators=(",", ":")),
                ),
            )
            self.conn.commit()
            return token_ids

    def save_rest_orderbook_snapshot(
        self, slug: str, condition_id: str, token_id: str, book: Dict[str, Any]
    ) -> None:
        with self.lock:
            insert_orderbook_snapshot(
                self.conn, slug, condition_id, token_id, book, utc_now_iso()
            )
            self.conn.commit()

    def save_trades(self, trades: Iterable[Dict[str, Any]]) -> int:
        with self.lock:
            n = insert_trades(self.conn, trades, utc_now_iso())
            self.conn.commit()
            return n

    def save_pair_bbo_snapshot(self, row: Dict[str, Any]) -> None:
        cols = [
            "slug", "condition_id", "event_ts_ms", "received_at", "phase", "time_to_start_sec",
            "up_token_id", "down_token_id",
            "up_bid_l1", "up_bid_sz_l1", "up_ask_l1", "up_ask_sz_l1",
            "up_bid_l2", "up_bid_sz_l2", "up_ask_l2", "up_ask_sz_l2",
            "up_bid_l3", "up_bid_sz_l3", "up_ask_l3", "up_ask_sz_l3",
            "down_bid_l1", "down_bid_sz_l1", "down_ask_l1", "down_ask_sz_l1",
            "down_bid_l2", "down_bid_sz_l2", "down_ask_l2", "down_ask_sz_l2",
            "down_bid_l3", "down_bid_sz_l3", "down_ask_l3", "down_ask_sz_l3",
            "sum_bid_l1", "sum_ask_l1", "pair_bid_capacity_l1", "pair_ask_capacity_l1",
        ]
        with self.lock:
            self.conn.execute(
                f"INSERT INTO pair_bbo_snapshots ({','.join(cols)}) VALUES ({','.join(['?'] * len(cols))})",
                tuple(row.get(c) for c in cols),
            )
            self.conn.commit()

    def upsert_second_bar(self, row: Dict[str, Any]) -> None:
        cols = [
            "slug", "condition_id", "sec_epoch", "phase", "time_to_start_sec",
            "ws_messages", "ws_price_changes", "ws_last_trade_events", "ws_last_trade_volume",
            "data_trades", "data_trade_volume",
            "up_best_bid", "up_best_ask", "up_bid_sz_l1", "up_ask_sz_l1",
            "down_best_bid", "down_best_ask", "down_bid_sz_l1", "down_ask_sz_l1",
            "sum_bid_l1", "sum_ask_l1",
        ]
        with self.lock:
            self.conn.execute(
                f"""
                INSERT INTO second_bars_micro ({','.join(cols)}) VALUES ({','.join(['?'] * len(cols))})
                ON CONFLICT(slug, sec_epoch) DO UPDATE SET
                    condition_id=excluded.condition_id,
                    phase=excluded.phase,
                    time_to_start_sec=excluded.time_to_start_sec,
                    ws_messages=excluded.ws_messages,
                    ws_price_changes=excluded.ws_price_changes,
                    ws_last_trade_events=excluded.ws_last_trade_events,
                    ws_last_trade_volume=excluded.ws_last_trade_volume,
                    data_trades=excluded.data_trades,
                    data_trade_volume=excluded.data_trade_volume,
                    up_best_bid=excluded.up_best_bid,
                    up_best_ask=excluded.up_best_ask,
                    up_bid_sz_l1=excluded.up_bid_sz_l1,
                    up_ask_sz_l1=excluded.up_ask_sz_l1,
                    down_best_bid=excluded.down_best_bid,
                    down_best_ask=excluded.down_best_ask,
                    down_bid_sz_l1=excluded.down_bid_sz_l1,
                    down_ask_sz_l1=excluded.down_ask_sz_l1,
                    sum_bid_l1=excluded.sum_bid_l1,
                    sum_ask_l1=excluded.sum_ask_l1
                """,
                tuple(row.get(c) for c in cols),
            )
            self.conn.commit()

    def insert_target_touch_event(self, row: Dict[str, Any]) -> int:
        cols = [
            "slug", "condition_id", "token_id", "outcome_name", "side", "target_price",
            "started_at", "ended_at", "start_event_ts_ms", "end_event_ts_ms", "start_best_price",
            "max_size_at_touch", "last_size_at_touch", "num_updates",
            "ws_trade_events_during", "ws_trade_volume_during",
            "data_trades_during", "data_trade_volume_during", "status",
        ]
        with self.lock:
            cur = self.conn.execute(
                f"INSERT INTO target_touch_events ({','.join(cols)}) VALUES ({','.join(['?'] * len(cols))})",
                tuple(row.get(c) for c in cols),
            )
            self.conn.commit()
            return int(cur.lastrowid)

    def update_target_touch_event(self, touch_id: int, row: Dict[str, Any]) -> None:
        cols = [
            "ended_at", "end_event_ts_ms", "max_size_at_touch", "last_size_at_touch", "num_updates",
            "ws_trade_events_during", "ws_trade_volume_during",
            "data_trades_during", "data_trade_volume_during", "status",
        ]
        with self.lock:
            self.conn.execute(
                f"UPDATE target_touch_events SET {','.join([c + '=?' for c in cols])} WHERE id = ?",
                tuple(row.get(c) for c in cols) + (touch_id,),
            )
            self.conn.commit()

    def save_ws_message(self, slug: str, condition_id: str, msg: Dict[str, Any]) -> None:
        received_at = utc_now_iso()
        event_type = msg.get("event_type")
        asset_id = msg.get("asset_id")
        if asset_id is None and isinstance(msg.get("price_changes"), list) and msg["price_changes"]:
            asset_id = msg["price_changes"][0].get("asset_id")
        event_ts = msg.get("timestamp")
        event_ts_ms = int(event_ts) if str(event_ts).isdigit() else None
        raw = json.dumps(msg, ensure_ascii=True, separators=(",", ":"))

        with self.lock:
            cur = self.conn.execute(
                """
                INSERT INTO ws_messages (
                    slug, condition_id, event_type, asset_id, event_timestamp_ms, received_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (slug, condition_id, event_type, asset_id, event_ts_ms, received_at, raw),
            )
            ws_id = int(cur.lastrowid)

            if event_type == "price_change":
                for i, ch in enumerate(msg.get("price_changes") or []):
                    self.conn.execute(
                        """
                        INSERT INTO ws_price_changes (
                            ws_message_id, change_index, market, asset_id, side, price, size, level_hash, best_bid, best_ask
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            ws_id,
                            i,
                            msg.get("market"),
                            ch.get("asset_id"),
                            ch.get("side"),
                            to_float(ch.get("price")),
                            to_float(ch.get("size")),
                            ch.get("hash"),
                            to_float(ch.get("best_bid")),
                            to_float(ch.get("best_ask")),
                        ),
                    )

            elif event_type == "last_trade_price":
                self.conn.execute(
                    """
                    INSERT INTO ws_last_trade_events (
                        ws_message_id, market, asset_id, side, price, size, fee_rate_bps
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ws_id,
                        msg.get("market"),
                        msg.get("asset_id"),
                        msg.get("side"),
                        to_float(msg.get("price")),
                        to_float(msg.get("size")),
                        to_float(msg.get("fee_rate_bps")),
                    ),
                )

            elif event_type == "best_bid_ask":
                self.conn.execute(
                    """
                    INSERT INTO ws_best_bid_ask_events (
                        ws_message_id, market, asset_id, best_bid, best_ask, spread
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ws_id,
                        msg.get("market"),
                        msg.get("asset_id"),
                        to_float(msg.get("best_bid")),
                        to_float(msg.get("best_ask")),
                        to_float(msg.get("spread")),
                    ),
                )

            elif event_type == "tick_size_change":
                self.conn.execute(
                    """
                    INSERT INTO ws_tick_size_changes (
                        ws_message_id, market, asset_id, old_tick_size, new_tick_size
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        ws_id,
                        msg.get("market"),
                        msg.get("asset_id"),
                        to_float(msg.get("old_tick_size")),
                        to_float(msg.get("new_tick_size")),
                    ),
                )

            elif event_type == "book":
                self.conn.execute(
                    """
                    INSERT INTO ws_book_messages (
                        ws_message_id, market, asset_id, book_hash, tick_size, min_order_size, last_trade_price
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ws_id,
                        msg.get("market"),
                        msg.get("asset_id"),
                        msg.get("hash"),
                        to_float(msg.get("tick_size")),
                        to_float(msg.get("min_order_size")),
                        to_float(msg.get("last_trade_price")),
                    ),
                )
                for src_side, dst_side in (("bids", "bid"), ("asks", "ask"), ("buys", "bid"), ("sells", "ask")):
                    levels = msg.get(src_side)
                    if not isinstance(levels, list):
                        continue
                    for i, lvl in enumerate(levels):
                        self.conn.execute(
                            """
                            INSERT OR REPLACE INTO ws_book_levels (ws_message_id, side, level_index, price, size)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                ws_id,
                                dst_side,
                                i,
                                float(lvl["price"]),
                                float(lvl["size"]),
                            ),
                        )

            self.conn.commit()

    def counts_snapshot(self, slug: str) -> Dict[str, int]:
        with self.lock:
            cur = self.conn.cursor()
            return {
                "ws_messages": cur.execute(
                    "SELECT COUNT(*) FROM ws_messages WHERE slug = ?", (slug,)
                ).fetchone()[0],
                "price_changes": cur.execute(
                    """
                    SELECT COUNT(*) FROM ws_price_changes
                    WHERE ws_message_id IN (SELECT id FROM ws_messages WHERE slug = ?)
                    """,
                    (slug,),
                ).fetchone()[0],
                "ws_last_trade_events": cur.execute(
                    """
                    SELECT COUNT(*) FROM ws_last_trade_events
                    WHERE ws_message_id IN (SELECT id FROM ws_messages WHERE slug = ?)
                    """,
                    (slug,),
                ).fetchone()[0],
                "trades": cur.execute(
                    "SELECT COUNT(*) FROM trades WHERE slug = ?", (slug,)
                ).fetchone()[0],
                "market_state_snapshots": cur.execute(
                    "SELECT COUNT(*) FROM market_state_snapshots WHERE slug = ?", (slug,)
                ).fetchone()[0],
                "pair_bbo": cur.execute(
                    "SELECT COUNT(*) FROM pair_bbo_snapshots WHERE slug = ?", (slug,)
                ).fetchone()[0],
                "touch_events": cur.execute(
                    "SELECT COUNT(*) FROM target_touch_events WHERE slug = ?", (slug,)
                ).fetchone()[0],
            }

    def target_touch_summary(self, slug: str) -> List[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                """
                SELECT outcome_name, side, target_price,
                       COUNT(*) AS n_touches,
                       AVG((julianday(COALESCE(ended_at, started_at)) - julianday(started_at)) * 86400.0) AS avg_dur_sec,
                       AVG(max_size_at_touch) AS avg_max_size,
                       AVG(data_trade_volume_during) AS avg_data_trade_vol_during,
                       SUM(CASE WHEN data_trades_during > 0 OR ws_trade_events_during > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_with_trade
                FROM target_touch_events
                WHERE slug = ? AND status = 'closed'
                GROUP BY outcome_name, side, target_price
                ORDER BY outcome_name, side, target_price
                """,
                (slug,),
            ).fetchall()


def normalize_ws_payload(raw_text: str) -> List[Dict[str, Any]]:
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        return [{"event_type": "raw_text", "payload": raw_text}]

    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return [{"event_type": "unknown_payload", "payload": data}]


def parse_iso_to_epoch_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return None


def fetch_recent_trades_until_seen(
    condition_id: str,
    seen_keys: Set[str],
    page_limit: int,
    max_pages: int,
) -> Tuple[List[Dict[str, Any]], int]:
    from fetch_polymarket_market_to_db import trade_uid

    new_rows: List[Dict[str, Any]] = []
    pages = 0
    offset = 0
    seen_hit = False
    while pages < max_pages and not seen_hit:
        batch = http_get_json(
            f"{DATA_BASE}/trades",
            {"market": condition_id, "limit": page_limit, "offset": offset},
        )
        if not isinstance(batch, list) or not batch:
            break
        pages += 1
        for tr in batch:
            k = trade_uid(tr)
            if k in seen_keys:
                seen_hit = True
                continue
            new_rows.append(tr)
        if len(batch) < page_limit:
            break
        offset += page_limit
        time.sleep(0.1)
    return new_rows, pages


class ContinuousCollector:
    def __init__(
        self,
        slug: str,
        db_path: str,
        gamma_interval: float,
        trades_backfill_interval: float,
        rest_book_interval: float,
        trades_page_limit: int,
        trades_backfill_max_pages: int,
        print_trades: bool,
        target_prices: List[float],
        print_fill_proxy: bool,
    ):
        self.slug = slug
        self.db = CollectorDB(db_path)
        self.gamma_interval = gamma_interval
        self.trades_backfill_interval = trades_backfill_interval
        self.rest_book_interval = rest_book_interval
        self.trades_page_limit = trades_page_limit
        self.trades_backfill_max_pages = trades_backfill_max_pages
        self.print_trades = print_trades
        self.target_prices = sorted(set(round(float(x), 4) for x in target_prices))
        self.print_fill_proxy = print_fill_proxy
        self.stop_event = threading.Event()
        self.run_id = self.db.start_run(slug)

        self.market: Optional[Dict[str, Any]] = None
        self.condition_id: Optional[str] = None
        self.token_ids: List[str] = []
        self.token_to_outcome: Dict[str, str] = {}
        self.outcome_to_token: Dict[str, str] = {}
        self.event_start_epoch: Optional[float] = None
        self.event_end_epoch: Optional[float] = None
        self.seen_trade_keys: Set[str] = set()
        self.trade_seen_lock = threading.Lock()

        self.book_state: Dict[str, Dict[str, Any]] = {}
        self.book_lock = threading.Lock()
        self.last_pair_snapshot_emit_ms = 0
        self.open_touch_states: Dict[Tuple[str, str, float], Dict[str, Any]] = {}
        self.second_bars: Dict[int, Dict[str, Any]] = {}
        self.second_bars_lock = threading.Lock()

        self.ws_conn = None
        self.ws_thread: Optional[threading.Thread] = None
        self.worker_threads: List[threading.Thread] = []

    def _print_trade(self, tr: Dict[str, Any], source: str) -> None:
        if not self.print_trades:
            return
        ts = tr.get("timestamp")
        try:
            ts_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat() if ts is not None else "NA"
        except Exception:
            ts_iso = str(ts)
        print(
            "[trade]",
            f"src={source}",
            f"ts={ts_iso}",
            f"outcome={tr.get('outcome')}",
            f"side={tr.get('side')}",
            f"price={tr.get('price')}",
            f"size={tr.get('size')}",
            f"asset={tr.get('asset')}",
            f"tx={tr.get('transactionHash')}",
            flush=True,
        )

    def _set_market_context(self, market: Dict[str, Any]) -> None:
        ev0 = (market.get("events") or [{}])[0] if market.get("events") else {}
        self.event_start_epoch = parse_iso_to_epoch_seconds(
            market.get("eventStartTime") or ev0.get("startTime") or market.get("startDate") or ev0.get("startDate")
        )
        self.event_end_epoch = parse_iso_to_epoch_seconds(
            market.get("endDate") or ev0.get("endDate")
        )
        token_ids = [str(x) for x in parse_json_maybe(market.get("clobTokenIds"), [])]
        outcomes = [str(x) for x in parse_json_maybe(market.get("outcomes"), [])]
        self.token_to_outcome = {}
        self.outcome_to_token = {}
        for i, token_id in enumerate(token_ids):
            outcome = outcomes[i] if i < len(outcomes) else f"outcome_{i}"
            self.token_to_outcome[token_id] = outcome
            self.outcome_to_token[outcome.lower()] = token_id

    def _phase_info(self, now_epoch: Optional[float] = None) -> Tuple[str, Optional[float]]:
        now_epoch = now_epoch if now_epoch is not None else time.time()
        if self.event_start_epoch is None:
            return "unknown", None
        d = self.event_start_epoch - now_epoch
        if d > 0:
            return "pre_open", d
        if d > -30:
            return "first_30s", d
        return "live", d

    def market_end_reached(self, grace_seconds: float = 0.0) -> bool:
        if self.event_end_epoch is None:
            return False
        return time.time() >= (self.event_end_epoch + grace_seconds)

    def _ensure_book_entry(self, token_id: str) -> Dict[str, Any]:
        if token_id not in self.book_state:
            self.book_state[token_id] = {
                "bids": {},  # price -> size
                "asks": {},
                "last_event_ts_ms": None,
            }
        return self.book_state[token_id]

    def _sorted_levels(self, levels_map: Dict[float, float], side: str, depth: int = 3) -> List[Tuple[float, float]]:
        items = [(p, s) for p, s in levels_map.items() if s > 0]
        items.sort(key=lambda x: x[0], reverse=(side == "bid"))
        return items[:depth]

    def _best_price_size(self, token_id: str, side: str) -> Tuple[Optional[float], Optional[float]]:
        st = self.book_state.get(token_id)
        if not st:
            return None, None
        levels = self._sorted_levels(st["bids" if side == "bid" else "asks"], side, depth=1)
        if not levels:
            return None, None
        return levels[0]

    def _load_book_snapshot_into_state(self, msg: Dict[str, Any]) -> None:
        token_id = str(msg.get("asset_id") or "")
        if not token_id:
            return
        st = self._ensure_book_entry(token_id)
        bids: Dict[float, float] = {}
        asks: Dict[float, float] = {}
        for src_side, dst in (("bids", bids), ("buys", bids), ("asks", asks), ("sells", asks)):
            if isinstance(msg.get(src_side), list):
                for lvl in msg.get(src_side) or []:
                    p = to_float(lvl.get("price"))
                    s = to_float(lvl.get("size"))
                    if p is None or s is None:
                        continue
                    dst[round(p, 6)] = float(s)
        st["bids"] = bids
        st["asks"] = asks
        ts = msg.get("timestamp")
        st["last_event_ts_ms"] = int(ts) if str(ts).isdigit() else None

    def _apply_price_change_to_state(self, msg: Dict[str, Any]) -> int:
        count = 0
        for ch in msg.get("price_changes") or []:
            token_id = str(ch.get("asset_id") or "")
            side_raw = str(ch.get("side") or "").lower()
            price = to_float(ch.get("price"))
            size = to_float(ch.get("size"))
            if not token_id or side_raw not in ("buy", "sell", "bid", "ask") or price is None or size is None:
                continue
            side = "bids" if side_raw in ("buy", "bid") else "asks"
            st = self._ensure_book_entry(token_id)
            key = round(price, 6)
            if size <= 0:
                st[side].pop(key, None)
            else:
                st[side][key] = float(size)
            count += 1
            ts = msg.get("timestamp")
            st["last_event_ts_ms"] = int(ts) if str(ts).isdigit() else st.get("last_event_ts_ms")
        return count

    def _pair_snapshot_row(self, event_ts_ms: Optional[int]) -> Optional[Dict[str, Any]]:
        up_token = self.outcome_to_token.get("up")
        down_token = self.outcome_to_token.get("down")
        if not up_token or not down_token:
            return None
        if up_token not in self.book_state or down_token not in self.book_state:
            return None

        up_bids = self._sorted_levels(self.book_state[up_token]["bids"], "bid", 3)
        up_asks = self._sorted_levels(self.book_state[up_token]["asks"], "ask", 3)
        dn_bids = self._sorted_levels(self.book_state[down_token]["bids"], "bid", 3)
        dn_asks = self._sorted_levels(self.book_state[down_token]["asks"], "ask", 3)

        def lv(ls: List[Tuple[float, float]], idx: int) -> Tuple[Optional[float], Optional[float]]:
            return ls[idx] if idx < len(ls) else (None, None)

        phase, tts = self._phase_info()
        up_bid_l1, up_bid_sz_l1 = lv(up_bids, 0)
        up_ask_l1, up_ask_sz_l1 = lv(up_asks, 0)
        dn_bid_l1, dn_bid_sz_l1 = lv(dn_bids, 0)
        dn_ask_l1, dn_ask_sz_l1 = lv(dn_asks, 0)
        sum_bid_l1 = (up_bid_l1 + dn_bid_l1) if up_bid_l1 is not None and dn_bid_l1 is not None else None
        sum_ask_l1 = (up_ask_l1 + dn_ask_l1) if up_ask_l1 is not None and dn_ask_l1 is not None else None

        row = {
            "slug": self.slug,
            "condition_id": self.condition_id,
            "event_ts_ms": event_ts_ms,
            "received_at": utc_now_iso(),
            "phase": phase,
            "time_to_start_sec": tts,
            "up_token_id": up_token,
            "down_token_id": down_token,
            "sum_bid_l1": sum_bid_l1,
            "sum_ask_l1": sum_ask_l1,
            "pair_bid_capacity_l1": min(x for x in [up_bid_sz_l1, dn_bid_sz_l1] if x is not None) if up_bid_sz_l1 is not None and dn_bid_sz_l1 is not None else None,
            "pair_ask_capacity_l1": min(x for x in [up_ask_sz_l1, dn_ask_sz_l1] if x is not None) if up_ask_sz_l1 is not None and dn_ask_sz_l1 is not None else None,
        }
        for prefix, bids, asks in (("up", up_bids, up_asks), ("down", dn_bids, dn_asks)):
            for n in (1, 2, 3):
                bp, bs = lv(bids, n - 1)
                ap, a_s = lv(asks, n - 1)
                row[f"{prefix}_bid_l{n}"] = bp
                row[f"{prefix}_bid_sz_l{n}"] = bs
                row[f"{prefix}_ask_l{n}"] = ap
                row[f"{prefix}_ask_sz_l{n}"] = a_s
        return row

    def _update_second_bar(
        self,
        *,
        event_ts_ms: Optional[int],
        ws_messages: int = 0,
        ws_price_changes: int = 0,
        ws_last_trade_events: int = 0,
        ws_last_trade_volume: float = 0.0,
        data_trades: int = 0,
        data_trade_volume: float = 0.0,
    ) -> None:
        sec_epoch = int(((event_ts_ms or int(time.time() * 1000)) // 1000))
        phase, tts = self._phase_info(sec_epoch)
        with self.second_bars_lock:
            bar = self.second_bars.setdefault(
                sec_epoch,
                {
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "sec_epoch": sec_epoch,
                    "phase": phase,
                    "time_to_start_sec": tts,
                    "ws_messages": 0,
                    "ws_price_changes": 0,
                    "ws_last_trade_events": 0,
                    "ws_last_trade_volume": 0.0,
                    "data_trades": 0,
                    "data_trade_volume": 0.0,
                },
            )
            bar["condition_id"] = self.condition_id
            bar["phase"] = phase
            bar["time_to_start_sec"] = tts
            bar["ws_messages"] += ws_messages
            bar["ws_price_changes"] += ws_price_changes
            bar["ws_last_trade_events"] += ws_last_trade_events
            bar["ws_last_trade_volume"] += float(ws_last_trade_volume or 0.0)
            bar["data_trades"] += data_trades
            bar["data_trade_volume"] += float(data_trade_volume or 0.0)
            pair = self._pair_snapshot_row(event_ts_ms)
            if pair:
                bar["up_best_bid"] = pair.get("up_bid_l1")
                bar["up_best_ask"] = pair.get("up_ask_l1")
                bar["up_bid_sz_l1"] = pair.get("up_bid_sz_l1")
                bar["up_ask_sz_l1"] = pair.get("up_ask_sz_l1")
                bar["down_best_bid"] = pair.get("down_bid_l1")
                bar["down_best_ask"] = pair.get("down_ask_l1")
                bar["down_bid_sz_l1"] = pair.get("down_bid_sz_l1")
                bar["down_ask_sz_l1"] = pair.get("down_ask_sz_l1")
                bar["sum_bid_l1"] = pair.get("sum_bid_l1")
                bar["sum_ask_l1"] = pair.get("sum_ask_l1")
            self.db.upsert_second_bar(bar)

    def _touch_active(self, best_price: Optional[float], target: float, side: str) -> bool:
        if best_price is None:
            return False
        best_price = round(best_price, 2)
        target = round(target, 2)
        if side == "bid":
            return best_price >= target
        return best_price <= target

    def _update_target_touches_for_token(self, token_id: str, event_ts_ms: Optional[int]) -> None:
        outcome_name = self.token_to_outcome.get(token_id)
        if not outcome_name:
            return
        for side in ("bid", "ask"):
            best_price, best_size = self._best_price_size(token_id, side)
            for target in self.target_prices:
                key = (token_id, side, round(target, 2))
                is_active = self._touch_active(best_price, target, side)
                state = self.open_touch_states.get(key)
                if is_active and state is None:
                    row = {
                        "slug": self.slug,
                        "condition_id": self.condition_id,
                        "token_id": token_id,
                        "outcome_name": outcome_name,
                        "side": side,
                        "target_price": round(target, 2),
                        "started_at": utc_now_iso(),
                        "ended_at": None,
                        "start_event_ts_ms": event_ts_ms,
                        "end_event_ts_ms": None,
                        "start_best_price": best_price,
                        "max_size_at_touch": best_size or 0.0,
                        "last_size_at_touch": best_size or 0.0,
                        "num_updates": 1,
                        "ws_trade_events_during": 0,
                        "ws_trade_volume_during": 0.0,
                        "data_trades_during": 0,
                        "data_trade_volume_during": 0.0,
                        "status": "open",
                    }
                    touch_id = self.db.insert_target_touch_event(row)
                    self.open_touch_states[key] = {**row, "id": touch_id}
                elif is_active and state is not None:
                    state["num_updates"] += 1
                    state["last_size_at_touch"] = best_size or 0.0
                    state["max_size_at_touch"] = max(float(state.get("max_size_at_touch") or 0.0), float(best_size or 0.0))
                    self.db.update_target_touch_event(state["id"], state)
                elif (not is_active) and state is not None:
                    state["ended_at"] = utc_now_iso()
                    state["end_event_ts_ms"] = event_ts_ms
                    state["status"] = "closed"
                    self.db.update_target_touch_event(state["id"], state)
                    self.open_touch_states.pop(key, None)

    def _process_book_metrics(self, payload: Dict[str, Any]) -> None:
        event_type = payload.get("event_type")
        event_ts_ms = int(payload.get("timestamp")) if str(payload.get("timestamp")).isdigit() else None
        price_change_count = 0
        touched_tokens: Set[str] = set()

        with self.book_lock:
            if event_type == "book":
                self._load_book_snapshot_into_state(payload)
                if payload.get("asset_id"):
                    touched_tokens.add(str(payload["asset_id"]))
            elif event_type == "price_change":
                price_change_count = self._apply_price_change_to_state(payload)
                for ch in payload.get("price_changes") or []:
                    if ch.get("asset_id"):
                        touched_tokens.add(str(ch["asset_id"]))

            if event_type in ("book", "price_change", "best_bid_ask", "last_trade_price"):
                if event_type in ("book", "price_change"):
                    for token_id in touched_tokens:
                        self._update_target_touches_for_token(token_id, event_ts_ms)
                if event_type in ("book", "price_change") and event_ts_ms:
                    # Sample pair BBO frequently but not for every single delta; cap ~20Hz
                    if event_ts_ms - self.last_pair_snapshot_emit_ms >= 50:
                        pair = self._pair_snapshot_row(event_ts_ms)
                        if pair:
                            self.db.save_pair_bbo_snapshot(pair)
                            if self.print_fill_proxy:
                                print(
                                    "[pair]",
                                    f"tts={pair.get('time_to_start_sec'):.1f}" if pair.get("time_to_start_sec") is not None else "tts=NA",
                                    f"sum_bid_l1={pair.get('sum_bid_l1')}",
                                    f"sum_ask_l1={pair.get('sum_ask_l1')}",
                                    f"cap_bid_l1={pair.get('pair_bid_capacity_l1')}",
                                    flush=True,
                                )
                        self.last_pair_snapshot_emit_ms = event_ts_ms

        ws_last_trade_events = 1 if event_type == "last_trade_price" else 0
        ws_last_trade_volume = to_float(payload.get("size")) if event_type == "last_trade_price" else 0.0
        self._update_second_bar(
            event_ts_ms=event_ts_ms,
            ws_messages=1,
            ws_price_changes=price_change_count,
            ws_last_trade_events=ws_last_trade_events,
            ws_last_trade_volume=ws_last_trade_volume or 0.0,
        )

        if event_type == "last_trade_price" and payload.get("asset_id"):
            token_id = str(payload["asset_id"])
            for side in ("bid", "ask"):
                for target in self.target_prices:
                    key = (token_id, side, round(target, 2))
                    state = self.open_touch_states.get(key)
                    if state:
                        state["ws_trade_events_during"] = int(state.get("ws_trade_events_during", 0)) + 1
                        state["ws_trade_volume_during"] = float(state.get("ws_trade_volume_during", 0.0)) + float(ws_last_trade_volume or 0.0)
                        self.db.update_target_touch_event(state["id"], state)

    def _record_data_trade_metrics(self, trades: Iterable[Dict[str, Any]]) -> None:
        per_sec: Dict[int, Dict[str, float]] = defaultdict(lambda: {"n": 0, "vol": 0.0})
        for tr in trades:
            ts = tr.get("timestamp")
            if not isinstance(ts, (int, float)):
                continue
            sec = int(ts)
            size = float(to_float(tr.get("size")) or 0.0)
            per_sec[sec]["n"] += 1
            per_sec[sec]["vol"] += size

            token_id = str(tr.get("asset") or "")
            if not token_id:
                continue
            # Attribute data-api trade activity to currently open touch states for the same token.
            for side in ("bid", "ask"):
                for target in self.target_prices:
                    key = (token_id, side, round(target, 2))
                    state = self.open_touch_states.get(key)
                    if not state:
                        continue
                    state["data_trades_during"] = int(state.get("data_trades_during", 0)) + 1
                    state["data_trade_volume_during"] = float(state.get("data_trade_volume_during", 0.0)) + size
                    self.db.update_target_touch_event(state["id"], state)

        for sec, agg in per_sec.items():
            self._update_second_bar(
                event_ts_ms=sec * 1000,
                data_trades=int(agg["n"]),
                data_trade_volume=float(agg["vol"]),
            )

    def bootstrap(self) -> None:
        market = fetch_market_by_slug(self.slug)
        self.market = market
        self.condition_id = market.get("conditionId")
        self._set_market_context(market)
        self.token_ids = self.db.save_market_refresh(market)

        if self.condition_id:
            recent = http_get_json(
                f"{DATA_BASE}/trades",
                {"market": self.condition_id, "limit": 100, "offset": 0},
            )
            if isinstance(recent, list):
                self.db.save_trades(recent)
                for tr in sorted(recent, key=lambda x: (x.get("timestamp", 0), x.get("transactionHash", ""))):
                    self._print_trade(tr, "bootstrap")
                self._record_data_trade_metrics(recent)
                from fetch_polymarket_market_to_db import trade_uid

                with self.trade_seen_lock:
                    for tr in recent:
                        self.seen_trade_keys.add(trade_uid(tr))

        for token_id in self.token_ids:
            try:
                book = fetch_orderbook(token_id)
                self.db.save_rest_orderbook_snapshot(self.slug, self.condition_id or "", token_id, book)
                time.sleep(0.1)
            except Exception as exc:
                print(f"[rest-book bootstrap] {token_id}: {exc}", flush=True)

    def start(self) -> None:
        self.bootstrap()

        self.worker_threads = [
            threading.Thread(target=self.gamma_loop, name="gamma-loop", daemon=True),
            threading.Thread(target=self.trades_backfill_loop, name="trades-backfill", daemon=True),
            threading.Thread(target=self.rest_orderbook_loop, name="rest-book-loop", daemon=True),
            threading.Thread(target=self.report_loop, name="report-loop", daemon=True),
        ]
        for t in self.worker_threads:
            t.start()

        self.start_ws_loop_thread()

    def start_ws_loop_thread(self) -> None:
        self.ws_thread = threading.Thread(target=self.ws_supervisor_loop, name="ws-supervisor", daemon=True)
        self.ws_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.ws_conn is not None:
            try:
                self.ws_conn.close()
            except Exception:
                pass

    def join(self) -> None:
        for t in self.worker_threads:
            t.join(timeout=1.0)
        if self.ws_thread:
            self.ws_thread.join(timeout=2.0)

    def _close_open_touches(self) -> None:
        event_ts_ms = int(time.time() * 1000)
        for key, state in list(self.open_touch_states.items()):
            state["ended_at"] = utc_now_iso()
            state["end_event_ts_ms"] = event_ts_ms
            state["status"] = "closed"
            try:
                self.db.update_target_touch_event(state["id"], state)
            except Exception:
                pass
            self.open_touch_states.pop(key, None)

    def finalize(self, status: str, note: str = "") -> None:
        self._close_open_touches()
        self.db.finish_run(self.run_id, status, note)
        self.db.close()

    def gamma_loop(self) -> None:
        while not self.stop_event.wait(self.gamma_interval):
            try:
                market = fetch_market_by_slug(self.slug)
                self.market = market
                self.condition_id = market.get("conditionId")
                self._set_market_context(market)
                token_ids = self.db.save_market_refresh(market)
                if token_ids != self.token_ids:
                    self.token_ids = token_ids
                    print("[gamma] token_ids updated (market changed?)", flush=True)
                    if self.ws_conn:
                        try:
                            self.ws_conn.send(
                                json.dumps(
                                    {
                                        "assets_ids": self.token_ids,
                                        "operation": "subscribe",
                                        "custom_feature_enabled": True,
                                    }
                                )
                            )
                        except Exception as exc:
                            print(f"[ws] resubscribe failed: {exc}", flush=True)
            except Exception as exc:
                print(f"[gamma] error: {exc}", flush=True)

    def trades_backfill_loop(self) -> None:
        if not self.condition_id:
            return
        while not self.stop_event.wait(self.trades_backfill_interval):
            try:
                with self.trade_seen_lock:
                    seen = set(self.seen_trade_keys)
                new_trades, pages = fetch_recent_trades_until_seen(
                    self.condition_id,
                    seen,
                    self.trades_page_limit,
                    self.trades_backfill_max_pages,
                )
                if new_trades:
                    # Data API returns newest first; store oldest->newest for easier reading
                    new_trades_sorted = sorted(
                        new_trades, key=lambda x: (x.get("timestamp", 0), x.get("transactionHash", ""))
                    )
                    self.db.save_trades(new_trades_sorted)
                    for tr in new_trades_sorted:
                        self._print_trade(tr, "backfill")
                    self._record_data_trade_metrics(new_trades_sorted)
                    from fetch_polymarket_market_to_db import trade_uid

                    with self.trade_seen_lock:
                        for tr in new_trades:
                            self.seen_trade_keys.add(trade_uid(tr))
                    print(f"[trades-backfill] +{len(new_trades)} trades ({pages} pages)", flush=True)
            except Exception as exc:
                print(f"[trades-backfill] error: {exc}", flush=True)

    def rest_orderbook_loop(self) -> None:
        while not self.stop_event.wait(self.rest_book_interval):
            if not self.token_ids:
                continue
            for token_id in list(self.token_ids):
                if self.stop_event.is_set():
                    return
                try:
                    book = fetch_orderbook(token_id)
                    self.db.save_rest_orderbook_snapshot(
                        self.slug, self.condition_id or "", token_id, book
                    )
                except Exception as exc:
                    print(f"[rest-book] {token_id} error: {exc}", flush=True)
                time.sleep(0.15)

    def report_loop(self) -> None:
        while not self.stop_event.wait(10.0):
            stats = self.db.counts_snapshot(self.slug)
            print(
                "[stats]",
                " ".join(f"{k}={v}" for k, v in stats.items()),
                f"time={datetime.now().strftime('%H:%M:%S')}",
                flush=True,
            )

    def ws_supervisor_loop(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                self._run_ws_once()
                backoff = 1.0
            except Exception as exc:
                print(f"[ws] supervisor error: {exc}", flush=True)
            if self.stop_event.is_set():
                break
            print(f"[ws] reconnect in {backoff:.1f}s", flush=True)
            self.stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 20.0)

    def _run_ws_once(self) -> None:
        if not self.token_ids:
            raise RuntimeError("No token_ids available for websocket subscription")

        sub = {
            "assets_ids": self.token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        with ws_connect(WSS_MARKET_URL, open_timeout=10, close_timeout=3) as ws:
            self.ws_conn = ws
            ws.send(json.dumps(sub))
            print(f"[ws] subscribed to {len(self.token_ids)} token_ids", flush=True)
            while not self.stop_event.is_set():
                try:
                    message = ws.recv(timeout=5)
                except TimeoutError:
                    continue
                if message is None:
                    raise RuntimeError("WebSocket closed by server")
                for payload in normalize_ws_payload(message):
                    if payload.get("event_type") is None and payload.get("type") == "error":
                        print(f"[ws] error message: {payload}", flush=True)
                    self._process_book_metrics(payload)
                    self.db.save_ws_message(self.slug, self.condition_id or "", payload)
        self.ws_conn = None


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuous Polymarket collector (WS + REST) into SQLite")
    parser.add_argument("--slug", required=True, help="Market slug to collect")
    parser.add_argument("--db", default="polymarket_collector.db", help="SQLite DB path")
    parser.add_argument("--gamma-interval", type=float, default=15.0, help="Seconds between Gamma market refreshes")
    parser.add_argument("--trades-backfill-interval", type=float, default=5.0, help="Seconds between Data API trade backfills")
    parser.add_argument("--rest-book-interval", type=float, default=20.0, help="Seconds between REST CLOB book snapshots")
    parser.add_argument("--trades-page-limit", type=int, default=200, help="Data API page size for trade backfill")
    parser.add_argument("--trades-backfill-max-pages", type=int, default=10, help="Max pages per trade backfill cycle")
    parser.add_argument("--run-seconds", type=float, default=0.0, help="Optional auto-stop after N seconds (0 = forever)")
    parser.add_argument("--print-trades", action="store_true", help="Print each trade as it is saved")
    parser.add_argument("--target-prices", default="0.49,0.50,0.51", help="Comma-separated target prices for touch/fillability proxies")
    parser.add_argument("--print-fill-proxy", action="store_true", help="Print pair L1 proxy snapshots periodically")
    parser.add_argument("--auto-stop-at-market-end", action="store_true", help="Stop automatically when market endDate is reached")
    parser.add_argument("--market-end-grace-seconds", type=float, default=10.0, help="Extra seconds to collect after market endDate when auto-stop is enabled")
    args = parser.parse_args()

    target_prices = [float(x.strip()) for x in args.target_prices.split(",") if x.strip()]

    collector = ContinuousCollector(
        slug=args.slug,
        db_path=args.db,
        gamma_interval=args.gamma_interval,
        trades_backfill_interval=args.trades_backfill_interval,
        rest_book_interval=args.rest_book_interval,
        trades_page_limit=args.trades_page_limit,
        trades_backfill_max_pages=args.trades_backfill_max_pages,
        print_trades=args.print_trades,
        target_prices=target_prices,
        print_fill_proxy=args.print_fill_proxy,
    )

    stop_once = threading.Event()

    def _handle_signal(signum, frame):
        if stop_once.is_set():
            return
        stop_once.set()
        print(f"[main] signal {signum}, stopping...", flush=True)
        collector.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        collector.start()
        print(f"[main] collecting slug={args.slug} into {args.db}", flush=True)
        if collector.event_end_epoch is not None:
            print(
                f"[main] market_end_utc={datetime.fromtimestamp(collector.event_end_epoch, tz=timezone.utc).isoformat()} "
                f"auto_stop={args.auto_stop_at_market_end} grace_s={args.market_end_grace_seconds}",
                flush=True,
            )
        start = time.time()
        while not collector.stop_event.is_set():
            if args.run_seconds > 0 and (time.time() - start) >= args.run_seconds:
                print("[main] run-seconds reached, stopping...", flush=True)
                collector.stop()
                break
            if args.auto_stop_at_market_end and collector.market_end_reached(args.market_end_grace_seconds):
                print("[main] market end reached, stopping...", flush=True)
                collector.stop()
                break
            time.sleep(0.5)
        collector.join()
        for row in collector.db.target_touch_summary(args.slug):
            print(
                "[touch-summary]",
                f"outcome={row['outcome_name']}",
                f"side={row['side']}",
                f"target={row['target_price']}",
                f"n={row['n_touches']}",
                f"avg_dur_s={round(row['avg_dur_sec'] or 0.0, 3)}",
                f"avg_max_size={round(row['avg_max_size'] or 0.0, 3)}",
                f"pct_with_trade={round(100.0 * (row['pct_with_trade'] or 0.0), 1)}%",
                flush=True,
            )
        collector.finalize("success", "stopped normally")
    except KeyboardInterrupt:
        collector.stop()
        collector.join()
        collector.finalize("success", "keyboard interrupt")
    except Exception as exc:
        collector.stop()
        collector.join()
        collector.finalize("error", str(exc))
        raise


if __name__ == "__main__":
    main()
