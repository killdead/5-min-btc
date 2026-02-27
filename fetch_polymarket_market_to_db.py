#!/usr/bin/env python3
import argparse
import json
import sqlite3
import ssl
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20) -> Any:
    if params:
        query = urlencode(params)
        url = f"{url}?{query}"
    req = Request(
        url,
        headers={
            "User-Agent": "polymarket-market-export/1.0",
            "Accept": "application/json",
        },
    )
    attempts = 4
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            # Retry only transient HTTPs.
            if e.code in (408, 425, 429, 500, 502, 503, 504) and attempt < attempts:
                time.sleep(0.4 * attempt)
                continue
            raise RuntimeError(f"HTTP {e.code} for {url}: {body[:500]}")
        except (URLError, ssl.SSLError, TimeoutError, ConnectionResetError) as e:
            if attempt < attempts:
                time.sleep(0.4 * attempt)
                continue
            raise RuntimeError(f"Network error for {url}: {e}")


def parse_json_maybe(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return default
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return default
    return default


def to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            note TEXT
        );

        CREATE TABLE IF NOT EXISTS markets (
            slug TEXT PRIMARY KEY,
            market_id TEXT,
            condition_id TEXT,
            question TEXT,
            description TEXT,
            resolution_source TEXT,
            start_date TEXT,
            end_date TEXT,
            created_at TEXT,
            updated_at TEXT,
            accepting_orders_timestamp TEXT,
            active INTEGER,
            closed INTEGER,
            archived INTEGER,
            restricted INTEGER,
            accepting_orders INTEGER,
            enable_order_book INTEGER,
            order_price_min_tick_size REAL,
            order_min_size REAL,
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
            maker_base_fee INTEGER,
            taker_base_fee INTEGER,
            raw_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_events (
            event_id TEXT PRIMARY KEY,
            market_slug TEXT NOT NULL,
            event_slug TEXT,
            ticker TEXT,
            title TEXT,
            description TEXT,
            start_date TEXT,
            end_date TEXT,
            start_time TEXT,
            creation_date TEXT,
            created_at TEXT,
            updated_at TEXT,
            active INTEGER,
            closed INTEGER,
            archived INTEGER,
            restricted INTEGER,
            enable_order_book INTEGER,
            liquidity REAL,
            volume REAL,
            liquidity_clob REAL,
            open_interest REAL,
            raw_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (market_slug) REFERENCES markets(slug)
        );

        CREATE TABLE IF NOT EXISTS market_tokens (
            token_id TEXT PRIMARY KEY,
            market_slug TEXT NOT NULL,
            condition_id TEXT,
            outcome_index INTEGER,
            outcome_name TEXT,
            outcome_price REAL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (market_slug) REFERENCES markets(slug)
        );

        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_slug TEXT NOT NULL,
            token_id TEXT NOT NULL,
            market_condition_id TEXT,
            book_market TEXT,
            book_timestamp_ms INTEGER,
            book_hash TEXT,
            min_order_size REAL,
            tick_size REAL,
            neg_risk INTEGER,
            last_trade_price REAL,
            raw_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (market_slug) REFERENCES markets(slug),
            FOREIGN KEY (token_id) REFERENCES market_tokens(token_id)
        );

        CREATE TABLE IF NOT EXISTS orderbook_levels (
            snapshot_id INTEGER NOT NULL,
            side TEXT NOT NULL CHECK(side IN ('bid','ask')),
            level_index INTEGER NOT NULL,
            price REAL NOT NULL,
            size REAL NOT NULL,
            PRIMARY KEY (snapshot_id, side, level_index),
            FOREIGN KEY (snapshot_id) REFERENCES orderbook_snapshots(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS trades (
            trade_uid TEXT PRIMARY KEY,
            condition_id TEXT,
            slug TEXT,
            event_slug TEXT,
            title TEXT,
            asset TEXT,
            outcome TEXT,
            outcome_index INTEGER,
            side TEXT,
            size REAL,
            price REAL,
            timestamp INTEGER,
            timestamp_iso TEXT,
            transaction_hash TEXT,
            proxy_wallet TEXT,
            trader_name TEXT,
            pseudonym TEXT,
            bio TEXT,
            profile_image TEXT,
            profile_image_optimized TEXT,
            raw_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_trades_condition_time
            ON trades(condition_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_token_time
            ON orderbook_snapshots(token_id, book_timestamp_ms DESC);
        """
    )


def insert_market(conn: sqlite3.Connection, market: Dict[str, Any], fetched_at: str) -> None:
    conn.execute(
        """
        INSERT INTO markets (
            slug, market_id, condition_id, question, description, resolution_source,
            start_date, end_date, created_at, updated_at, accepting_orders_timestamp,
            active, closed, archived, restricted, accepting_orders, enable_order_book,
            order_price_min_tick_size, order_min_size, best_bid, best_ask, last_trade_price,
            spread, volume, volume_num, volume_clob, liquidity, liquidity_num, liquidity_clob,
            maker_base_fee, taker_base_fee, raw_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            market_id=excluded.market_id,
            condition_id=excluded.condition_id,
            question=excluded.question,
            description=excluded.description,
            resolution_source=excluded.resolution_source,
            start_date=excluded.start_date,
            end_date=excluded.end_date,
            created_at=excluded.created_at,
            updated_at=excluded.updated_at,
            accepting_orders_timestamp=excluded.accepting_orders_timestamp,
            active=excluded.active,
            closed=excluded.closed,
            archived=excluded.archived,
            restricted=excluded.restricted,
            accepting_orders=excluded.accepting_orders,
            enable_order_book=excluded.enable_order_book,
            order_price_min_tick_size=excluded.order_price_min_tick_size,
            order_min_size=excluded.order_min_size,
            best_bid=excluded.best_bid,
            best_ask=excluded.best_ask,
            last_trade_price=excluded.last_trade_price,
            spread=excluded.spread,
            volume=excluded.volume,
            volume_num=excluded.volume_num,
            volume_clob=excluded.volume_clob,
            liquidity=excluded.liquidity,
            liquidity_num=excluded.liquidity_num,
            liquidity_clob=excluded.liquidity_clob,
            maker_base_fee=excluded.maker_base_fee,
            taker_base_fee=excluded.taker_base_fee,
            raw_json=excluded.raw_json,
            fetched_at=excluded.fetched_at
        """,
        (
            market.get("slug"),
            str(market.get("id")) if market.get("id") is not None else None,
            market.get("conditionId"),
            market.get("question"),
            market.get("description"),
            market.get("resolutionSource"),
            market.get("startDate"),
            market.get("endDate"),
            market.get("createdAt"),
            market.get("updatedAt"),
            market.get("acceptingOrdersTimestamp"),
            int(bool(market.get("active"))),
            int(bool(market.get("closed"))),
            int(bool(market.get("archived"))),
            int(bool(market.get("restricted"))),
            int(bool(market.get("acceptingOrders"))),
            int(bool(market.get("enableOrderBook"))),
            to_float(market.get("orderPriceMinTickSize")),
            to_float(market.get("orderMinSize")),
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
            market.get("makerBaseFee"),
            market.get("takerBaseFee"),
            json.dumps(market, ensure_ascii=True, separators=(",", ":")),
            fetched_at,
        ),
    )


def insert_event(conn: sqlite3.Connection, market_slug: str, event: Dict[str, Any], fetched_at: str) -> None:
    event_id = str(event.get("id")) if event.get("id") is not None else None
    if not event_id:
        return
    conn.execute(
        """
        INSERT INTO market_events (
            event_id, market_slug, event_slug, ticker, title, description,
            start_date, end_date, start_time, creation_date, created_at, updated_at,
            active, closed, archived, restricted, enable_order_book,
            liquidity, volume, liquidity_clob, open_interest, raw_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            market_slug=excluded.market_slug,
            event_slug=excluded.event_slug,
            ticker=excluded.ticker,
            title=excluded.title,
            description=excluded.description,
            start_date=excluded.start_date,
            end_date=excluded.end_date,
            start_time=excluded.start_time,
            creation_date=excluded.creation_date,
            created_at=excluded.created_at,
            updated_at=excluded.updated_at,
            active=excluded.active,
            closed=excluded.closed,
            archived=excluded.archived,
            restricted=excluded.restricted,
            enable_order_book=excluded.enable_order_book,
            liquidity=excluded.liquidity,
            volume=excluded.volume,
            liquidity_clob=excluded.liquidity_clob,
            open_interest=excluded.open_interest,
            raw_json=excluded.raw_json,
            fetched_at=excluded.fetched_at
        """,
        (
            event_id,
            market_slug,
            event.get("slug"),
            event.get("ticker"),
            event.get("title"),
            event.get("description"),
            event.get("startDate"),
            event.get("endDate"),
            event.get("startTime"),
            event.get("creationDate"),
            event.get("createdAt"),
            event.get("updatedAt"),
            int(bool(event.get("active"))),
            int(bool(event.get("closed"))),
            int(bool(event.get("archived"))),
            int(bool(event.get("restricted"))),
            int(bool(event.get("enableOrderBook"))),
            to_float(event.get("liquidity")),
            to_float(event.get("volume")),
            to_float(event.get("liquidityClob")),
            to_float(event.get("openInterest")),
            json.dumps(event, ensure_ascii=True, separators=(",", ":")),
            fetched_at,
        ),
    )


def insert_tokens(conn: sqlite3.Connection, market: Dict[str, Any], fetched_at: str) -> List[str]:
    market_slug = market["slug"]
    condition_id = market.get("conditionId")
    token_ids = parse_json_maybe(market.get("clobTokenIds"), [])
    outcomes = parse_json_maybe(market.get("outcomes"), [])
    outcome_prices = parse_json_maybe(market.get("outcomePrices"), [])
    for idx, token_id in enumerate(token_ids):
        outcome = outcomes[idx] if idx < len(outcomes) else None
        outcome_price = to_float(outcome_prices[idx]) if idx < len(outcome_prices) else None
        conn.execute(
            """
            INSERT INTO market_tokens (
                token_id, market_slug, condition_id, outcome_index, outcome_name, outcome_price, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(token_id) DO UPDATE SET
                market_slug=excluded.market_slug,
                condition_id=excluded.condition_id,
                outcome_index=excluded.outcome_index,
                outcome_name=excluded.outcome_name,
                outcome_price=excluded.outcome_price,
                fetched_at=excluded.fetched_at
            """,
            (str(token_id), market_slug, condition_id, idx, outcome, outcome_price, fetched_at),
        )
    return [str(x) for x in token_ids]


def insert_orderbook_snapshot(
    conn: sqlite3.Connection,
    market_slug: str,
    condition_id: Optional[str],
    token_id: str,
    book: Dict[str, Any],
    fetched_at: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO orderbook_snapshots (
            market_slug, token_id, market_condition_id, book_market, book_timestamp_ms, book_hash,
            min_order_size, tick_size, neg_risk, last_trade_price, raw_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            market_slug,
            token_id,
            condition_id,
            book.get("market"),
            int(book["timestamp"]) if str(book.get("timestamp", "")).isdigit() else None,
            book.get("hash"),
            to_float(book.get("min_order_size")),
            to_float(book.get("tick_size")),
            int(bool(book.get("neg_risk"))),
            to_float(book.get("last_trade_price")),
            json.dumps(book, ensure_ascii=True, separators=(",", ":")),
            fetched_at,
        ),
    )
    snapshot_id = int(cur.lastrowid)

    for side_src, side_dst in (("bids", "bid"), ("asks", "ask")):
        levels = book.get(side_src) or []
        for i, level in enumerate(levels):
            conn.execute(
                """
                INSERT INTO orderbook_levels (snapshot_id, side, level_index, price, size)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    side_dst,
                    i,
                    float(level["price"]),
                    float(level["size"]),
                ),
            )
    return snapshot_id


def trade_uid(tr: Dict[str, Any]) -> str:
    parts = [
        str(tr.get("transactionHash") or ""),
        str(tr.get("asset") or ""),
        str(tr.get("timestamp") or ""),
        str(tr.get("price") or ""),
        str(tr.get("size") or ""),
        str(tr.get("side") or ""),
    ]
    return "|".join(parts)


def insert_trades(conn: sqlite3.Connection, trades: Iterable[Dict[str, Any]], fetched_at: str) -> int:
    count = 0
    for tr in trades:
        ts = tr.get("timestamp")
        ts_iso = None
        if isinstance(ts, (int, float)):
            ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO trades (
                trade_uid, condition_id, slug, event_slug, title, asset, outcome, outcome_index,
                side, size, price, timestamp, timestamp_iso, transaction_hash, proxy_wallet,
                trader_name, pseudonym, bio, profile_image, profile_image_optimized, raw_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_uid) DO UPDATE SET
                condition_id=excluded.condition_id,
                slug=excluded.slug,
                event_slug=excluded.event_slug,
                title=excluded.title,
                asset=excluded.asset,
                outcome=excluded.outcome,
                outcome_index=excluded.outcome_index,
                side=excluded.side,
                size=excluded.size,
                price=excluded.price,
                timestamp=excluded.timestamp,
                timestamp_iso=excluded.timestamp_iso,
                transaction_hash=excluded.transaction_hash,
                proxy_wallet=excluded.proxy_wallet,
                trader_name=excluded.trader_name,
                pseudonym=excluded.pseudonym,
                bio=excluded.bio,
                profile_image=excluded.profile_image,
                profile_image_optimized=excluded.profile_image_optimized,
                raw_json=excluded.raw_json,
                fetched_at=excluded.fetched_at
            """,
            (
                trade_uid(tr),
                tr.get("conditionId"),
                tr.get("slug"),
                tr.get("eventSlug"),
                tr.get("title"),
                str(tr.get("asset")) if tr.get("asset") is not None else None,
                tr.get("outcome"),
                tr.get("outcomeIndex"),
                tr.get("side"),
                to_float(tr.get("size")),
                to_float(tr.get("price")),
                tr.get("timestamp"),
                ts_iso,
                tr.get("transactionHash"),
                tr.get("proxyWallet"),
                tr.get("name"),
                tr.get("pseudonym"),
                tr.get("bio"),
                tr.get("profileImage"),
                tr.get("profileImageOptimized"),
                json.dumps(tr, ensure_ascii=True, separators=(",", ":")),
                fetched_at,
            ),
        )
        count += 1
    return count


def fetch_all_trades(condition_id: str, page_limit: int = 500, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
    all_trades: List[Dict[str, Any]] = []
    offset = 0
    pages = 0
    while True:
        batch = http_get_json(
            f"{DATA_BASE}/trades",
            {"market": condition_id, "limit": page_limit, "offset": offset},
        )
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected trades response type: {type(batch).__name__}")
        all_trades.extend(batch)
        pages += 1
        if len(batch) < page_limit:
            break
        if max_pages is not None and pages >= max_pages:
            break
        offset += page_limit
        time.sleep(0.15)
    return all_trades


def fetch_market_by_slug(slug: str) -> Dict[str, Any]:
    payload = http_get_json(f"{GAMMA_BASE}/markets/slug/{slug}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected market response type: {type(payload).__name__}")
    if not payload.get("slug"):
        raise RuntimeError("Market payload missing slug")
    return payload


def fetch_orderbook(token_id: str) -> Dict[str, Any]:
    payload = http_get_json(f"{CLOB_BASE}/book", {"token_id": token_id})
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected book response type: {type(payload).__name__}")
    return payload


def summarize(conn: sqlite3.Connection, slug: str) -> Dict[str, Any]:
    cur = conn.cursor()
    market = cur.execute(
        "SELECT slug, condition_id, question, volume_num, liquidity_num, best_bid, best_ask, last_trade_price FROM markets WHERE slug = ?",
        (slug,),
    ).fetchone()
    token_count = cur.execute(
        "SELECT COUNT(*) FROM market_tokens WHERE market_slug = ?", (slug,)
    ).fetchone()[0]
    book_snaps = cur.execute(
        "SELECT COUNT(*) FROM orderbook_snapshots WHERE market_slug = ?", (slug,)
    ).fetchone()[0]
    trades_count = cur.execute(
        "SELECT COUNT(*) FROM trades WHERE slug = ? OR condition_id = (SELECT condition_id FROM markets WHERE slug = ?)",
        (slug, slug),
    ).fetchone()[0]
    if market is None:
        return {}
    return {
        "slug": market[0],
        "condition_id": market[1],
        "question": market[2],
        "volume_num": market[3],
        "liquidity_num": market[4],
        "best_bid": market[5],
        "best_ask": market[6],
        "last_trade_price": market[7],
        "token_count": token_count,
        "orderbook_snapshots": book_snaps,
        "trades": trades_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a Polymarket market (Gamma + CLOB + trades) and store in SQLite."
    )
    parser.add_argument("--slug", required=True, help="Market slug, e.g. btc-updown-5m-1771967400")
    parser.add_argument("--db", default="polymarket_markets.db", help="SQLite DB path")
    parser.add_argument("--trades-page-limit", type=int, default=500, help="Trades API page size")
    parser.add_argument("--trades-max-pages", type=int, default=None, help="Cap number of trade pages")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    run_id = conn.execute(
        "INSERT INTO ingest_runs (slug, started_at, status) VALUES (?, ?, ?)",
        (args.slug, utc_now_iso(), "running"),
    ).lastrowid
    conn.commit()

    try:
        fetched_at = utc_now_iso()
        market = fetch_market_by_slug(args.slug)
        insert_market(conn, market, fetched_at)

        for event in market.get("events") or []:
            if isinstance(event, dict):
                insert_event(conn, market["slug"], event, fetched_at)

        token_ids = insert_tokens(conn, market, fetched_at)

        for token_id in token_ids:
            book = fetch_orderbook(token_id)
            insert_orderbook_snapshot(
                conn=conn,
                market_slug=market["slug"],
                condition_id=market.get("conditionId"),
                token_id=token_id,
                book=book,
                fetched_at=utc_now_iso(),
            )
            time.sleep(0.1)

        condition_id = market.get("conditionId")
        trade_rows = 0
        if condition_id:
            trades = fetch_all_trades(
                condition_id=condition_id,
                page_limit=args.trades_page_limit,
                max_pages=args.trades_max_pages,
            )
            trade_rows = insert_trades(conn, trades, utc_now_iso())

        conn.execute(
            "UPDATE ingest_runs SET finished_at = ?, status = ?, note = ? WHERE id = ?",
            (utc_now_iso(), "success", f"inserted/updated trades rows: {trade_rows}", run_id),
        )
        conn.commit()

        print(json.dumps(summarize(conn, market["slug"]), indent=2, ensure_ascii=True))
        print(f"DB saved to: {args.db}")
    except Exception as exc:
        conn.execute(
            "UPDATE ingest_runs SET finished_at = ?, status = ?, note = ? WHERE id = ?",
            (utc_now_iso(), "error", str(exc), run_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
