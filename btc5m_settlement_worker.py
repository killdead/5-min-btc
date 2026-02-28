#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from eth_abi import encode as abi_encode
from eth_utils import keccak

from fetch_polymarket_market_to_db import DATA_BASE, fetch_market_by_slug, http_get_json, parse_json_maybe, to_float, utc_now_iso
from btc5m_trading_bot import TradingBotDB
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams


WAIT_STATUSES = {
    "both_filled_wait_resolution",
    "up_only_wait_resolution",
    "down_only_wait_resolution",
    "partial_both_wait_resolution",
    "live_orders_submitted",
    "live_submit_partial_or_error",
}

USDC_DECIMALS = 6
NEG_RISK_ADAPTER_DEFAULT = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
TERMINAL_SETTLEMENT_STATUSES = (
    "live_redeem_confirmed",
    "live_redeem_disabled",
    "live_redeem_missing_client",
    "live_redeem_missing_creds",
)

_CLOB_CLIENT: Optional[ClobClient] = None


def _safe_obj_to_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "json"):
        try:
            j = obj.json()
            if isinstance(j, dict):
                return j
            if isinstance(j, str):
                s = j.strip()
                if s.startswith("{") and s.endswith("}"):
                    parsed = json.loads(s)
                    if isinstance(parsed, dict):
                        return parsed
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        try:
            d = obj.__dict__
            if isinstance(d, dict):
                return d
        except Exception:
            pass
    return {"raw": str(obj)}


def _extract_order_state(order_resp: Any) -> Dict[str, Any]:
    d = _safe_obj_to_dict(order_resp)
    obj = d.get("order") if isinstance(d.get("order"), dict) else d
    status = str(
        obj.get("status")
        or obj.get("state")
        or obj.get("order_status")
        or d.get("status")
        or d.get("state")
        or "unknown"
    ).lower()
    size = (
        obj.get("size")
        or obj.get("original_size")
        or obj.get("remaining")
        or obj.get("quantity")
        or d.get("size")
    )
    matched = (
        obj.get("size_matched")
        or obj.get("matched_size")
        or obj.get("filled_size")
        or obj.get("filledSize")
        or obj.get("executed")
        or obj.get("filled")
        or d.get("size_matched")
        or d.get("matched_size")
        or d.get("filled_size")
        or d.get("filledSize")
    )
    price = obj.get("price") or obj.get("limit_price") or d.get("price")
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


def _get_live_client_for_reconcile() -> Optional[ClobClient]:
    global _CLOB_CLIENT
    if _CLOB_CLIENT is not None:
        return _CLOB_CLIENT
    try:
        host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLY_CHAIN_ID", str(POLYGON)))
        private_key = os.getenv("POLY_PRIVATE_KEY")
        if not private_key:
            return None
        funder = os.getenv("POLY_FUNDER")
        signature_type = os.getenv("POLY_SIGNATURE_TYPE")
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
            client.set_api_creds(ApiCreds(api_key=creds.api_key, api_secret=creds.api_secret, api_passphrase=creds.api_passphrase))
        _CLOB_CLIENT = client
        return _CLOB_CLIENT
    except Exception:
        return None


def _reconcile_cycle_fills_from_live_orders(db: TradingBotDB, cycle_id: str) -> int:
    """
    Backfill missing fills by polling CLOB order state for each submitted exchange_order_id.
    Useful when trading runs with --no-live-monitor.
    Returns number of inserted fill rows.
    """
    client = _get_live_client_for_reconcile()
    if client is None:
        return 0
    rows = db.conn.execute(
        """
        SELECT local_order_id, slug, outcome_name, chosen_price, exchange_order_id
        FROM strategy_orders
        WHERE cycle_id=? AND exchange_order_id IS NOT NULL AND exchange_order_id<>''
        """,
        (cycle_id,),
    ).fetchall()
    inserted = 0
    for r in rows:
        local_order_id = str(r["local_order_id"])
        ex_id = str(r["exchange_order_id"])
        try:
            resp = client.get_order(ex_id)
            st = _extract_order_state(resp)
        except Exception:
            continue
        matched_total = float(st.get("matched_size") or 0.0)
        if matched_total < 1e-9:
            continue
        prev_row = db.conn.execute(
            "SELECT COALESCE(SUM(fill_size),0) AS s FROM strategy_fills WHERE local_order_id=?",
            (local_order_id,),
        ).fetchone()
        already = float(prev_row["s"] or 0.0)
        delta = round(matched_total - already, 10)
        if delta <= 1e-9:
            continue
        chosen_price = to_float(r["chosen_price"]) or to_float(st.get("price")) or 0.0
        fill_price = to_float(st.get("avg_price")) or to_float(st.get("price")) or chosen_price
        fill_id = f"{local_order_id}:reconcile:{matched_total:.8f}"
        db.insert_fill(
            {
                "fill_id": fill_id,
                "local_order_id": local_order_id,
                "cycle_id": cycle_id,
                "slug": str(r["slug"]),
                "outcome_name": str(r["outcome_name"]),
                "side": "BUY",
                "fill_price": float(fill_price),
                "fill_size": float(delta),
                "maker": True,
                "fill_time": utc_now_iso(),
                "source": "clob_get_order_reconcile",
                "raw_json": st.get("raw"),
            }
        )
        db.insert_capital_event(
            {
                "cycle_id": cycle_id,
                "slug": str(r["slug"]),
                "event_type": "fill_cost",
                "outcome_name": str(r["outcome_name"]),
                "local_order_id": local_order_id,
                "amount_usdc": float(delta) * float(chosen_price),
                "note": "fill cost from reconcile matched_size delta",
                "raw_json": {"matched_total": matched_total, "delta": delta, "exchange_order_id": ex_id},
            }
        )
        inserted += 1
    if inserted > 0:
        print("[reconcile]", f"cycle_id={cycle_id}", f"fills_inserted={inserted}", flush=True)
    return inserted


def infer_winner_from_market_payload(market: Dict[str, Any]) -> Tuple[Optional[str], bool, Dict[str, Any]]:
    closed = bool(market.get("closed"))
    outcomes = [str(x) for x in parse_json_maybe(market.get("outcomes"), [])]
    prices_raw = parse_json_maybe(market.get("outcomePrices"), [])
    prices = [to_float(x) for x in prices_raw]
    winner = None
    if outcomes and prices and len(outcomes) == len(prices):
        for i, p in enumerate(prices):
            if p is not None and p >= 0.999:
                winner = outcomes[i]
                break
    info = {
        "closed": closed,
        "outcomes": outcomes,
        "outcomePrices": prices,
        "endDate": market.get("endDate"),
        "updatedAt": market.get("updatedAt"),
    }
    return winner, closed, info


def parse_iso_ts(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return None


def _get_conditional_balance(token_id: str) -> Optional[float]:
    """
    Read current conditional token balance from CLOB auth context.
    Returns token balance in human units (1e6 decimals) or None if unavailable.
    """
    try:
        host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLY_CHAIN_ID", str(POLYGON)))
        private_key = os.getenv("POLY_PRIVATE_KEY")
        if not private_key:
            return None
        funder = os.getenv("POLY_FUNDER")
        sig = int(os.getenv("POLY_SIGNATURE_TYPE", "1"))
        client = ClobClient(host=host, chain_id=chain_id, key=private_key, signature_type=sig, funder=funder)
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(ApiCreds(api_key=creds.api_key, api_secret=creds.api_secret, api_passphrase=creds.api_passphrase))
        bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=str(token_id)))
        raw = None
        if isinstance(bal, dict):
            raw = bal.get("balance")
        else:
            raw = getattr(bal, "balance", None)
        if raw is None:
            return None
        return float(raw) / (10**USDC_DECIMALS)
    except Exception:
        return None


def _fetch_user_positions_for_condition(condition_id: str) -> List[Dict[str, Any]]:
    """
    Fetch user positions from Data API filtered by conditionId.
    Uses POLY_FUNDER as wallet/proxy identity.
    """
    user = (os.getenv("POLY_FUNDER") or "").strip()
    if not user:
        return []
    try:
        payload = http_get_json(f"{DATA_BASE}/positions", {"user": user, "size": 500})
    except Exception:
        return []
    rows = payload if isinstance(payload, list) else []
    out: List[Dict[str, Any]] = []
    cid_l = condition_id.lower()
    for r in rows:
        if not isinstance(r, dict):
            continue
        if str(r.get("conditionId") or "").lower() != cid_l:
            continue
        out.append(r)
    return out


def get_waiting_cycles(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    qmarks = ",".join("?" * len(WAIT_STATUSES))
    return conn.execute(
        f"""
        SELECT c.cycle_id, c.slug, c.condition_id, c.status
        FROM strategy_cycles c
        WHERE c.status IN ({qmarks})
          AND NOT EXISTS (
            SELECT 1 FROM strategy_settlements s
            WHERE s.cycle_id = c.cycle_id AND s.settlement_mode = 'paper_estimate'
          )
        ORDER BY c.created_at ASC
        LIMIT ?
        """,
        tuple(WAIT_STATUSES) + (limit,),
    ).fetchall()


def get_waiting_cycles_for_mode(conn: sqlite3.Connection, limit: int, settlement_mode: str) -> List[sqlite3.Row]:
    qmarks = ",".join("?" * len(WAIT_STATUSES))
    terminal_qmarks = ",".join("?" * len(TERMINAL_SETTLEMENT_STATUSES))
    return conn.execute(
        f"""
        SELECT c.cycle_id, c.slug, c.condition_id, c.status, c.mode
        FROM strategy_cycles c
        WHERE c.status IN ({qmarks})
          AND c.mode IN ('paper-live', 'live')
          AND c.slug NOT LIKE 'btc-updown-5m-MOCK%'
          AND NOT EXISTS (
            SELECT 1 FROM strategy_settlements s
            WHERE s.cycle_id = c.cycle_id AND s.settlement_mode = ?
              AND (s.status LIKE 'settled_%' OR s.status IN ({terminal_qmarks}))
          )
        ORDER BY c.created_at ASC
        LIMIT ?
        """,
        tuple(WAIT_STATUSES) + (settlement_mode,) + tuple(TERMINAL_SETTLEMENT_STATUSES) + (limit,),
    ).fetchall()


def get_waiting_cycles_filtered(
    conn: sqlite3.Connection,
    limit: int,
    settlement_mode: str,
    cycle_id: Optional[str] = None,
    slug: Optional[str] = None,
) -> List[sqlite3.Row]:
    if cycle_id:
        terminal_qmarks = ",".join("?" * len(TERMINAL_SETTLEMENT_STATUSES))
        row = conn.execute(
            """
            SELECT c.cycle_id, c.slug, c.condition_id, c.status, c.mode
            FROM strategy_cycles c
            WHERE c.cycle_id = ?
              AND c.status IN ({})
              AND c.mode IN ('paper-live', 'live')
              AND c.slug NOT LIKE 'btc-updown-5m-MOCK%'
              AND NOT EXISTS (
                SELECT 1 FROM strategy_settlements s
                WHERE s.cycle_id = c.cycle_id AND s.settlement_mode = ?
                  AND (s.status LIKE 'settled_%' OR s.status IN ({}))
              )
            """.format(",".join("?" * len(WAIT_STATUSES)), terminal_qmarks),
            (cycle_id, *tuple(WAIT_STATUSES), settlement_mode, *tuple(TERMINAL_SETTLEMENT_STATUSES)),
        ).fetchone()
        return [row] if row else []
    if slug:
        qmarks = ",".join("?" * len(WAIT_STATUSES))
        terminal_qmarks = ",".join("?" * len(TERMINAL_SETTLEMENT_STATUSES))
        return conn.execute(
            f"""
            SELECT c.cycle_id, c.slug, c.condition_id, c.status, c.mode
            FROM strategy_cycles c
            WHERE c.slug = ?
              AND c.status IN ({qmarks})
              AND c.mode IN ('paper-live', 'live')
              AND c.slug NOT LIKE 'btc-updown-5m-MOCK%'
              AND NOT EXISTS (
                SELECT 1 FROM strategy_settlements s
                WHERE s.cycle_id = c.cycle_id AND s.settlement_mode = ?
                  AND (s.status LIKE 'settled_%' OR s.status IN ({terminal_qmarks}))
              )
            ORDER BY c.created_at ASC
            LIMIT ?
            """,
            (slug, *tuple(WAIT_STATUSES), settlement_mode, *tuple(TERMINAL_SETTLEMENT_STATUSES), limit),
        ).fetchall()
    return get_waiting_cycles_for_mode(conn, limit, settlement_mode)


def settle_cycle_estimate(db: TradingBotDB, cycle: sqlite3.Row) -> Tuple[bool, str]:
    slug = str(cycle["slug"])
    market = fetch_market_by_slug(slug)
    winner, closed, info = infer_winner_from_market_payload(market)
    now_ts = time.time()
    end_ts = parse_iso_ts(market.get("endDate") or info.get("endDate"))
    after_end = bool(end_ts and now_ts >= end_ts)
    if not closed and not after_end:
        return False, "market_not_closed"
    claim_start_delay_s = float(os.getenv("POLY_CLAIM_START_DELAY_SECONDS", "300"))
    if end_ts and now_ts < (end_ts + claim_start_delay_s):
        return False, f"claim_start_delay_not_reached wait_s={round((end_ts + claim_start_delay_s) - now_ts,1)}"

    fills = db.cycle_fills_by_outcome(str(cycle["cycle_id"]))
    up_filled = float(fills.get("Up", 0.0))
    down_filled = float(fills.get("Down", 0.0))
    redeem_est = 0.0
    if winner == "Up":
        redeem_est = up_filled
    elif winner == "Down":
        redeem_est = down_filled

    db.insert_settlement(
        {
            "cycle_id": str(cycle["cycle_id"]),
            "slug": slug,
            "condition_id": cycle["condition_id"],
            "settlement_mode": "paper_estimate",
            "market_closed": 1,
            "winner_outcome": winner,
            "redeem_value_est": redeem_est,
            "up_filled": up_filled,
            "down_filled": down_filled,
            "status": "settled_estimated",
            "note": "paper estimate from outcomePrices",
            "raw_json": info,
        }
    )
    if redeem_est > 0:
        db.insert_capital_event(
            {
                "cycle_id": str(cycle["cycle_id"]),
                "slug": slug,
                "event_type": "redeem_estimate",
                "amount_usdc": redeem_est,
                "note": f"winner={winner}",
                "raw_json": {"winner": winner, "up_filled": up_filled, "down_filled": down_filled},
            }
        )
    db.upsert_cycle(
        {
            "cycle_id": str(cycle["cycle_id"]),
            "slug": slug,
            "condition_id": cycle["condition_id"],
            "target_price": 0.49,  # preserved by upsert; placeholder overwritten by ON CONFLICT only status/note/raw_json
            "target_size": 5.0,
            "status": "settled_estimated",
            "mode": "paper-live",
            "note": f"winner={winner} redeem_est={redeem_est}",
            "raw_json": {"settlement_worker": True, "winner": winner, "redeem_est": redeem_est},
        }
    )
    return True, f"winner={winner} redeem_est={redeem_est}"


def settle_cycle_live_redeem(db: TradingBotDB, cycle: sqlite3.Row) -> Tuple[bool, str]:
    """
    Live redeem placeholder with explicit prerequisite checks.
    Real Polymarket redeem for proxy wallets generally requires relayer/builder client credentials.
    """
    slug = str(cycle["slug"])
    cycle_id = str(cycle["cycle_id"])
    # Reconcile fills from live order state before settlement logic.
    try:
        _reconcile_cycle_fills_from_live_orders(db, cycle_id)
    except Exception:
        pass
    market = fetch_market_by_slug(slug)
    winner, closed, info = infer_winner_from_market_payload(market)
    now_ts = time.time()
    end_ts = parse_iso_ts(market.get("endDate") or info.get("endDate"))
    after_end = bool(end_ts and now_ts >= end_ts)
    if not closed and not after_end:
        return False, "market_not_closed"

    # Prereq checks only if disabled/missing.
    enable_live = os.getenv("POLY_ENABLE_LIVE_REDEEM", "").strip() == "1"
    if not enable_live:
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "live_redeem",
                "market_closed": 1,
                "winner_outcome": winner,
                "redeem_value_est": None,
                "up_filled": float(db.cycle_fills_by_outcome(cycle_id).get("Up", 0.0)),
                "down_filled": float(db.cycle_fills_by_outcome(cycle_id).get("Down", 0.0)),
                "status": "live_redeem_disabled",
                "note": "Set POLY_ENABLE_LIVE_REDEEM=1 after configuring relayer client/creds",
                "raw_json": info,
            }
        )
        return False, "live_redeem_disabled"
    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import OperationType, SafeTransaction
        from py_builder_signing_sdk.config import BuilderConfig
        from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
    except Exception:
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "live_redeem",
                "market_closed": 1,
                "winner_outcome": winner,
                "redeem_value_est": None,
                "up_filled": float(db.cycle_fills_by_outcome(cycle_id).get("Up", 0.0)),
                "down_filled": float(db.cycle_fills_by_outcome(cycle_id).get("Down", 0.0)),
                "status": "live_redeem_missing_client",
                "note": "py-builder-relayer-client / py-builder-signing-sdk not installed",
                "raw_json": info,
            }
        )
        return False, "live_redeem_missing_client"
    private_key = os.getenv("POLY_PRIVATE_KEY")
    builder_key = os.getenv("POLY_BUILDER_API_KEY")
    builder_secret = os.getenv("POLY_BUILDER_SECRET")
    builder_passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not (private_key and builder_key and builder_secret and builder_passphrase):
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "live_redeem",
                "market_closed": 1,
                "winner_outcome": winner,
                "redeem_value_est": None,
                "up_filled": float(db.cycle_fills_by_outcome(cycle_id).get("Up", 0.0)),
                "down_filled": float(db.cycle_fills_by_outcome(cycle_id).get("Down", 0.0)),
                "status": "live_redeem_missing_creds",
                "note": "Missing POLY_PRIVATE_KEY or POLY_BUILDER_* env vars",
                "raw_json": info,
            }
        )
        return False, "live_redeem_missing_creds"

    chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
    relayer_url = os.getenv("POLY_RELAYER_HOST", "https://relayer-v2.polymarket.com/")
    adapter_addr = os.getenv("POLY_NEG_RISK_ADAPTER", NEG_RISK_ADAPTER_DEFAULT)

    # Filled quantities are stored in "shares"; adapter amounts are base units (1e6).
    fills = db.cycle_fills_by_outcome(cycle_id)
    up_filled = float(fills.get("Up", 0.0))
    down_filled = float(fills.get("Down", 0.0))
    positions_rows = _fetch_user_positions_for_condition(str(cycle["condition_id"] or ""))
    if up_filled <= 0 and down_filled <= 0 and positions_rows:
        # Fallback for cycles where DB missed fill events but wallet still has shares.
        up_from_pos = 0.0
        down_from_pos = 0.0
        for r in positions_rows:
            try:
                sz = float(r.get("size") or 0.0)
            except Exception:
                sz = 0.0
            outcome = str(r.get("outcome") or "")
            if outcome.lower() == "up":
                up_from_pos += max(0.0, sz)
            elif outcome.lower() == "down":
                down_from_pos += max(0.0, sz)
        if up_from_pos > 0 or down_from_pos > 0:
            up_filled = up_from_pos
            down_filled = down_from_pos
            db.upsert_cycle(
                {
                    "cycle_id": cycle_id,
                    "slug": slug,
                    "condition_id": cycle["condition_id"],
                    "target_price": 0.49,
                    "target_size": 5.0,
                    "status": cycle["status"],
                    "mode": "live",
                    "note": f"fills recovered from positions up={round(up_filled,6)} down={round(down_filled,6)}",
                    "raw_json": {"settlement_worker": True, "recovered_from_positions": True},
                }
            )

    require_redeemable = os.getenv("POLY_REQUIRE_DATA_REDEEMABLE", "1").strip() == "1"
    if require_redeemable and positions_rows:
        # Gate real redeem attempts by Data API redeemable flag to reduce failing tx spam.
        winner_l = (winner or "").lower()
        eligible = []
        for r in positions_rows:
            try:
                sz = float(r.get("size") or 0.0)
            except Exception:
                sz = 0.0
            if sz <= 0:
                continue
            out_l = str(r.get("outcome") or "").lower()
            if winner_l and out_l != winner_l:
                continue
            if bool(r.get("redeemable")):
                eligible.append(r)
        if not eligible:
            giveup_s = float(os.getenv("POLY_DATA_REDEEMABLE_GIVEUP_SECONDS", "10800"))
            if end_ts and now_ts >= (end_ts + giveup_s):
                # After a long post-end window, treat persistent non-redeemable data-api state as terminal.
                db.insert_settlement(
                    {
                        "cycle_id": cycle_id,
                        "slug": slug,
                        "condition_id": cycle["condition_id"],
                        "settlement_mode": "live_redeem",
                        "market_closed": 1 if closed else 0,
                        "winner_outcome": winner,
                        "redeem_value_est": 0.0,
                        "up_filled": up_filled,
                        "down_filled": down_filled,
                        "status": "settled_zero_redeem",
                        "note": "data_api_not_redeemable_after_timeout",
                        "raw_json": {
                            **info,
                            "positions_rows": positions_rows[:20],
                            "giveup_seconds": giveup_s,
                        },
                    }
                )
                db.upsert_cycle(
                    {
                        "cycle_id": cycle_id,
                        "slug": slug,
                        "condition_id": cycle["condition_id"],
                        "target_price": 0.49,
                        "target_size": 5.0,
                        "status": "settled_zero_redeem",
                        "mode": "live",
                        "note": "data api still non-redeemable after timeout",
                        "raw_json": {"settlement_worker": True, "data_api_non_redeemable": True},
                    }
                )
                return True, "settled_zero_redeem_data_api_timeout"
            return False, "live_redeem_not_claimable_yet_data_api"

    if up_filled <= 0 and down_filled <= 0:
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "live_redeem",
                "market_closed": 1 if closed else 0,
                "winner_outcome": winner,
                "redeem_value_est": 0.0,
                "up_filled": up_filled,
                "down_filled": down_filled,
                "status": "settled_zero_redeem",
                "note": "no_fills_to_redeem",
                "raw_json": {**info, "positions_rows": positions_rows[:20]},
            }
        )
        db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "target_price": 0.49,
                "target_size": 5.0,
                "status": "no_fill",
                "mode": "live",
                "note": "no fills detected; nothing to redeem",
                "raw_json": {"settlement_worker": True, "no_fills": True},
            }
        )
        return True, "settled_zero_redeem_no_fills"

    # If winner is already known, compute winner-side amount only.
    # If winner is not yet visible but market passed endDate, try claiming both amounts and let the contract decide.
    if winner:
        up_amt = int(round((up_filled if winner == "Up" else 0.0) * (10**USDC_DECIMALS)))
        down_amt = int(round((down_filled if winner == "Down" else 0.0) * (10**USDC_DECIMALS)))
    else:
        if not after_end:
            return False, "winner_not_inferred"
        up_amt = int(round(up_filled * (10**USDC_DECIMALS)))
        down_amt = int(round(down_filled * (10**USDC_DECIMALS)))

    if up_amt == 0 and down_amt == 0:
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "live_redeem",
                "market_closed": 1 if closed else 0,
                "winner_outcome": winner,
                "redeem_value_est": 0.0,
                "up_filled": up_filled,
                "down_filled": down_filled,
                "status": "settled_zero_redeem",
                "note": "winner_fill_zero (no winning shares to redeem)",
                "raw_json": info,
            }
        )
        db.upsert_cycle(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "target_price": 0.49,
                "target_size": 5.0,
                "status": "settled_zero_redeem",
                "mode": "live",
                "note": "no winning shares to redeem",
                "raw_json": {"settlement_worker": True, "winner": winner, "redeem": 0},
            }
        )
        return True, "settled_zero_redeem"

    # Extra safety: if winner is known and on-chain winner token balance is zero,
    # skip redeem attempts to avoid repeated failed claims.
    try:
        outcomes = [str(x) for x in parse_json_maybe(market.get("outcomes"), [])]
        token_ids = [str(x) for x in parse_json_maybe(market.get("clobTokenIds"), [])]
        winner_token_id = None
        if winner in outcomes and len(outcomes) == len(token_ids):
            winner_token_id = token_ids[outcomes.index(winner)]
        if winner_token_id:
            winner_bal = _get_conditional_balance(winner_token_id)
            if winner_bal is not None and winner_bal <= 1e-9:
                db.insert_settlement(
                    {
                        "cycle_id": cycle_id,
                        "slug": slug,
                        "condition_id": cycle["condition_id"],
                        "settlement_mode": "live_redeem",
                        "market_closed": 1 if closed else 0,
                        "winner_outcome": winner,
                        "redeem_value_est": 0.0,
                        "up_filled": up_filled,
                        "down_filled": down_filled,
                        "status": "settled_zero_redeem",
                        "note": "winner_token_balance_zero",
                        "raw_json": {**info, "winner_token_id": winner_token_id, "winner_token_balance": winner_bal},
                    }
                )
                db.upsert_cycle(
                    {
                        "cycle_id": cycle_id,
                        "slug": slug,
                        "condition_id": cycle["condition_id"],
                        "target_price": 0.49,
                        "target_size": 5.0,
                        "status": "settled_zero_redeem",
                        "mode": "live",
                        "note": "winner token balance is zero",
                        "raw_json": {"settlement_worker": True, "winner": winner, "winner_token_balance": winner_bal},
                    }
                )
                return True, "settled_zero_redeem_winner_balance_zero"
    except Exception:
        pass

    condition_id = str(cycle["condition_id"] or "").strip()
    if not condition_id.startswith("0x") or len(condition_id) != 66:
        return False, "invalid_condition_id"

    # Preferred path for proxy wallets: TypeScript relayer helper (official client supports PROXY tx type).
    relayer_impl = os.getenv("POLY_RELAYER_IMPL", "ts").strip().lower()
    if relayer_impl == "ts":
        # After endDate, try claim even if winner is not yet visible in Gamma.
        up_shares_arg = up_filled if (winner == "Up" or (winner is None and after_end)) else 0.0
        down_shares_arg = down_filled if (winner == "Down" or (winner is None and after_end)) else 0.0
        route = "neg-risk" if bool(market.get("negRisk")) else "ctf"
        claim_retry_grace_s = float(os.getenv("POLY_CLAIM_RETRY_GRACE_SECONDS", "900"))
        cmd = [
            os.getenv("POLY_RELAYER_TS_RUNNER", "./run_relayer_redeem.sh"),
            "--condition-id",
            condition_id,
            "--up-shares",
            str(up_shares_arg),
            "--down-shares",
            str(down_shares_arg),
            "--route",
            route,
            "--execute",
            "--wait",
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            now_after_call = time.time()
            stdout = (proc.stdout or "")[-3000:]
            stderr = (proc.stderr or "")[-2000:]
            combined = f"{stdout}\n{stderr}".lower()
            within_claim_grace = bool(after_end and end_ts and now_after_call < (end_ts + claim_retry_grace_s))
            onchain_failed = "failed onchain" in combined
            if (
                "failed onchain" in combined
                or "redeem tx wait returned no final state" in combined
                or "http request failed" in combined
            ):
                # Common transient path: relayer accepted request but claim is not yet executable/finalized.
                status = "live_redeem_not_claimable_yet"
            elif proc.returncode == 0:
                # We call helper with --wait, so rc=0 means it completed and did not observe a failed final state.
                status = "live_redeem_confirmed"
            elif "not claimable" in combined or "revert" in combined or "execution reverted" in combined:
                status = "live_redeem_not_claimable_yet"
            elif within_claim_grace:
                # Be conservative for the first minutes after market end; the site often lags before claims succeed.
                status = "live_redeem_not_claimable_yet"
            else:
                status = "live_redeem_error"

            # If tx failed onchain and winner-side conditional balance is already zero,
            # there is nothing left to redeem for this cycle; stop retries.
            winner_bal_after = None
            winner_token_id = None
            if onchain_failed and winner:
                try:
                    outcomes = [str(x) for x in parse_json_maybe(market.get("outcomes"), [])]
                    token_ids = [str(x) for x in parse_json_maybe(market.get("clobTokenIds"), [])]
                    if winner in outcomes and len(outcomes) == len(token_ids):
                        winner_token_id = token_ids[outcomes.index(winner)]
                        winner_bal_after = _get_conditional_balance(winner_token_id)
                        if winner_bal_after is not None and winner_bal_after <= 1e-9:
                            status = "settled_zero_redeem"
                except Exception:
                    pass
            db.insert_settlement(
                {
                    "cycle_id": cycle_id,
                    "slug": slug,
                    "condition_id": cycle["condition_id"],
                    "settlement_mode": "live_redeem",
                    "market_closed": 1 if closed else 0,
                    "winner_outcome": winner,
                    "redeem_value_est": (up_amt + down_amt) / (10**USDC_DECIMALS),
                    "up_filled": up_filled,
                    "down_filled": down_filled,
                    "status": status,
                    "note": f"ts_helper rc={proc.returncode}",
                    "raw_json": {
                        **info,
                        "ts_cmd": cmd,
                        "stdout": stdout,
                        "stderr": stderr,
                        "adapter": adapter_addr,
                        "up_amt": up_amt,
                        "down_amt": down_amt,
                        "route": route,
                        "within_claim_grace": within_claim_grace,
                        "claim_retry_grace_s": claim_retry_grace_s,
                        "onchain_failed": onchain_failed,
                        "winner_token_id": winner_token_id,
                        "winner_token_balance_after": winner_bal_after,
                    },
                }
            )
            if proc.returncode == 0:
                db.upsert_cycle(
                    {
                        "cycle_id": cycle_id,
                        "slug": slug,
                        "condition_id": cycle["condition_id"],
                        "target_price": 0.49,
                        "target_size": 5.0,
                        "status": "settled_live_redeem_confirmed",
                        "mode": "live",
                        "note": "ts helper confirmed redeem",
                        "raw_json": {"settlement_worker": True, "redeem_confirmed": True},
                    }
                )
                return True, "live_redeem_confirmed_ts_helper"
            if status == "settled_zero_redeem":
                db.upsert_cycle(
                    {
                        "cycle_id": cycle_id,
                        "slug": slug,
                        "condition_id": cycle["condition_id"],
                        "target_price": 0.49,
                        "target_size": 5.0,
                        "status": "settled_zero_redeem",
                        "mode": "live",
                        "note": "ts helper failed onchain and winner token balance is zero",
                        "raw_json": {
                            "settlement_worker": True,
                            "winner": winner,
                            "winner_token_id": winner_token_id,
                            "winner_token_balance_after": winner_bal_after,
                        },
                    }
                )
                return True, "settled_zero_redeem_winner_balance_zero_after_failed_onchain"
            if status == "live_redeem_not_claimable_yet":
                return False, "live_redeem_not_claimable_yet"
            return False, f"live_redeem_ts_helper_error rc={proc.returncode}"
        except Exception as exc:
            return False, f"live_redeem_ts_helper_exec_error {exc}"

    # Encode redeemPositions(bytes32,uint256[]) for Neg Risk Adapter
    fn_sig = "redeemPositions(bytes32,uint256[])"
    selector = keccak(text=fn_sig)[:4]
    data_args = abi_encode(["bytes32", "uint256[]"], [bytes.fromhex(condition_id[2:]), [up_amt, down_amt]])
    calldata = "0x" + (selector + data_args).hex()

    builder_cfg = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=builder_key,
            secret=builder_secret,
            passphrase=builder_passphrase,
        )
    )
    client = RelayClient(
        relayer_url=relayer_url,
        chain_id=chain_id,
        private_key=private_key,
        builder_config=builder_cfg,
    )

    tx = SafeTransaction(
        to=adapter_addr,
        operation=OperationType.Call,
        data=calldata,
        value="0",
    )
    try:
        tx_resp = client.execute([tx], metadata=f"redeem:{slug}:{cycle_id}")
        tx_id = getattr(tx_resp, "transaction_id", None)
        tx_hash = getattr(tx_resp, "transaction_hash", None)
        tx_status = None
        note = {
            "tx_id": tx_id,
            "tx_hash": tx_hash,
            "tx_status": tx_status,
            "adapter": adapter_addr,
            "winner": winner,
            "up_amt": up_amt,
            "down_amt": down_amt,
        }
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "live_redeem",
                "market_closed": 1 if closed else 0,
                "winner_outcome": winner,
                "redeem_value_est": (up_amt + down_amt) / (10**USDC_DECIMALS),
                "up_filled": up_filled,
                "down_filled": down_filled,
                "status": "live_redeem_submitted",
                "note": json.dumps(note, ensure_ascii=True),
                "raw_json": {
                    **info,
                    "tx_response": {
                        "transactionID": tx_id,
                        "transactionHash": tx_hash,
                        "transactionStatus": tx_status,
                    },
                    "adapter": adapter_addr,
                    "calldata_prefix": calldata[:34],
                },
            }
        )
        # Python relayer path does not wait for final on-chain confirmation.
        # Keep cycle in waiting state and let next worker iterations reconcile final state.
        return False, f"live_redeem_submitted_pending_confirmation tx_hash={tx_hash or tx_id}"
    except Exception as exc:
        db.insert_settlement(
            {
                "cycle_id": cycle_id,
                "slug": slug,
                "condition_id": cycle["condition_id"],
                "settlement_mode": "live_redeem",
                "market_closed": 1 if closed else 0,
                "winner_outcome": winner,
                "redeem_value_est": None,
                "up_filled": up_filled,
                "down_filled": down_filled,
                "status": "live_redeem_error",
                "note": str(exc)[:500],
                "raw_json": {**info, "adapter": adapter_addr, "up_amt": up_amt, "down_amt": down_amt},
            }
        )
        return False, f"live_redeem_error {exc}"


def main() -> None:
    p = argparse.ArgumentParser(description="Settle strategy cycles post-resolution (paper estimate)")
    p.add_argument("--db", default="polymarket_collector.db")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--loop", action="store_true", help="Run continuously")
    p.add_argument("--poll-seconds", type=float, default=15.0)
    p.add_argument("--mode", choices=["paper-estimate", "live-redeem"], default="paper-estimate")
    p.add_argument("--cycle-id", help="Process only this cycle_id")
    p.add_argument("--slug", help="Process only this market slug")
    args = p.parse_args()

    db = TradingBotDB(args.db)
    try:
        while True:
            settlement_mode = "paper_estimate" if args.mode == "paper-estimate" else "live_redeem"
            rows = get_waiting_cycles_filtered(
                db.conn,
                args.limit,
                settlement_mode,
                cycle_id=args.cycle_id,
                slug=args.slug,
            )
            if rows:
                print(f"[settlement] mode={args.mode} waiting_cycles={len(rows)}", flush=True)
            for row in rows:
                try:
                    if args.mode == "paper-estimate":
                        ok, msg = settle_cycle_estimate(db, row)
                    else:
                        ok, msg = settle_cycle_live_redeem(db, row)
                    if ok:
                        print(f"[settlement] settled cycle_id={row['cycle_id']} slug={row['slug']} {msg}", flush=True)
                    else:
                        print(f"[settlement] skipped cycle_id={row['cycle_id']} slug={row['slug']} {msg}", flush=True)
                except Exception as exc:
                    print(f"[settlement] error cycle_id={row['cycle_id']} slug={row['slug']} err={exc}", flush=True)
            if not args.loop:
                break
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("[settlement] stopped", flush=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
