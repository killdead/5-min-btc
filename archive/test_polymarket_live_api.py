#!/usr/bin/env python3
import argparse
import json
import os
from typing import Any, Dict, Optional

from fetch_polymarket_market_to_db import fetch_market_by_slug, parse_json_maybe
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OrderArgs,
    OrderType,
)
from py_clob_client.constants import POLYGON


def jprint(label: str, payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=True, default=str)
    if len(text) > 1200:
        text = text[:1200] + "...<truncated>"
    print(f"[{label}] {text}", flush=True)


def init_client() -> ClobClient:
    host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("POLY_CHAIN_ID", str(POLYGON)))
    private_key = os.getenv("POLY_PRIVATE_KEY")
    if not private_key:
        raise RuntimeError("Missing POLY_PRIVATE_KEY")
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
        print("[auth] using API creds from env", flush=True)
    else:
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        print("[auth] derived API creds from signer", flush=True)
    return client


def get_market_ctx(slug: str) -> Dict[str, str]:
    m = fetch_market_by_slug(slug)
    token_ids = [str(x) for x in parse_json_maybe(m.get("clobTokenIds"), [])]
    outcomes = [str(x) for x in parse_json_maybe(m.get("outcomes"), [])]
    mapping = {outcomes[i].lower(): token_ids[i] for i in range(min(len(outcomes), len(token_ids)))}
    if "up" not in mapping or "down" not in mapping:
        raise RuntimeError(f"Market {slug} missing Up/Down mapping")
    return {
        "slug": slug,
        "conditionId": str(m.get("conditionId") or ""),
        "question": str(m.get("question") or ""),
        "up_token_id": mapping["up"],
        "down_token_id": mapping["down"],
    }


def safe_json(resp: Any) -> Any:
    try:
        if hasattr(resp, "json"):
            j = resp.json()
            if isinstance(j, str):
                try:
                    return json.loads(j)
                except Exception:
                    return j
            return j
    except Exception:
        pass
    try:
        if hasattr(resp, "__dict__"):
            d = resp.__dict__
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    return resp


def best_prices_from_book(book: Any) -> Dict[str, Optional[float]]:
    payload = safe_json(book)
    if not isinstance(payload, dict):
        return {"best_bid": None, "best_ask": None}
    bids = (payload or {}).get("bids") or []
    asks = (payload or {}).get("asks") or []
    bid_vals = []
    ask_vals = []
    for x in bids:
        try:
            if isinstance(x, dict):
                bid_vals.append(float(x.get("price")))
            else:
                bid_vals.append(float(getattr(x, "price")))
        except Exception:
            pass
    for x in asks:
        try:
            if isinstance(x, dict):
                ask_vals.append(float(x.get("price")))
            else:
                ask_vals.append(float(getattr(x, "price")))
        except Exception:
            pass
    return {
        "best_bid": max(bid_vals) if bid_vals else None,
        "best_ask": min(ask_vals) if ask_vals else None,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Smoke-test Polymarket live APIs (Gamma + CLOB)")
    p.add_argument("--slug", required=True, help="BTC 5m market slug")
    p.add_argument("--size-shares", type=float, default=5.0)
    p.add_argument("--price", type=float, default=0.49, help="Test BUY price for create_order signing")
    p.add_argument("--submit-test-order", action="store_true", help="Actually post one test order (real)")
    p.add_argument("--outcome", choices=["Up", "Down"], default="Up", help="Outcome for submit-test-order")
    p.add_argument("--confirm-real", action="store_true", help="Required with --submit-test-order")
    args = p.parse_args()

    slug = args.slug.strip().strip("~")
    if slug != args.slug:
        print(f"[input] normalized_slug={slug}", flush=True)
    market = get_market_ctx(slug)
    print(
        "[market]",
        f"slug={market['slug']}",
        f"condition_id={market['conditionId']}",
        f"up={market['up_token_id']}",
        f"down={market['down_token_id']}",
        flush=True,
    )

    client = init_client()
    signer_addr = None
    try:
        signer_addr = client.signer.account.address if getattr(client, "signer", None) else None
    except Exception:
        pass
    print(
        "[auth-context]",
        f"signer={signer_addr}",
        f"funder={os.getenv('POLY_FUNDER')}",
        f"signature_type={os.getenv('POLY_SIGNATURE_TYPE')}",
        flush=True,
    )

    ok = client.get_ok()
    print("[clob]", f"ok={ok}", flush=True)

    bal_params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    bal = safe_json(client.get_balance_allowance(bal_params))
    jprint("balance_allowance_collateral", bal)

    for outcome, token_id in (("Up", market["up_token_id"]), ("Down", market["down_token_id"])):
        book = client.get_order_book(token_id)
        payload = safe_json(book)
        bp = best_prices_from_book(payload)
        print(
            "[book]",
            f"outcome={outcome}",
            f"token={token_id}",
            f"best_bid={bp['best_bid']}",
            f"best_ask={bp['best_ask']}",
            flush=True,
        )
        # Also check conditional token balance/allowance for this outcome token
        try:
            cbal = safe_json(
                client.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                )
            )
            jprint(f"balance_allowance_conditional_{outcome}", cbal)
        except Exception as exc:
            print("[balance-conditional-error]", f"outcome={outcome} err={exc}", flush=True)

    # Always validate signing path by creating (but not posting) one order.
    token_id = market["up_token_id"] if args.outcome == "Up" else market["down_token_id"]
    order_args = OrderArgs(token_id=token_id, price=float(args.price), size=float(args.size_shares), side="BUY")
    signed = client.create_order(order_args)
    jprint("create_order_signed_preview", signed)
    print("[validate]", f"create_order=ok outcome={args.outcome} price={args.price} size={args.size_shares}", flush=True)

    if args.submit_test_order:
        if not args.confirm_real:
            raise RuntimeError("--submit-test-order requires --confirm-real")
        resp = client.post_order(signed, OrderType.GTC)
        payload = safe_json(resp)
        jprint("post_order_response", payload)
        print("[submit]", "post_order sent", flush=True)


if __name__ == "__main__":
    main()
