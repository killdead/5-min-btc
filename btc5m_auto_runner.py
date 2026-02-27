#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fetch_polymarket_market_to_db import http_get_json


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


def iso_utc(ts: Optional[float]) -> str:
    if ts is None:
        return "NA"
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def discover_btc5m_markets(limit: int = 1000) -> List[Dict[str, Any]]:
    markets = http_get_json(
        f"{GAMMA_BASE}/markets",
        {"closed": "false", "limit": limit},
    )
    if not isinstance(markets, list):
        raise RuntimeError("Unexpected Gamma /markets response")

    out: List[Dict[str, Any]] = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        slug = str(m.get("slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            continue
        ev0 = (m.get("events") or [{}])[0] if m.get("events") else {}
        start_ts = parse_iso_ts(
            m.get("eventStartTime") or ev0.get("startTime") or m.get("startDate") or ev0.get("startDate")
        )
        end_ts = parse_iso_ts(m.get("endDate") or ev0.get("endDate"))
        out.append(
            {
                "slug": slug,
                "question": m.get("question"),
                "conditionId": m.get("conditionId"),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "active": bool(m.get("active")),
                "enableOrderBook": bool(m.get("enableOrderBook")),
                "acceptingOrders": bool(m.get("acceptingOrders")),
                "raw": m,
            }
        )
    out.sort(key=lambda x: (x["start_ts"] or 10**18, x["end_ts"] or 10**18, x["slug"]))
    return out


def market_row_from_payload(m: Dict[str, Any]) -> Dict[str, Any]:
    ev0 = (m.get("events") or [{}])[0] if m.get("events") else {}
    start_ts = parse_iso_ts(
        m.get("eventStartTime") or ev0.get("startTime") or m.get("startDate") or ev0.get("startDate")
    )
    end_ts = parse_iso_ts(m.get("endDate") or ev0.get("endDate"))
    return {
        "slug": str(m.get("slug") or ""),
        "question": m.get("question"),
        "conditionId": m.get("conditionId"),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "active": bool(m.get("active")),
        "enableOrderBook": bool(m.get("enableOrderBook")),
        "acceptingOrders": bool(m.get("acceptingOrders")),
        "raw": m,
    }


def discover_btc5m_markets_by_slug_probe(
    *,
    now_ts: float,
    hours_back: int = 1,
    hours_ahead: int = 24,
) -> List[Dict[str, Any]]:
    # BTC 5m slugs are deterministic: btc-updown-5m-{eventStartEpochRounded5m}
    start_epoch = int(now_ts) - hours_back * 3600
    end_epoch = int(now_ts) + hours_ahead * 3600
    first = start_epoch - (start_epoch % 300)
    out: List[Dict[str, Any]] = []
    for ts in range(first, end_epoch + 1, 300):
        slug = f"btc-updown-5m-{ts}"
        try:
            payload = http_get_json(f"{GAMMA_BASE}/markets/slug/{slug}")
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("slug") or "") != slug:
            continue
        if bool(payload.get("closed")):
            continue
        out.append(market_row_from_payload(payload))
    out.sort(key=lambda x: (x["start_ts"] or 10**18, x["end_ts"] or 10**18, x["slug"]))
    return out


def choose_next_market(
    markets: List[Dict[str, Any]],
    now_ts: float,
    lead_seconds: float,
    completed: set,
) -> Optional[Dict[str, Any]]:
    # Prefer markets not yet completed and not already ended.
    candidates = [
        m
        for m in markets
        if m["slug"] not in completed and (m.get("end_ts") is None or m["end_ts"] > now_ts)
    ]
    if not candidates:
        return None

    # Ready = within lead window to start (or already started)
    ready = []
    for m in candidates:
        start_ts = m.get("start_ts")
        if start_ts is None or now_ts >= (start_ts - lead_seconds):
            ready.append(m)

    if ready:
        return min(ready, key=lambda m: (m.get("end_ts") or 10**18, m.get("start_ts") or 10**18))

    # Else earliest upcoming.
    return min(candidates, key=lambda m: (m.get("start_ts") or 10**18, m.get("end_ts") or 10**18))


def run_collector_for_market(
    *,
    slug: str,
    db: str,
    target_prices: str,
    market_end_grace_seconds: float,
    collector_script: str,
    print_trades: bool,
    print_fill_proxy: bool,
    extra_args: List[str],
) -> int:
    cmd = [
        sys.executable,
        collector_script,
        "--slug",
        slug,
        "--db",
        db,
        "--auto-stop-at-market-end",
        "--market-end-grace-seconds",
        str(market_end_grace_seconds),
        "--target-prices",
        target_prices,
    ]
    if print_trades:
        cmd.append("--print-trades")
    if print_fill_proxy:
        cmd.append("--print-fill-proxy")
    cmd.extend(extra_args)

    print(f"[runner] launch slug={slug}", flush=True)
    print(f"[runner] cmd={' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd)
    print(f"[runner] collector exit code={proc.returncode} slug={slug}", flush=True)
    return proc.returncode


def main() -> None:
    p = argparse.ArgumentParser(description="Auto-run collector for BTC 5m markets in sequence")
    p.add_argument("--db", default="polymarket_collector.db", help="Shared SQLite DB")
    p.add_argument("--lead-seconds", type=float, default=300.0, help="Start collecting this many seconds before eventStartTime (price-to-beat start)")
    p.add_argument("--poll-seconds", type=float, default=20.0, help="Discovery polling interval")
    p.add_argument("--market-end-grace-seconds", type=float, default=0.0, help="Extra seconds after endDate before collector stops")
    p.add_argument("--target-prices", default="0.49", help="Target prices passed to collector")
    p.add_argument("--collector-script", default="polymarket_continuous_collector.py", help="Collector script path")
    p.add_argument("--print-trades", action="store_true", help="Pass --print-trades to collector")
    p.add_argument("--print-fill-proxy", action="store_true", help="Pass --print-fill-proxy to collector")
    p.add_argument("--gamma-limit", type=int, default=1000, help="Gamma /markets limit")
    p.add_argument("--slug-probe-hours-back", type=int, default=1, help="Fallback probe window backward in hours when listings miss BTC 5m")
    p.add_argument("--slug-probe-hours-ahead", type=int, default=3, help="Fallback probe window forward in hours when listings miss BTC 5m")
    p.add_argument("--run-once", action="store_true", help="Discover and run at most one market, then exit")
    p.add_argument("--max-idle-sleep", type=float, default=300.0, help="Cap sleep when next market is far away")
    p.add_argument("collector_extra_args", nargs="*", help="Extra args passed through to collector")
    args = p.parse_args()

    completed = set()

    while True:
        now_ts = time.time()
        try:
            markets = discover_btc5m_markets(limit=args.gamma_limit)
        except Exception as exc:
            print(f"[runner] discovery error: {exc}", flush=True)
            time.sleep(args.poll_seconds)
            continue

        if not markets:
            try:
                markets = discover_btc5m_markets_by_slug_probe(
                    now_ts=now_ts,
                    hours_back=args.slug_probe_hours_back,
                    hours_ahead=args.slug_probe_hours_ahead,
                )
                if markets:
                    print(f"[runner] listing empty; slug-probe found {len(markets)} BTC 5m markets", flush=True)
            except Exception as exc:
                print(f"[runner] slug-probe error: {exc}", flush=True)

        chosen = choose_next_market(markets, now_ts, args.lead_seconds, completed)
        if chosen is None:
            print("[runner] no BTC 5m candidates found, sleeping", flush=True)
            time.sleep(args.poll_seconds)
            continue

        start_ts = chosen.get("start_ts")
        end_ts = chosen.get("end_ts")
        wait_until = (start_ts - args.lead_seconds) if start_ts is not None else now_ts
        seconds_until_run = wait_until - now_ts

        print(
            "[runner] next",
            f"slug={chosen['slug']}",
            f"start_utc={iso_utc(start_ts)}",
            f"end_utc={iso_utc(end_ts)}",
            f"active={chosen.get('active')}",
            f"acceptingOrders={chosen.get('acceptingOrders')}",
            f"enableOrderBook={chosen.get('enableOrderBook')}",
            f"run_in_s={round(max(seconds_until_run, 0), 1)}",
            flush=True,
        )

        if seconds_until_run > 0:
            sleep_s = min(seconds_until_run, args.max_idle_sleep, args.poll_seconds)
            time.sleep(max(1.0, sleep_s))
            continue

        rc = run_collector_for_market(
            slug=chosen["slug"],
            db=args.db,
            target_prices=args.target_prices,
            market_end_grace_seconds=args.market_end_grace_seconds,
            collector_script=args.collector_script,
            print_trades=args.print_trades,
            print_fill_proxy=args.print_fill_proxy,
            extra_args=args.collector_extra_args,
        )

        # Mark completed if market should be over by now or collector exited cleanly.
        if chosen.get("end_ts") and time.time() >= chosen["end_ts"] + args.market_end_grace_seconds:
            completed.add(chosen["slug"])
        elif rc == 0:
            # Avoid relaunch loops on same slug if collector exits quickly for any reason.
            completed.add(chosen["slug"])

        if args.run_once:
            break

        time.sleep(1.0)


if __name__ == "__main__":
    main()
