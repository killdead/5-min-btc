#!/usr/bin/env python3
import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from btc5m_auto_runner import (
    choose_next_market,
    discover_btc5m_markets,
    discover_btc5m_markets_by_slug_probe,
    iso_utc,
)


def launch_paper_bot(
    *,
    slug: str,
    db: str,
    target_price: float,
    size_shares: float,
    lead_seconds: float,
    paper_poll_seconds: float,
    bot_script: str,
    extra_args: List[str],
) -> subprocess.Popen:
    cmd = [
        sys.executable,
        bot_script,
        "--db",
        db,
        "--slug",
        slug,
        "--mode",
        "paper-live",
        "--target-price",
        str(target_price),
        "--size-shares",
        str(size_shares),
        "--lead-seconds",
        str(lead_seconds),
        "--paper-poll-seconds",
        str(paper_poll_seconds),
    ]
    cmd.extend(extra_args)
    print(f"[paper-runner] launch slug={slug}", flush=True)
    print(f"[paper-runner] cmd={' '.join(cmd)}", flush=True)
    return subprocess.Popen(cmd)


def main() -> None:
    p = argparse.ArgumentParser(description="Auto scheduler for BTC 5m paper-live bot")
    p.add_argument("--db", default="polymarket_collector.db")
    p.add_argument("--target-price", type=float, default=0.49)
    p.add_argument("--size-shares", type=float, default=5.0)
    p.add_argument("--lead-seconds", type=float, default=300.0, help="Start paper bot this many seconds before eventStartTime")
    p.add_argument("--paper-poll-seconds", type=float, default=1.0)
    p.add_argument("--max-concurrent", type=int, default=2, help="Parallel paper bots (needed due overlapping pre-open windows)")
    p.add_argument("--poll-seconds", type=float, default=15.0, help="Scheduler discovery loop interval")
    p.add_argument("--gamma-limit", type=int, default=1000)
    p.add_argument("--slug-probe-hours-back", type=int, default=1)
    p.add_argument("--slug-probe-hours-ahead", type=int, default=3)
    p.add_argument("--bot-script", default="btc5m_trading_bot.py")
    p.add_argument("--run-seconds", type=float, default=0.0, help="Optional scheduler runtime cap (0=forever)")
    p.add_argument("--run-once", action="store_true", help="Launch at most one paper bot then exit")
    p.add_argument("bot_extra_args", nargs="*", help="Extra args passed to btc5m_trading_bot.py (use -- before them)")
    args = p.parse_args()

    started = time.time()
    active: Dict[str, subprocess.Popen] = {}
    active_meta: Dict[str, dict] = {}
    completed: Set[str] = set()
    launched_count = 0

    while True:
        if args.run_seconds > 0 and time.time() - started >= args.run_seconds:
            print("[paper-runner] run-seconds reached, stopping scheduler loop", flush=True)
            break

        # Reap finished children.
        for slug, proc in list(active.items()):
            rc = proc.poll()
            if rc is None:
                continue
            print(f"[paper-runner] child exit slug={slug} rc={rc}", flush=True)
            active.pop(slug, None)
            meta = active_meta.pop(slug, None)
            if meta and meta.get("end_ts") and time.time() >= meta["end_ts"]:
                completed.add(slug)
            else:
                # Avoid tight relaunch loops; mark completed for this session.
                completed.add(slug)

        now_ts = time.time()
        try:
            markets = discover_btc5m_markets(limit=args.gamma_limit)
        except Exception as exc:
            print(f"[paper-runner] listing discovery error: {exc}", flush=True)
            markets = []
        if not markets:
            try:
                markets = discover_btc5m_markets_by_slug_probe(
                    now_ts=now_ts,
                    hours_back=args.slug_probe_hours_back,
                    hours_ahead=args.slug_probe_hours_ahead,
                )
                if markets:
                    print(f"[paper-runner] listing empty; slug-probe found {len(markets)} BTC 5m markets", flush=True)
            except Exception as exc:
                print(f"[paper-runner] slug-probe error: {exc}", flush=True)
                markets = []

        if not markets:
            print("[paper-runner] no BTC 5m candidates found, sleeping", flush=True)
            time.sleep(args.poll_seconds)
            continue

        # Launch all eligible markets up to concurrency.
        eligible = []
        for m in markets:
            slug = m["slug"]
            if slug in completed or slug in active:
                continue
            end_ts = m.get("end_ts")
            if end_ts is not None and end_ts <= now_ts:
                completed.add(slug)
                continue
            start_ts = m.get("start_ts")
            if start_ts is None:
                continue
            if now_ts >= (start_ts - args.lead_seconds):
                eligible.append(m)

        eligible.sort(key=lambda m: (m.get("start_ts") or 10**18, m.get("slug")))

        for m in eligible:
            if len(active) >= args.max_concurrent:
                break
            slug = m["slug"]
            print(
                "[paper-runner] schedule",
                f"slug={slug}",
                f"start_utc={iso_utc(m.get('start_ts'))}",
                f"end_utc={iso_utc(m.get('end_ts'))}",
                f"active={m.get('active')}",
                f"acceptingOrders={m.get('acceptingOrders')}",
                flush=True,
            )
            proc = launch_paper_bot(
                slug=slug,
                db=args.db,
                target_price=args.target_price,
                size_shares=args.size_shares,
                lead_seconds=args.lead_seconds,
                paper_poll_seconds=args.paper_poll_seconds,
                bot_script=args.bot_script,
                extra_args=args.bot_extra_args,
            )
            active[slug] = proc
            active_meta[slug] = m
            launched_count += 1
            if args.run_once:
                break

        if args.run_once and launched_count > 0:
            break

        # Helpful heartbeat if nothing launched.
        if not eligible:
            nxt = choose_next_market(markets, now_ts, args.lead_seconds, completed)
            if nxt:
                run_in = max(0.0, (nxt.get("start_ts") or now_ts) - args.lead_seconds - now_ts)
                print(
                    "[paper-runner] next",
                    f"slug={nxt['slug']}",
                    f"start_utc={iso_utc(nxt.get('start_ts'))}",
                    f"run_in_s={round(run_in,1)}",
                    f"active_children={len(active)}",
                    flush=True,
                )

        time.sleep(args.poll_seconds)

    # Scheduler exiting; do not kill children by default (let them finish).
    print(f"[paper-runner] exiting scheduler; active_children={len(active)}", flush=True)


if __name__ == "__main__":
    main()
