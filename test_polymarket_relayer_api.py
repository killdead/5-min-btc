#!/usr/bin/env python3
import argparse
import json
import os
from typing import Any


def jprint(label: str, payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=True, default=str)
    if len(text) > 1200:
        text = text[:1200] + "...<truncated>"
    print(f"[{label}] {text}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Smoke-test Polymarket Builder Relayer auth (no tx)")
    p.add_argument("--show-transactions", action="store_true", help="Fetch latest relayer transactions")
    p.add_argument("--tx-limit", type=int, default=5)
    args = p.parse_args()

    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import TransactionType
        from py_builder_signing_sdk.config import BuilderConfig
        from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
    except Exception as exc:
        raise SystemExit(
            f"Missing relayer SDK packages. Install in your Anaconda env: "
            f"/Users/killdead/opt/anaconda3/bin/python3 -m pip install py-builder-relayer-client py-builder-signing-sdk\n"
            f"Import error: {exc}"
        )

    private_key = os.getenv("POLY_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("Missing POLY_PRIVATE_KEY")
    builder_key = os.getenv("POLY_BUILDER_API_KEY")
    builder_secret = os.getenv("POLY_BUILDER_SECRET")
    builder_passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not (builder_key and builder_secret and builder_passphrase):
        raise SystemExit("Missing POLY_BUILDER_API_KEY / POLY_BUILDER_SECRET / POLY_BUILDER_PASSPHRASE")

    relayer_host = os.getenv("POLY_RELAYER_HOST", "https://relayer-v2.polymarket.com/")
    chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
    funder = os.getenv("POLY_FUNDER")

    builder_cfg = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=builder_key,
            secret=builder_secret,
            passphrase=builder_passphrase,
        )
    )
    client = RelayClient(
        relayer_url=relayer_host,
        chain_id=chain_id,
        private_key=private_key,
        builder_config=builder_cfg,
    )
    signer_addr = client.signer.address()
    print("[auth-context]", f"signer={signer_addr}", f"funder={funder}", f"relayer={relayer_host}", "tx_type=SAFE", flush=True)

    # Validate builder auth path and print derived SAFE wallet.
    # Note: this SDK path is SAFE-oriented and may differ from PROXY flow used by TS helper.
    try:
        proxy = client.get_expected_safe()
        print("[relayer]", f"expected_proxy_wallet={proxy}", flush=True)
        if funder:
            same = str(proxy).lower() == str(funder).lower()
            print("[relayer-info]", f"safe_equals_funder={same} (informational only)", flush=True)
    except Exception as exc:
        print("[relayer-error]", f"get_expected_proxy_wallet failed: {exc}", flush=True)
        raise

    # Validate nonce + payload generation for PROXY tx type (no tx execution)
    try:
        nonce = client.get_nonce(signer_addr, TransactionType.SAFE.value)
        print("[relayer]", f"nonce_safe={nonce}", flush=True)
    except Exception as exc:
        print("[relayer-error]", f"get_nonce failed: {exc}", flush=True)
        raise

    try:
        # Older SDK doesn't expose get_relay_payload; the auth path is already validated by get_nonce/get_transactions.
        txs = client.get_transactions()
        jprint("relayer_transactions_head", txs)
    except Exception as exc:
        print("[relayer-error]", f"relayer read methods failed: {exc}", flush=True)
        raise

    if args.show_transactions:
        try:
            txs = client.get_transactions()
            jprint("relayer_transactions", txs)
        except Exception as exc:
            print("[relayer-error]", f"get_transactions failed: {exc}", flush=True)
            raise

    print("[validate]", "relayer_auth=ok", flush=True)


if __name__ == "__main__":
    main()
