#!/usr/bin/env node
import { createWalletClient, encodeFunctionData, http, parseUnits } from "viem";
import { polygon } from "viem/chains";
import { privateKeyToAccount } from "viem/accounts";
import {
  BuilderConfig,
} from "@polymarket/builder-signing-sdk";
import {
  RelayClient,
  RelayerTxType,
} from "@polymarket/builder-relayer-client";

type Args = Record<string, string | boolean>;

function parseArgs(argv: string[]): Args {
  const out: Args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      out[key] = true;
    } else {
      out[key] = next;
      i++;
    }
  }
  return out;
}

function req(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

function help(): void {
  console.log(
    `Usage: tsx relayer_redeem_proxy.ts --condition-id 0x... --up-shares 0 --down-shares 5 [--route ctf|neg-risk] [--execute] [--wait]

Env required:
  POLY_PRIVATE_KEY
  POLY_BUILDER_API_KEY
  POLY_BUILDER_SECRET
  POLY_BUILDER_PASSPHRASE
Optional:
  POLY_RELAYER_HOST (default https://relayer-v2.polymarket.com/)
  POLY_NEG_RISK_ADAPTER (default Polymarket Polygon adapter)
  POLY_CTF_EXCHANGE (default Polymarket ConditionalTokens CTF contract)
  POLY_COLLATERAL_TOKEN (default Polygon USDC.e)
  POLY_RPC_URL (default https://polygon-rpc.com)
  POLY_FUNDER (used only for expected proxy comparison)
`
  );
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    help();
    return;
  }

  const conditionId = String(args["condition-id"] || "");
  if (!/^0x[0-9a-fA-F]{64}$/.test(conditionId)) {
    throw new Error(`Invalid --condition-id: ${conditionId}`);
  }

  const upShares = Number(args["up-shares"] ?? 0);
  const downShares = Number(args["down-shares"] ?? 0);
  if (!(upShares > 0 || downShares > 0)) {
    throw new Error("At least one of --up-shares / --down-shares must be > 0");
  }

  const execute = Boolean(args.execute);
  const wait = Boolean(args.wait);
  const route = String(args.route || process.env.POLY_REDEEM_ROUTE || "neg-risk").toLowerCase();

  const privateKey = req("POLY_PRIVATE_KEY") as `0x${string}`;
  const relayerUrl = process.env.POLY_RELAYER_HOST || "https://relayer-v2.polymarket.com/";
  const chainId = Number(process.env.POLY_CHAIN_ID || "137");
  const negRiskAdapter =
    (process.env.POLY_NEG_RISK_ADAPTER as `0x${string}`) ||
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";
  const ctfExchange =
    (process.env.POLY_CTF_EXCHANGE as `0x${string}`) ||
    "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
  const collateralToken =
    (process.env.POLY_COLLATERAL_TOKEN as `0x${string}`) ||
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
  const rpcUrl = process.env.POLY_RPC_URL || "https://polygon-bor-rpc.publicnode.com";

  const builderConfig = new BuilderConfig({
    localBuilderCreds: {
      key: req("POLY_BUILDER_API_KEY"),
      secret: req("POLY_BUILDER_SECRET"),
      passphrase: req("POLY_BUILDER_PASSPHRASE"),
    },
  });

  const account = privateKeyToAccount(privateKey);
  const walletClient = createWalletClient({
    account,
    chain: polygon,
    transport: http(rpcUrl),
  });

  const relayClient = new RelayClient(
    relayerUrl,
    chainId,
    walletClient,
    builderConfig,
    RelayerTxType.PROXY,
  );

  const relayPayload = await relayClient.getRelayPayload(account.address, "PROXY");
  const relayAddress = String((relayPayload as any)?.address || "");
  const relayNonce = String((relayPayload as any)?.nonce || "");
  const funder = process.env.POLY_FUNDER || "";
  // NOTE: relayPayload.address is the relay address, NOT the user's proxy wallet.
  console.log(
    `[relayer-auth] signer=${account.address} funder=${funder} relay=${relayAddress} nonce=${relayNonce}`
  );

  const upAmt = parseUnits(String(upShares), 6);
  const downAmt = parseUnits(String(downShares), 6);

  const zeroBytes32 = ("0x" + "00".repeat(32)) as `0x${string}`;
  let txTo: `0x${string}` = negRiskAdapter;
  let data: `0x${string}`;
  if (route === "ctf") {
    txTo = ctfExchange;
    data = encodeFunctionData({
      abi: [
        {
          type: "function",
          name: "redeemPositions",
          stateMutability: "nonpayable",
          inputs: [
            { name: "collateralToken", type: "address" },
            { name: "parentCollectionId", type: "bytes32" },
            { name: "conditionId", type: "bytes32" },
            { name: "indexSets", type: "uint256[]" },
          ],
          outputs: [],
        },
      ],
      functionName: "redeemPositions",
      args: [collateralToken, zeroBytes32, conditionId as `0x${string}`, [1n, 2n]],
    });
  } else {
    txTo = negRiskAdapter;
    data = encodeFunctionData({
      abi: [
        {
          type: "function",
          name: "redeemPositions",
          stateMutability: "nonpayable",
          inputs: [
            { name: "conditionId", type: "bytes32" },
            { name: "amounts", type: "uint256[]" },
          ],
          outputs: [],
        },
      ],
      functionName: "redeemPositions",
      args: [conditionId as `0x${string}`, [upAmt, downAmt]],
    });
  }

  console.log(
    `[redeem-plan] route=${route} condition_id=${conditionId} up_shares=${upShares} down_shares=${downShares} up_amt=${upAmt} down_amt=${downAmt} to=${txTo} collateral=${collateralToken} execute=${execute}`
  );

  if (!execute) {
    console.log("[redeem-dry-run] no transaction submitted");
    return;
  }

  const txResp = await relayClient.execute([
    { to: txTo, data, value: "0x0" },
  ], JSON.stringify({
      action: "redeem_positions",
      route,
      conditionId,
      upShares,
      downShares,
    }));

  console.log(
    `[redeem-submit] tx_id=${(txResp as any).transactionID ?? ""} tx_hash=${(txResp as any).transactionHash ?? (txResp as any).hash ?? ""} status=${(txResp as any).state ?? ""}`
  );

  if (wait && (txResp as any).transactionID) {
    const finalTx = await (txResp as any).wait();
    const finalState = String((finalTx as any)?.state ?? "").toUpperCase();
    console.log(
      `[redeem-wait] tx_id=${(txResp as any).transactionID} tx_hash=${(finalTx as any)?.transactionHash ?? (txResp as any).transactionHash ?? (txResp as any).hash ?? ""} status=${(finalTx as any)?.state ?? ""}`
    );
    if (!finalState) {
      throw new Error("Redeem tx wait returned no final state");
    }
    const okStates = new Set([
      "CONFIRMED",
      "SUCCESS",
      "SUCCEEDED",
      "EXECUTED",
      "MINED",
      "STATE_CONFIRMED",
      "STATE_MINED",
    ]);
    if (!okStates.has(finalState)) {
      throw new Error(`Redeem tx not confirmed successfully (state=${finalState})`);
    }
  }
}

main().catch((err) => {
  console.error(`[redeem-error] ${err?.message || err}`);
  process.exit(1);
});
