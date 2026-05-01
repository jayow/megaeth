"""Pull `Claimed` events from the Distributor proxy on MegaETH.

The event signature is:
  Claimed(bytes16 indexed tokenDistributionUUID,
          bytes16 indexed entityUUID,
          address indexed receiver,
          uint256 amount,
          uint256 amountUSDC,
          uint256 amountCarry)

Each event tells us:
  - WHO claimed (entityUUID = the off-chain identity from Echo)
  - WHERE the MEGA went (receiver = MegaETH wallet)
  - HOW MUCH (amount in MEGA wei)

By cross-referencing entityUUID with `eth_entity` (entityUUID -> Ethereum
address from MegaSale's EntityInitialized), we get the TRUE picture of
which Ethereum identities have claimed, regardless of whether they used
the same wallet on both chains.
"""

import sys
import time

import requests

from db import connect


MEGA_RPC = "https://mainnet.megaeth.com/rpc"
DISTRIBUTOR = "0xcf4b83ce5273adaeb0221b645240f0b68678d7a1"
TOPIC_CLAIMED = "0xddc469e1e78b774ed8fa261ecf2cb0081b304697ade1665e57a5e2d627134375"
DEPLOY_BLOCK_GUESS = 1_592_579  # MEGA token deploy; distributor is later


def init_schema():
    # Schema lives in db.py SCHEMA_STATEMENTS; connect() creates it.
    return connect()


def rpc(method, params):
    for attempt in range(6):
        try:
            r = requests.post(MEGA_RPC, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "result" in data:
                return data["result"]
            if "error" in data and attempt < 5:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"RPC error: {data}")
        except requests.RequestException as e:
            print(f"  retry {attempt+1}: {e}", file=sys.stderr)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"RPC failed: {method}")


def fetch_logs_paged(address, topics, from_block, to_block, max_chunk=200_000):
    """Pull eth_getLogs in chunks, halving on size errors."""
    out = []
    cur = from_block
    chunk = max_chunk
    while cur <= to_block:
        end = min(cur + chunk - 1, to_block)
        try:
            res = rpc("eth_getLogs", [{
                "address": address,
                "topics": topics,
                "fromBlock": hex(cur),
                "toBlock": hex(end),
            }])
            out.extend(res)
            print(f"  blocks {cur:>10,}..{end:>10,}: +{len(res)}  (total {len(out)})")
            cur = end + 1
            chunk = min(chunk * 2, max_chunk)
        except RuntimeError as e:
            msg = str(e).lower()
            if any(k in msg for k in ("limit", "too many", "rang", "exceed")):
                chunk = max(chunk // 2, 1000)
                print(f"  shrink chunk to {chunk}")
                continue
            raise
    return out


def parse_amount(data, slot):
    """Extract uint256 at slot N (0-indexed) from data hex."""
    start = 2 + slot * 64
    return int(data[start:start + 64], 16)


def run():
    conn = init_schema()
    head = int(rpc("eth_blockNumber", []), 16)
    # Resume from highest seen block + 1
    last = conn.execute("SELECT COALESCE(MAX(block), 0) FROM megaeth_claimed").fetchone()[0]
    start = max(DEPLOY_BLOCK_GUESS, last)
    print(f"Pulling Claimed events from block {start:,} to {head:,}")

    logs = fetch_logs_paged(DISTRIBUTOR, [TOPIC_CLAIMED], start, head)
    print(f"\nGot {len(logs)} Claimed logs")

    rows = []
    for lg in logs:
        topics = lg.get("topics") or []
        if len(topics) < 4:
            continue
        # topics[1]=distribUUID(bytes16 in 32), topics[2]=entityUUID(bytes16 in 32), topics[3]=receiver(address)
        dist  = "0x" + topics[1][2:34].lower()
        ent   = "0x" + topics[2][2:34].lower()
        recv  = "0x" + topics[3][-40:].lower()
        data = lg.get("data") or "0x"
        amount       = str(parse_amount(data, 0)) if len(data) >= 66  else "0"
        amount_usdc  = str(parse_amount(data, 1)) if len(data) >= 130 else "0"
        amount_carry = str(parse_amount(data, 2)) if len(data) >= 194 else "0"
        rows.append((
            lg["transactionHash"], int(lg["logIndex"], 16), int(lg["blockNumber"], 16),
            dist, ent, recv, amount, amount_usdc, amount_carry,
        ))

    conn.executemany(
        "INSERT INTO megaeth_claimed("
        "tx_hash,log_index,block,distribution_uuid,entity_uuid,receiver,"
        "amount,amount_usdc,amount_carry) VALUES(?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(tx_hash, log_index) DO NOTHING",
        rows,
    )
    conn.commit()

    n_total      = conn.execute("SELECT COUNT(*) FROM megaeth_claimed").fetchone()[0]
    n_entities   = conn.execute("SELECT COUNT(DISTINCT entity_uuid) FROM megaeth_claimed").fetchone()[0]
    n_receivers  = conn.execute("SELECT COUNT(DISTINCT receiver) FROM megaeth_claimed").fetchone()[0]
    n_distrib    = conn.execute("SELECT COUNT(DISTINCT distribution_uuid) FROM megaeth_claimed").fetchone()[0]
    total_amt    = conn.execute("SELECT SUM(CAST(amount AS REAL))/1e18 FROM megaeth_claimed").fetchone()[0] or 0
    print(f"\n=== MegaETH Claimed events ===")
    print(f"  Total Claimed events:          {n_total:>6,}")
    print(f"  Unique entityUUIDs claimed:    {n_entities:>6,}")
    print(f"  Unique receiver wallets:       {n_receivers:>6,}")
    print(f"  Distinct distribution rounds:  {n_distrib:>6,}")
    print(f"  Total MEGA claimed:            {total_amt:>16,.2f}")

    # Cross-reference with Ethereum: entities whose UUID matches MegaSale
    n_match = conn.execute(
        """SELECT COUNT(DISTINCT c.entity_uuid)
           FROM megaeth_claimed c
           JOIN eth_entity e ON e.entity_id = c.entity_uuid"""
    ).fetchone()[0]
    print(f"\n  Of those, matched to a MegaSale entity: {n_match:,}")

    # Per-distribution breakdown
    print("\nClaims per distribution UUID:")
    for r in conn.execute(
        "SELECT distribution_uuid, COUNT(*) AS n, SUM(CAST(amount AS REAL))/1e18 AS total FROM megaeth_claimed GROUP BY distribution_uuid ORDER BY total DESC"
    ):
        print(f"  {r['distribution_uuid']}  n={r['n']:>5}  total={r['total']:>16,.2f} MEGA")


if __name__ == "__main__":
    run()
