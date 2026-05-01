"""Pull MegaETH Ethereum-mainnet contract data:

  * MegaSale (Echo Public Sale auction): all EntityInitialized events
    map entityID -> wallet, all AllocationSet events list winning entityIDs.
    Intersection = the on-chain Echo eligibility list.

  * Fluffle NFT (soulbound, 5,000 holders): the holder set IS the
    Fluffle airdrop eligibility list.

Stores results in `mega.db` so the dashboard can cross-reference with
MegaETH-side claims.
"""

import json
import sys
import time

import requests

from db import connect


ETH_RPC = "https://ethereum-rpc.publicnode.com"

MEGASALE   = "0xab02bf85a7a851b6a379ea3d5bd3b9b4f5dd8461"
FLUFFLE    = "0x4e502ab1bb313b3c1311eb0d11b31a6b62988b86"

TOPIC_ENTITY_INIT  = "0x1fd864eedf348e273a59630d7391e57ac24aaed7e25228cecde30c995799c410"
TOPIC_ALLOCATION   = "0x2a8c364c867b048e22d5b19971b1e9e959da4aa25ea45edc8d25e7ff36cc1f51"
TOPIC_BID_PLACED   = "0x804b42e7e171589ed3879c1882898c8dd15f3d4168afc3292253f5d9ebabc7be"
TOPIC_REFUNDED     = "0xc899f65d555fa732cfff132a257eadf835200b245c57c1aa0bff0b7ffd29360f"
TOPIC_TRANSFER     = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO32             = "0x0000000000000000000000000000000000000000000000000000000000000000"


def init_eth_schema():
    # Schema lives in db.py now (SCHEMA_STATEMENTS); connect() ensures it.
    return connect()


def rpc(method, params):
    for attempt in range(6):
        try:
            r = requests.post(ETH_RPC, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "result" in data:
                return data["result"]
            if "error" in data:
                err = data["error"]
                # rate-limit / transient
                if attempt < 5 and ("limit" in str(err).lower() or "timeout" in str(err).lower()):
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"RPC error: {err}")
        except requests.RequestException as e:
            print(f"  retry {attempt+1}: {e}", file=sys.stderr)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"RPC failed: {method}")


def hex_to_int(h):
    return int(h, 16)


def fetch_logs_paged(address, topics, from_block, to_block, max_chunk=10_000):
    """Adaptive paged eth_getLogs. Halves the chunk on size errors."""
    out = []
    cur = from_block
    chunk = max_chunk
    while cur <= to_block:
        end = min(cur + chunk - 1, to_block)
        try:
            params = [{
                "address": address,
                "topics": topics,
                "fromBlock": hex(cur),
                "toBlock": hex(end),
            }]
            res = rpc("eth_getLogs", params)
            out.extend(res)
            print(f"    blocks {cur:>10,}..{end:>10,}: +{len(res)} (total {len(out)})")
            cur = end + 1
            # Tentatively grow back up, but cap.
            chunk = min(chunk * 2, max_chunk)
        except RuntimeError as e:
            msg = str(e).lower()
            if any(k in msg for k in ("limit", "too many", "rang", "exceed", "timeout")):
                chunk = max(chunk // 2, 1000)
                print(f"    shrink chunk to {chunk} due to {e}")
                continue
            raise
    return out


def find_creation_block(addr):
    """Binary-search the deployment block for a contract."""
    head = hex_to_int(rpc("eth_blockNumber", []))
    lo, hi = 0, head
    # Confirm code at head
    if rpc("eth_getCode", [addr, hex(head)]) in ("0x", ""):
        raise RuntimeError(f"{addr} has no code at head {head}")
    while lo < hi:
        mid = (lo + hi) // 2
        code = rpc("eth_getCode", [addr, hex(mid)])
        if code in ("0x", ""):
            lo = mid + 1
        else:
            hi = mid
    return lo


def pull_megasale():
    conn = init_eth_schema()
    print("Finding MegaSale deployment block on Ethereum…")
    deploy = find_creation_block(MEGASALE)
    head = hex_to_int(rpc("eth_blockNumber", []))
    print(f"  deployed at block {deploy:,}, head is {head:,}")

    print("\n--- EntityInitialized events ---")
    init_logs = fetch_logs_paged(MEGASALE, [TOPIC_ENTITY_INIT], deploy, head)
    for lg in init_logs:
        # topics[1] = entityID (bytes16 padded to 32), topics[2] = address (padded)
        if len(lg["topics"]) < 3:
            continue
        entity = "0x" + lg["topics"][1][2:34].lower()  # first 16 bytes
        addr   = "0x" + lg["topics"][2][-40:].lower()
        block  = hex_to_int(lg["blockNumber"])
        conn.execute(
            "INSERT INTO eth_entity(entity_id,address,block) VALUES(?,?,?) ON CONFLICT(entity_id) DO NOTHING",
            (entity, addr, block),
        )
    conn.commit()
    n_entities = conn.execute("SELECT COUNT(*) FROM eth_entity").fetchone()[0]
    print(f"  stored {n_entities} entities")

    print("\n--- BidPlaced events ---")
    bid_logs = fetch_logs_paged(MEGASALE, [TOPIC_BID_PLACED], deploy, head)
    for lg in bid_logs:
        if len(lg["topics"]) < 2:
            continue
        entity = "0x" + lg["topics"][1][2:34].lower()
        block  = hex_to_int(lg["blockNumber"])
        conn.execute(
            "INSERT INTO eth_bid(entity_id,block) VALUES(?,?) ON CONFLICT(entity_id, block) DO NOTHING",
            (entity, block),
        )
    conn.commit()
    n_bids = conn.execute("SELECT COUNT(*) FROM eth_bid").fetchone()[0]
    n_bidders = conn.execute("SELECT COUNT(DISTINCT entity_id) FROM eth_bid").fetchone()[0]
    print(f"  stored {n_bids} bids from {n_bidders} entities")

    print("\n--- AllocationSet events (winning bids) ---")
    alloc_logs = fetch_logs_paged(MEGASALE, [TOPIC_ALLOCATION], deploy, head)
    for lg in alloc_logs:
        if len(lg["topics"]) < 2:
            continue
        entity = "0x" + lg["topics"][1][2:34].lower()
        block  = hex_to_int(lg["blockNumber"])
        # data = abi.encode(uint256 acceptedAmountUSDT)
        data = lg.get("data") or "0x"
        accepted = str(int(data[2:66], 16)) if len(data) >= 66 else "0"
        conn.execute(
            "INSERT INTO eth_allocation(entity_id,accepted_usdt,block) VALUES(?,?,?) "
            "ON CONFLICT(entity_id) DO UPDATE SET accepted_usdt = EXCLUDED.accepted_usdt, block = EXCLUDED.block",
            (entity, accepted, block),
        )
    conn.commit()
    n_alloc = conn.execute("SELECT COUNT(*) FROM eth_allocation").fetchone()[0]
    total_usdt = conn.execute("SELECT SUM(CAST(accepted_usdt AS REAL))/1e6 FROM eth_allocation").fetchone()[0] or 0
    print(f"  stored {n_alloc} allocations, total accepted: {total_usdt:,.2f} USDT")

    print("\n--- Refunded events ---")
    ref_logs = fetch_logs_paged(MEGASALE, [TOPIC_REFUNDED], deploy, head)
    for lg in ref_logs:
        if len(lg["topics"]) < 3:
            continue
        entity = "0x" + lg["topics"][1][2:34].lower()
        addr   = "0x" + lg["topics"][2][-40:].lower()
        block  = hex_to_int(lg["blockNumber"])
        conn.execute(
            "INSERT INTO eth_refunded(entity_id,address,block) VALUES(?,?,?) ON CONFLICT(entity_id) DO NOTHING",
            (entity, addr, block),
        )
    conn.commit()
    n_ref = conn.execute("SELECT COUNT(*) FROM eth_refunded").fetchone()[0]
    print(f"  stored {n_ref} refunds")


def pull_fluffle():
    conn = init_eth_schema()
    print("\nFinding Fluffle NFT deployment block on Ethereum…")
    deploy = find_creation_block(FLUFFLE)
    head = hex_to_int(rpc("eth_blockNumber", []))
    print(f"  deployed at block {deploy:,}, head is {head:,}")

    print("\n--- Fluffle Transfer events (mints + transfers, but soulbound so only mints) ---")
    logs = fetch_logs_paged(FLUFFLE, [TOPIC_TRANSFER, ZERO32], deploy, head)

    owners = {}
    for lg in logs:
        if len(lg["topics"]) < 3:
            continue
        # topics[1] = from (zero), topics[2] = to (owner)
        owner = "0x" + lg["topics"][2][-40:].lower()
        owners[owner] = owners.get(owner, 0) + 1

    conn.executemany(
        "INSERT INTO fluffle_owner(address,n_tokens) VALUES(?,?) ON CONFLICT(address) DO UPDATE SET n_tokens = EXCLUDED.n_tokens",
        [(a, n) for a, n in owners.items()],
    )
    conn.commit()
    print(f"  stored {len(owners)} unique Fluffle holders")


def summary():
    conn = init_eth_schema()
    n_entities = conn.execute("SELECT COUNT(*) FROM eth_entity").fetchone()[0]
    n_bidders  = conn.execute("SELECT COUNT(DISTINCT entity_id) FROM eth_bid").fetchone()[0]
    n_alloc    = conn.execute("SELECT COUNT(*) FROM eth_allocation").fetchone()[0]
    n_ref      = conn.execute("SELECT COUNT(*) FROM eth_refunded").fetchone()[0]
    n_fluffle  = conn.execute("SELECT COUNT(*) FROM fluffle_owner").fetchone()[0]

    # Echo-eligible addresses = entities with allocation that aren't refunded.
    eligible = conn.execute(
        """
        SELECT DISTINCT e.address
        FROM eth_allocation a
        JOIN eth_entity e ON e.entity_id = a.entity_id
        WHERE a.entity_id NOT IN (SELECT entity_id FROM eth_refunded)
        """
    ).fetchall()
    n_echo_addrs = len(eligible)

    print("\n=== Ethereum-side summary ===")
    print(f"  Entities initialized:    {n_entities:>6,}")
    print(f"  Bidders (unique):        {n_bidders:>6,}")
    print(f"  Allocations granted:     {n_alloc:>6,}")
    print(f"  Refunded entities:       {n_ref:>6,}")
    print(f"  Echo-eligible addresses: {n_echo_addrs:>6,}  (allocation - refunds, mapped to wallet)")
    print(f"  Fluffle NFT holders:     {n_fluffle:>6,}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd in ("all", "megasale"):
        pull_megasale()
    if cmd in ("all", "fluffle"):
        pull_fluffle()
    summary()
