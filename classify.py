"""Classify addresses by inferring distributors, DEX pairs, treasuries.

Distribution channels (per the official MegaETH token page):
  * Echo signature-claim distributor  - smart contract proxy, on-chain pool
  * Fluffle auto-delivery batcher     - EOA used for batch transfers
  * Other batch senders (Mainnet Campaign, SONAR, KPI rewards via Flux)

Heuristic: any address that has sent MEGA to >= MIN_UNIQ unique recipients
AND whose unique-to-total ratio is high (>0.8 = each recipient mostly got
one transfer = airdrop-like fan-out) AND is not a known DEX. Both
contracts (Distributor proxies) and EOAs (batch-sender bots) count.

DEX detection unchanged: contract address with substantial two-way swap
activity across many counterparties.
"""

import json

import requests

from config import BLOCKSCOUT, ZERO_ADDR
from db import connect, set_state, upsert_meta


MIN_UNIQ = 80           # at least this many unique recipients
MIN_RATIO = 0.55        # uniq/total >= this means fan-out distribution
META_CACHE = {}

# Known special contracts that should NOT be classified as DEX even
# though they have high two-way activity.
STAKING_ADDRS = {"0x42bfaaa203b8259270a1b5ef4576db6b8359daa1"}  # MegaStaking

# DEX aggregators / routers that route trades through pools.
# A transfer from a user TO one of these = a sell (the aggregator forwards
# to the actual pool). Add new ones here as we identify them.
KNOWN_AGGREGATORS = {
    "0x956df8424b556f0076e8abf5481605f5a791cc7f": "MagpieRouterV3_1",
}

# EOAs that act as one-way MEGA sinks at significant scale — almost
# certainly CEX deposit hot wallets (consolidate small inflows, send to
# cold storage). Sending MEGA to one of these = a sell to a CEX.
# Detected by heuristic: ≥100 unique senders, very low outflow uniqueness.
KNOWN_CEX_SINKS = {
    "0x7e3042bddcbf60928074ea0a5907c5ed5ae57e91": "CEX-like sink #1 (171M MEGA, 1,910 senders)",
    "0x57b83aaff113ef81a729b63274ed6f17404c9ba6": "CEX-like sink #2 (59M MEGA, 285 senders)",
    "0x1b158617ff3aa88f41439b20cdc50b03d0758fc3": "CEX-like sink #3 (51M MEGA, 430 senders)",
    "0xc2cc15728b677343b7c330ca147b8b635d9b08c4": "CEX-like sink #4 (30M MEGA, 46 senders)",
    "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23": "CEX-like sink #5 (27M MEGA, 108 senders)",
    "0x700f1874314a4df3bb9a10f7b5ea187a39d8de74": "CEX-like sink #6 (5.7M MEGA, 303 senders)",
}


def fetch_address_meta(addr):
    if addr in META_CACHE:
        return META_CACHE[addr]
    try:
        r = requests.get(f"{BLOCKSCOUT}/api/v2/addresses/{addr}", timeout=15)
        if r.status_code == 200:
            data = r.json()
            impls = data.get("implementations") or []
            meta = {
                "is_contract": bool(data.get("is_contract")),
                "name": data.get("name"),
                "implementation_name": impls[0].get("name") if impls else None,
                "implementation_addr": impls[0].get("address_hash") if impls else None,
            }
        else:
            meta = {"is_contract": False, "name": None, "implementation_name": None, "implementation_addr": None}
    except Exception:
        meta = {"is_contract": False, "name": None, "implementation_name": None, "implementation_addr": None}
    META_CACHE[addr] = meta
    return meta


def detect_distributors(conn):
    """Return list of dicts: {addr, uniq, n, kind} where kind in
    {echo_distributor, batch_sender, unknown_distributor}."""
    rows = conn.execute(
        """
        SELECT from_addr, COUNT(DISTINCT to_addr) AS uniq, COUNT(*) AS n
        FROM transfers
        WHERE from_addr != ?
        GROUP BY from_addr
        HAVING COUNT(DISTINCT to_addr) >= ?
        ORDER BY uniq DESC
        LIMIT 30
        """,
        (ZERO_ADDR, MIN_UNIQ),
    ).fetchall()

    print("Top fan-out senders (candidates):")
    distributors = []
    for r in rows:
        ratio = r["uniq"] / r["n"] if r["n"] else 0
        meta = fetch_address_meta(r["from_addr"])
        kind = None
        if meta["implementation_name"] == "Distributor":
            kind = "echo_distributor"
        elif ratio >= MIN_RATIO and r["uniq"] >= MIN_UNIQ:
            # Could be DEX router though - filter out contracts that ALSO
            # receive many transfers (DEX-like).
            inflow = conn.execute(
                "SELECT COUNT(DISTINCT from_addr) AS uniq_in FROM transfers WHERE to_addr = ?",
                (r["from_addr"],),
            ).fetchone()["uniq_in"]
            if inflow < r["uniq"] / 5:  # not a DEX
                kind = "batch_sender" if not meta["is_contract"] else "unknown_distributor"
        marker = f" <-- {kind}" if kind else ""
        print(f"  {r['from_addr']}  uniq={r['uniq']:>5}  n={r['n']:>5}  ratio={ratio:.2f}  "
              f"{'contract' if meta['is_contract'] else 'EOA':8s} impl={meta['implementation_name']}{marker}")
        if kind:
            distributors.append({
                "address": r["from_addr"],
                "uniq": r["uniq"],
                "n": r["n"],
                "kind": kind,
                "is_contract": meta["is_contract"],
                "implementation": meta["implementation_name"],
            })
            label = {
                "echo_distributor": "claim",
                "batch_sender": "batch_sender",
                "unknown_distributor": "distributor",
            }[kind]
            upsert_meta(r["from_addr"], label=label, is_contract=1 if meta["is_contract"] else 0)
    return distributors


def detect_dex_addresses(conn, distributor_addrs):
    """DEX = contract address with high two-way activity across many counterparties."""
    skip = set(distributor_addrs) | {ZERO_ADDR}
    placeholders = ",".join("?" * len(skip)) if skip else "''"
    rows = conn.execute(
        f"""
        SELECT addr, CASE WHEN uin < uout THEN uin ELSE uout END AS swap_score, uin, uout, n_in, n_out
        FROM (
            SELECT a.addr,
                   COALESCE(SUM(CASE WHEN t.to_addr   = a.addr THEN 1 ELSE 0 END), 0) AS n_in,
                   COALESCE(SUM(CASE WHEN t.from_addr = a.addr THEN 1 ELSE 0 END), 0) AS n_out,
                   COUNT(DISTINCT CASE WHEN t.to_addr   = a.addr THEN t.from_addr END) AS uin,
                   COUNT(DISTINCT CASE WHEN t.from_addr = a.addr THEN t.to_addr   END) AS uout
            FROM (
                SELECT DISTINCT from_addr AS addr FROM transfers
                UNION SELECT DISTINCT to_addr AS addr FROM transfers
            ) a
            JOIN transfers t ON t.from_addr = a.addr OR t.to_addr = a.addr
            GROUP BY a.addr
        )
        WHERE addr NOT IN ({placeholders})
        ORDER BY swap_score DESC
        LIMIT 15
        """,
        tuple(skip),
    ).fetchall()

    print("\nTop two-way activity candidates (DEX detection):")
    dex = []
    for r in rows:
        meta = fetch_address_meta(r["addr"])
        kind = "contract" if meta["is_contract"] else "EOA"
        marker = ""
        if r["addr"] in STAKING_ADDRS:
            upsert_meta(r["addr"], label="staking", is_contract=1, note="MegaStaking")
            marker = "  (staking — excluded from DEX)"
        elif meta["is_contract"] and r["uin"] >= 25 and r["uout"] >= 25:
            dex.append(r["addr"])
            upsert_meta(r["addr"], label="dex", is_contract=1, note=f"auto: uin={r['uin']}, uout={r['uout']}")
            marker = "  <-- DEX"
        print(f"  {r['addr']}  uin={r['uin']:>5} uout={r['uout']:>5} n_in={r['n_in']:>5} n_out={r['n_out']:>5}  [{kind}]{marker}")
    return set(dex)


def run():
    conn = connect()
    print("=== Detecting distribution channels ===")
    distributors = detect_distributors(conn)
    distributor_addrs = [d["address"] for d in distributors]
    print(f"\nFound {len(distributors)} distribution channels.")

    # Capture EVERY Distributor proxy (multiple may exist for separate
    # distribution rounds). Keep the first as the primary "claim_contract"
    # for backward compat; expose all via "distributor_proxies".
    echo_proxies = [d["address"] for d in distributors if d["kind"] == "echo_distributor"]
    echo_primary = echo_proxies[0] if echo_proxies else None
    batchers = [d["address"] for d in distributors if d["kind"] == "batch_sender"]
    others = [d["address"] for d in distributors if d["kind"] == "unknown_distributor"]

    print("\n=== Detecting DEX pair(s) ===")
    dex = detect_dex_addresses(conn, distributor_addrs)

    # Sells go to: detected DEX pairs + known aggregators/routers + known CEX sinks.
    sell_sinks = sorted(set(dex) | set(KNOWN_AGGREGATORS.keys()) | set(KNOWN_CEX_SINKS.keys()))
    for a in KNOWN_AGGREGATORS:
        upsert_meta(a, label="aggregator", is_contract=1, note=KNOWN_AGGREGATORS[a])
    for a in KNOWN_CEX_SINKS:
        upsert_meta(a, label="cex_sink", is_contract=0, note=KNOWN_CEX_SINKS[a])

    set_state("claim_contract",     echo_primary or "")
    set_state("distributor_proxies",json.dumps(echo_proxies))
    set_state("batch_senders",      json.dumps(batchers))
    set_state("other_distributors", json.dumps(others))
    set_state("distributors",       json.dumps(distributors))
    set_state("dex_addresses",      json.dumps(sell_sinks))   # combined sell venues
    set_state("dex_pairs_only",     json.dumps(sorted(dex)))  # original AMM pairs
    set_state("aggregators",        json.dumps(sorted(KNOWN_AGGREGATORS.keys())))
    set_state("cex_sinks",          json.dumps(sorted(KNOWN_CEX_SINKS.keys())))
    print(f"\nSaved: echo_proxies={echo_proxies}, batchers={batchers}, others={others}")
    print(f"       sell_sinks (DEX + aggregators + CEX): {len(sell_sinks)} total")


if __name__ == "__main__":
    run()
