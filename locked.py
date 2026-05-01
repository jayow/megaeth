"""Identify and categorize 'locked' MEGA holders.

Categories:
  - claim_pool : tokens still sitting in the Distributor proxy
                 (eligible-but-not-yet-claimed)
  - treasury   : Safe multi-sig wallets (team / foundation / investor locks)
  - dex_liquidity : MEGA held inside detected DEX pair contracts
  - other_contracts : unverified contracts holding MEGA (could be vesting,
                       lockers, bridges, etc.)
  - circulating : tokens held by EOAs

Uses Blockscout's `/api/v2/tokens/{token}/holders` for the full holder list
and `/api/v2/addresses/{addr}` for type detection (cached).
"""

import json
import time

import requests

from config import BLOCKSCOUT, TOKEN
from db import connect, get_state, set_state, upsert_meta


SAFE_IMPL_NAMES = {"SafeL2", "Safe", "GnosisSafeL2", "GnosisSafe"}
META_CACHE_FILE = "meta_cache.json"


def load_meta_cache():
    try:
        with open(META_CACHE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_meta_cache(cache):
    with open(META_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def fetch_addr_meta(addr, cache):
    addr = addr.lower()
    if addr in cache:
        return cache[addr]
    try:
        r = requests.get(f"{BLOCKSCOUT}/api/v2/addresses/{addr}", timeout=15)
        if r.status_code != 200:
            cache[addr] = {"is_contract": False, "impl": None, "name": None}
            return cache[addr]
        data = r.json()
        impls = data.get("implementations") or []
        impl_name = impls[0].get("name") if impls else None
        cache[addr] = {
            "is_contract": bool(data.get("is_contract")),
            "impl": impl_name,
            "name": data.get("name"),
        }
    except Exception:
        cache[addr] = {"is_contract": False, "impl": None, "name": None}
    return cache[addr]


def fetch_all_holders():
    """Walk paginated /tokens/{TOKEN}/holders. Returns [(addr, balance_int), ...]."""
    out = []
    params = {}
    while True:
        r = requests.get(f"{BLOCKSCOUT}/api/v2/tokens/{TOKEN}/holders", params=params, timeout=30)
        if r.status_code != 200:
            time.sleep(2)
            continue
        data = r.json()
        for it in data.get("items") or []:
            addr = (it.get("address") or {}).get("hash", "").lower()
            val = int(it.get("value", "0"))
            if addr:
                out.append((addr, val))
        npp = data.get("next_page_params")
        if not npp:
            break
        params = npp
        if len(out) % 500 == 0:
            print(f"  pulled {len(out)} holders…")
    return out


def categorize(holders, claim_addr, dex_addrs, cache):
    cats = {
        "claim_pool": 0,
        "treasury_safe": 0,
        "dex_liquidity": 0,
        "other_contracts": 0,
        "circulating_eoa": 0,
    }
    detail = {k: [] for k in cats}

    claim_addr = (claim_addr or "").lower()
    dex_addrs = {d.lower() for d in dex_addrs}

    for i, (addr, bal) in enumerate(holders):
        if addr == claim_addr:
            cats["claim_pool"] += bal
            detail["claim_pool"].append((addr, bal))
            continue
        if addr in dex_addrs:
            cats["dex_liquidity"] += bal
            detail["dex_liquidity"].append((addr, bal))
            continue
        meta = fetch_addr_meta(addr, cache)
        if meta["is_contract"]:
            if meta["impl"] in SAFE_IMPL_NAMES or "Safe" in (meta["impl"] or ""):
                cats["treasury_safe"] += bal
                detail["treasury_safe"].append((addr, bal))
            else:
                cats["other_contracts"] += bal
                detail["other_contracts"].append((addr, bal))
        else:
            cats["circulating_eoa"] += bal
            detail["circulating_eoa"].append((addr, bal))
        if i % 50 == 0 and i:
            save_meta_cache(cache)
    save_meta_cache(cache)
    return cats, detail


def run():
    print("Pulling all holders from Blockscout…")
    holders = fetch_all_holders()
    print(f"Fetched {len(holders)} holders")

    claim = (get_state("claim_contract") or "").lower() or None
    dex = json.loads(get_state("dex_addresses") or "[]")
    print(f"Claim: {claim}")
    print(f"DEX:   {dex}")

    cache = load_meta_cache()
    cats, detail = categorize(holders, claim, dex, cache)

    # Persist labels to DB so the main dashboard can use them.
    for kind, items in detail.items():
        label = {
            "claim_pool": "claim_pool",
            "treasury_safe": "treasury",
            "dex_liquidity": "dex",
            "other_contracts": "contract",
            "circulating_eoa": None,
        }[kind]
        if not label:
            continue
        for addr, _ in items:
            upsert_meta(addr, label=label, is_contract=1 if label != "circulating_eoa" else 0)

    out = {
        "by_category": {k: str(v) for k, v in cats.items()},
        "by_category_mega": {k: f"{v / 10**18:.4f}" for k, v in cats.items()},
        "top_locked": {
            k: [(a, str(b), f"{b / 10**18:.4f}") for a, b in sorted(items, key=lambda x: -x[1])[:10]]
            for k, items in detail.items()
        },
        "total_holders": len(holders),
    }
    set_state("supply_distribution", json.dumps(out))
    print("\n=== Supply distribution ===")
    total = sum(cats.values()) / 10**18
    for k, v in cats.items():
        m = v / 10**18
        pct = (m / total * 100) if total else 0
        print(f"  {k:20s} {m:>16,.2f} MEGA  ({pct:5.2f}%)")
    print(f"  {'TOTAL':20s} {total:>16,.2f} MEGA")
    print("\nSaved to sync_state['supply_distribution']")


if __name__ == "__main__":
    run()
