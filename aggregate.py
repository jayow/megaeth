"""Per-address aggregates from the transfers table.

Returns dicts in raw wei (use 1e18 to convert to MEGA).
Status taxonomy:
  - "claimed_held"     : claimed > 0, current >= claimed * 0.9 (kept ~all)
  - "claimed_partial"  : claimed > 0, 0 < current < claimed * 0.9
  - "claimed_dumped"   : claimed > 0, current == 0
  - "claimed_added"    : claimed > 0, bought > sold (net buyer on top of claim)
  - "buyer"            : never claimed, bought > 0
  - "received"         : received transfers but no claim/buy/sell on DEX
"""

import json
from decimal import Decimal

from db import connect


def status(claimed, bought, sold, current):
    c, b, s, cur = (Decimal(x) for x in (claimed, bought, sold, current))
    if c > 0:
        if b > s and b > 0:
            return "claimed_added"
        if cur == 0:
            return "claimed_dumped"
        if cur >= c * Decimal("0.9"):
            return "claimed_held"
        return "claimed_partial"
    if b > 0 or s > 0:
        return "buyer" if b >= s else "seller"
    return "received"


def per_address():
    """Return list of dicts: {address, claimed, bought, sold, received_other,
    sent_other, current, status}."""
    conn = connect()
    from db import get_state
    # All Distributor proxies (multiple distribution rounds = multiple proxies)
    proxies = {p.lower() for p in json.loads(get_state("distributor_proxies") or "[]")}
    primary = (get_state("claim_contract") or "").lower()
    if primary:
        proxies.add(primary)
    batchers = {b.lower() for b in json.loads(get_state("batch_senders") or "[]")}
    others = {o.lower() for o in json.loads(get_state("other_distributors") or "[]")}
    # MegaStakingProxy — sending MEGA here is staking, NOT selling
    staking_addrs = {"0x42bfaaa203b8259270a1b5ef4576db6b8359daa1"}
    # All Distributor proxies + batcher EOAs + any other detected distributors
    claim_sources = proxies | batchers | others
    dex = set(json.loads(get_state("dex_addresses") or "[]"))
    dex = {d.lower() for d in dex}

    # Build params safely (sqlite IN-clause). Note: we want claim and dex
    # separate so we can label.
    rows = conn.execute(
        "SELECT DISTINCT addr FROM ("
        "SELECT from_addr AS addr FROM transfers "
        "UNION SELECT to_addr AS addr FROM transfers"
        ")"
    ).fetchall()
    addrs = [r["addr"] for r in rows]

    # Pre-pull all transfers once and aggregate in Python (faster than 8k
    # queries for our size; ~100k rows fits easily in memory).
    all_t = conn.execute(
        "SELECT from_addr, to_addr, value FROM transfers"
    ).fetchall()

    # Authoritative current balances from balanceOf() (if available)
    # true_balance always exists (created by db.SCHEMA_STATEMENTS) — safe to query.
    true_bal = {}
    try:
        for r in conn.execute("SELECT address, balance FROM true_balance"):
            true_bal[r["address"]] = int(r["balance"])
    except Exception:
        pass

    claimed = {}
    bought = {}
    sold = {}
    staked = {}      # MEGA sent to MegaStaking
    unstaked = {}    # MEGA received back from MegaStaking
    recv_other = {}
    sent_other = {}
    inflow = {}
    outflow = {}

    for t in all_t:
        f, to, v = t["from_addr"], t["to_addr"], int(t["value"])
        inflow[to] = inflow.get(to, 0) + v
        outflow[f] = outflow.get(f, 0) + v

        # ---- INFLOW classification (from -> to) ----
        if f in claim_sources:
            claimed[to] = claimed.get(to, 0) + v
        elif f in dex:
            bought[to] = bought.get(to, 0) + v
        elif f in staking_addrs:
            unstaked[to] = unstaked.get(to, 0) + v
        else:
            recv_other[to] = recv_other.get(to, 0) + v

        # ---- OUTFLOW classification (from sends out) ----
        if to in dex:
            sold[f] = sold.get(f, 0) + v
        elif to in staking_addrs:
            staked[f] = staked.get(f, 0) + v
        elif to in claim_sources:
            pass
        else:
            sent_other[f] = sent_other.get(f, 0) + v

    out = []
    skip = claim_sources | dex | staking_addrs | {"0x0000000000000000000000000000000000000000"}
    for a in addrs:
        if a in skip:
            continue
        # Prefer authoritative balanceOf; fall back to inflow-outflow.
        if a in true_bal:
            cur = true_bal[a]
        else:
            cur = inflow.get(a, 0) - outflow.get(a, 0)
            if cur < 0:
                cur = 0
        rec = {
            "address": a,
            "claimed": claimed.get(a, 0),
            "bought": bought.get(a, 0),
            "sold": sold.get(a, 0),
            "staked": staked.get(a, 0),
            "unstaked": unstaked.get(a, 0),
            "received_other": recv_other.get(a, 0),
            "sent_other": sent_other.get(a, 0),
            "current": cur,
        }
        rec["status"] = status(rec["claimed"], rec["bought"], rec["sold"], rec["current"])
        out.append(rec)
    return out


def summary(rows):
    """Roll-up counts and totals (in raw wei)."""
    s = {
        "total_addresses": len(rows),
        "by_status": {},
        "totals": {"claimed": 0, "bought": 0, "sold": 0, "current": 0},
    }
    for r in rows:
        s["by_status"][r["status"]] = s["by_status"].get(r["status"], 0) + 1
        s["totals"]["claimed"] += r["claimed"]
        s["totals"]["bought"] += r["bought"]
        s["totals"]["sold"]   += r["sold"]
        s["totals"]["current"] += r["current"]
    return s


if __name__ == "__main__":
    rows = per_address()
    s = summary(rows)
    print("Total addresses:", s["total_addresses"])
    print("By status:", s["by_status"])
    print("Totals (MEGA):")
    for k, v in s["totals"].items():
        print(f"  {k}: {v / 10**18:,.2f}")
