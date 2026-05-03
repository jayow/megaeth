"""Precompute all dashboard data blobs and write them to the cache table.

Run on every cron tick. This does the ONE expensive read of the transfers
table, computes every aggregate the dashboard needs, and writes the
results as JSON blobs keyed by endpoint name. After this, API endpoints
serve from cache (single small read) instead of recomputing per request.

Cache keys written:
  snapshot:behavior  — full /api/behavior payload
  snapshot:summary   — full /api/summary payload
  snapshot:eligibility — /api/eligibility payload
  snapshot:timeline  — /api/timeline payload
  snapshot:meta      — counters and timestamps
"""

import json
import time

from db import connect, get_state, cache_set, transfer_count


def _all_claim_proxies():
    proxies = set(json.loads(get_state("distributor_proxies") or "[]"))
    primary = (get_state("claim_contract") or "").lower()
    if primary:
        proxies.add(primary)
    return [p.lower() for p in proxies if p]


def _to_mega(v):
    return f"{int(v) / 10**18:.4f}"


def build_behavior(rows_dict):
    """Equivalent of /api/behavior — returns a JSON-serializable dict."""
    conn = connect()

    # Eligibility sources
    fluffle = {r["address"] for r in conn.execute("SELECT address FROM fluffle_owner")}

    n_alloc = conn.execute("SELECT COUNT(*) FROM eth_allocation").fetchone()[0]
    echo_winners = set()
    if n_alloc > 0:
        echo_winners = {r["address"] for r in conn.execute(
            "SELECT DISTINCT e.address FROM eth_allocation a JOIN eth_entity e ON e.entity_id = a.entity_id"
        )}

    # Distributor Claimed event receivers
    eth_addrs_who_claimed = set()
    receiver_wallets_via_claims = set()
    eth_addrs_who_claimed = {r["address"] for r in conn.execute(
        """SELECT DISTINCT e.address FROM megaeth_claimed c
           JOIN eth_entity e ON e.entity_id = c.entity_uuid"""
    )}
    receiver_wallets_via_claims = {r["receiver"] for r in conn.execute(
        "SELECT DISTINCT receiver FROM megaeth_claimed"
    )}

    # Distributor recipients on MegaETH
    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]
    sources = _all_claim_proxies() + batchers
    received_set = set()
    if sources:
        ph = ",".join("?" * len(sources))
        for r in conn.execute(
            f"SELECT DISTINCT to_addr FROM transfers WHERE from_addr IN ({ph})", tuple(sources),
        ):
            received_set.add(r["to_addr"])

    eligible = fluffle | echo_winners | received_set | receiver_wallets_via_claims

    # Bucket every eligible wallet
    buckets = {"sold": [], "held": [], "staked": [], "bought_more": [], "not_claimed": []}
    for addr in eligible:
        r = rows_dict.get(addr)
        if not r or r["claimed"] == 0:
            buckets["not_claimed"].append(addr); continue
        if r.get("staked", 0) > 0:
            buckets["staked"].append(addr); continue
        if r["bought"] > r["sold"] and r["bought"] > 0:
            buckets["bought_more"].append(addr); continue
        if r["sold"] > 0:
            buckets["sold"].append(addr); continue
        buckets["held"].append(addr)

    # Pending breakdown
    pending_set = set(buckets["not_claimed"])
    pending_fluffle = len(pending_set & (fluffle - receiver_wallets_via_claims))
    pending_echo    = len(pending_set & echo_winners)
    pending_other   = max(0, len(pending_set) - pending_fluffle - pending_echo)

    # Cohort sets
    other_recv = received_set - fluffle - echo_winners - receiver_wallets_via_claims
    cohorts = {
        "fluffle":      fluffle,
        "conviction":   echo_winners,
        "echo_private": receiver_wallets_via_claims,
        "other":        other_recv,
    }

    # Per-round allocation accounting
    delivery_recipients = received_set
    def per_round_buckets(round_name, members):
        bk = {"held": 0, "sold": 0, "staked": 0, "bought_more": 0, "pending": 0, "locked_2027": 0}
        mega = {"sold": 0, "current": 0, "bought": 0, "claimed": 0, "staked": 0}
        for addr in members:
            r = rows_dict.get(addr)
            if (not r) or r["claimed"] == 0:
                if round_name == "conviction":
                    bk["locked_2027"] += 1
                else:
                    bk["pending"] += 1
                continue
            n_rounds_for_wallet = (
                (1 if addr in fluffle else 0)
                + (1 if addr in echo_winners and addr in delivery_recipients else 0)
                + (1 if addr in receiver_wallets_via_claims else 0)
                + (1 if addr in other_recv else 0)
            )
            share = 1.0 / max(1, n_rounds_for_wallet)
            sold_share   = r["sold"]   * share
            bought_share = r["bought"] * share
            mega["sold"]    += sold_share
            mega["current"] += r["current"]* share
            mega["bought"]  += bought_share
            mega["claimed"] += r["claimed"]* share
            mega["staked"]  += r.get("staked", 0) * share
            if r.get("staked", 0) > 0:
                bk["staked"] += 1
            elif bought_share > sold_share and bought_share > 0:
                bk["bought_more"] += 1
            elif sold_share > 0:
                bk["sold"] += 1
            else:
                bk["held"] += 1
        delivered = bk["held"] + bk["sold"] + bk["bought_more"] + bk["staked"]
        effective_held = bk["held"] + bk["staked"]
        return {
            "members": len(members),
            "buckets": bk,
            "mega": {k: _to_mega(int(v)) for k, v in mega.items()},
            "hold_rate": round(effective_held / delivered * 100, 1) if delivered else 0,
        }

    rounds_data = {
        "fluffle":      per_round_buckets("fluffle",      fluffle),
        "conviction":   per_round_buckets("conviction",   echo_winners),
        "echo_private": per_round_buckets("echo_private", receiver_wallets_via_claims),
        "other":        per_round_buckets("other",        other_recv),
    }

    # Per-bucket detail rows (top 500 each + sort key)
    def detail(addr):
        r = rows_dict.get(addr)
        sold_pct = None; kept_pct = None
        if r and (r["claimed"] + r["bought"]) > 0:
            received = r["claimed"] + r["bought"]
            sold_pct = round(min(100, r["sold"] / received * 100), 1)
            kept_pct = round(min(100, r["current"] / received * 100), 1) if received else None
        in_echo_private = addr in receiver_wallets_via_claims
        rounds = []
        if addr in fluffle:        rounds.append("fluffle")
        if addr in echo_winners:   rounds.append("conviction")
        if in_echo_private:        rounds.append("echo_private")
        if not rounds and r and r["claimed"] > 0: rounds.append("other")
        pending_reason = None
        if (not r) or r["claimed"] == 0:
            if addr in echo_winners and addr in fluffle:
                pending_reason = "Conviction Round lockup chosen → Apr 30, 2027 delivery (Fluffle delivery also missing)"
            elif addr in echo_winners:
                pending_reason = "Conviction Round lockup chosen → Apr 30, 2027 delivery"
            elif addr in fluffle:
                pending_reason = "Fluffle Round auto-delivery not yet sent"
            else:
                pending_reason = "No allocation found in known eligibility lists"
        out = {
            "address": addr,
            "claimed": _to_mega(r["claimed"]) if r else "0",
            "bought":  _to_mega(r["bought"])  if r else "0",
            "sold":    _to_mega(r["sold"])    if r else "0",
            "staked":  _to_mega(r.get("staked", 0)) if r else "0",
            "unstaked":_to_mega(r.get("unstaked", 0)) if r else "0",
            "current": _to_mega(r["current"]) if r else "0",
            "sold_pct": sold_pct,
            "kept_pct": kept_pct,
            "in_fluffle": addr in fluffle,
            "in_echo_winners": addr in echo_winners,
            "in_echo_private": in_echo_private,
            "rounds": rounds,
            "n_rounds": len(rounds),
            "pending_reason": pending_reason,
        }
        return out

    # Build totals
    totals_mega = {}
    for k, lst in buckets.items():
        s = {"claimed":0,"bought":0,"sold":0,"current":0,"staked":0}
        for a in lst:
            r = rows_dict.get(a)
            if not r: continue
            s["claimed"] += r["claimed"]; s["bought"] += r["bought"]
            s["sold"]    += r["sold"];    s["current"]+= r["current"]
            s["staked"]  += r.get("staked", 0)
        totals_mega[k] = {kk: _to_mega(vv) for kk, vv in s.items()}

    # Per-bucket sorted top-500 detail
    LIMIT = 500
    sortkey = {
        "sold":        lambda a: -(rows_dict[a]["sold"]    if a in rows_dict else 0),
        "held":        lambda a: -(rows_dict[a]["current"] if a in rows_dict else 0),
        "staked":      lambda a: -(rows_dict[a].get("staked", 0) if a in rows_dict else 0),
        "bought_more": lambda a: -(rows_dict[a]["bought"]  if a in rows_dict else 0),
        "not_claimed": lambda a: a,
    }
    rows_out = {}
    for k, lst in buckets.items():
        rows_out[k] = [detail(a) for a in sorted(lst, key=sortkey[k])[:LIMIT * 4]]  # 2000 per bucket

    return {
        "totals": {
            "eligible":     len(eligible),
            "sold":         len(buckets["sold"]),
            "held":         len(buckets["held"]),
            "staked":       len(buckets["staked"]),
            "bought_more":  len(buckets["bought_more"]),
            "not_claimed":  len(buckets["not_claimed"]),
        },
        "pending_by_reason": {
            "echo_sale_not_live":     pending_echo,
            "fluffle_not_delivered":  pending_fluffle,
            "other":                  pending_other,
        },
        "sources": {
            "fluffle_holders":          len(fluffle),
            "echo_auction_winners":     len(echo_winners),
            "received_from_distributor":len(received_set),
        },
        "cohorts":      {k: len(v) for k, v in cohorts.items()},
        "cohort_pct":   {k: round(len(v) / len(eligible) * 100, 2) if eligible else 0 for k, v in cohorts.items()},
        "rounds":       rounds_data,
        "totals_mega":  totals_mega,
        "rows":         rows_out,
    }


def build_summary(rows_dict, behavior):
    """Lighter version of /api/summary."""
    n = transfer_count()
    return {
        "by_status": {},  # legacy field, kept for compat
        "totals": behavior["totals_mega"],
        "totals_mega": behavior["totals_mega"],
        "total_addresses": behavior["totals"]["eligible"],
        "sync": {
            "transfers_in_db": n,
            "claim_contract":  get_state("claim_contract") or None,
            "dex_addresses":   json.loads(get_state("dex_addresses") or "[]"),
            "page_idx":        int(get_state("page_idx", "0")),
            "next_page_params":get_state("next_page_params") or None,
        },
        "supply": json.loads(get_state("supply_distribution") or "null"),
    }


def build_eligibility():
    conn = connect()
    fluffle_count = conn.execute("SELECT COUNT(*) FROM fluffle_owner").fetchone()[0]
    fluffle = {r["address"] for r in conn.execute("SELECT address FROM fluffle_owner")}

    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]
    delivery_sources = _all_claim_proxies() + batchers
    delivery_recipients = set()
    if delivery_sources:
        ph = ",".join("?" * len(delivery_sources))
        for r in conn.execute(
            f"SELECT DISTINCT to_addr FROM transfers WHERE from_addr IN ({ph})", tuple(delivery_sources),
        ):
            delivery_recipients.add(r["to_addr"])

    fluffle_recipients = fluffle & delivery_recipients
    fluffle_pending   = sorted(fluffle - delivery_recipients)

    n_entities = conn.execute("SELECT COUNT(*) FROM eth_entity").fetchone()[0]
    n_alloc    = conn.execute("SELECT COUNT(*) FROM eth_allocation").fetchone()[0]
    n_ref      = conn.execute("SELECT COUNT(*) FROM eth_refunded").fetchone()[0]

    echo_eligible = []
    if n_alloc > 0:
        echo_eligible = [r["address"] for r in conn.execute(
            "SELECT DISTINCT e.address FROM eth_allocation a JOIN eth_entity e ON e.entity_id = a.entity_id"
        )]
    echo_set = set(echo_eligible)
    delivered = echo_set & delivery_recipients
    pending   = sorted(echo_set - delivery_recipients)

    return {
        "fluffle": {
            "eligible_total": fluffle_count,
            "delivered":      len(fluffle_recipients),
            "pending":        len(fluffle_pending),
            "other_batcher_recipients": len(delivery_recipients - fluffle - echo_set),
            "pending_sample": fluffle_pending[:25],
        },
        "echo": {
            "entities_initialized":    n_entities,
            "allocations":             n_alloc,
            "refunded":                n_ref,
            "eligible_addresses":      len(echo_eligible),
            "delivered":               len(delivered),
            "pending":                 len(pending) if echo_eligible else None,
            "pending_sample":          pending[:25] if echo_eligible else [],
            "claimed":                 len(delivered),
            "claim_recipients_unmatched": 0,
        },
    }


STAKING_ADDR = "0x42bfaaa203b8259270a1b5ef4576db6b8359daa1"
MEGA_ADDR    = "0x28B7E77f82B25B95953825F1E3eA0E36c1c29861"

def build_staking():
    """Staking-specific aggregate: counts, hourly activity, top stakers."""
    conn = connect()

    # All transfers in/out of the staking contract
    in_rows = list(conn.execute(
        "SELECT timestamp, from_addr, value FROM transfers WHERE to_addr = ?",
        (STAKING_ADDR,),
    ))
    out_rows = list(conn.execute(
        "SELECT timestamp, to_addr, value FROM transfers WHERE from_addr = ?",
        (STAKING_ADDR,),
    ))

    # Per-staker totals
    by_addr = {}  # addr -> {staked, unstaked, last_ts_in, last_ts_out, n_in, n_out}
    for r in in_rows:
        a = r["from_addr"]
        v = int(r["value"])
        s = by_addr.setdefault(a, {"staked":0, "unstaked":0, "n_in":0, "n_out":0, "last_in":None, "last_out":None})
        s["staked"] += v; s["n_in"] += 1
        if not s["last_in"] or r["timestamp"] > s["last_in"]:
            s["last_in"] = r["timestamp"]
    for r in out_rows:
        a = r["to_addr"]
        v = int(r["value"])
        s = by_addr.setdefault(a, {"staked":0, "unstaked":0, "n_in":0, "n_out":0, "last_in":None, "last_out":None})
        s["unstaked"] += v; s["n_out"] += 1
        if not s["last_out"] or r["timestamp"] > s["last_out"]:
            s["last_out"] = r["timestamp"]

    # Hourly activity buckets
    hourly = {}
    for r in in_rows:
        ts = r["timestamp"]
        if not ts: continue
        h = ts[:13]
        b = hourly.setdefault(h, {"n_stake":0, "n_unstake":0, "mega_stake":0, "mega_unstake":0})
        b["n_stake"] += 1
        b["mega_stake"] += int(r["value"])
    for r in out_rows:
        ts = r["timestamp"]
        if not ts: continue
        h = ts[:13]
        b = hourly.setdefault(h, {"n_stake":0, "n_unstake":0, "mega_stake":0, "mega_unstake":0})
        b["n_unstake"] += 1
        b["mega_unstake"] += int(r["value"])

    series = []
    cumulative_n = 0
    cumulative_mega = 0.0
    for h in sorted(hourly):
        b = hourly[h]
        net_n   = b["n_stake"] - b["n_unstake"]
        net_mga = (b["mega_stake"] - b["mega_unstake"]) / 10**18
        cumulative_n    += net_n
        cumulative_mega += net_mga
        series.append({
            "hour":           h,
            "n_stake":        b["n_stake"],
            "n_unstake":      b["n_unstake"],
            "stake_mega":     round(b["mega_stake"]   / 10**18, 2),
            "unstake_mega":   round(b["mega_unstake"] / 10**18, 2),
            "net_n":          net_n,            # positive = net inflow
            "net_mega":       round(net_mga, 2),
            "cumul_n":        cumulative_n,     # running net stakers (events)
            "cumul_mega":     round(cumulative_mega, 2),
        })

    # Top stakers (by net staked)
    addrs = []
    for a, s in by_addr.items():
        net = s["staked"] - s["unstaked"]
        addrs.append({
            "address": a,
            "staked":      _to_mega(s["staked"]),
            "unstaked":    _to_mega(s["unstaked"]),
            "net_staked":  _to_mega(max(0, net)),
            "n_stake_tx":  s["n_in"],
            "n_unstake_tx":s["n_out"],
            "last_action": (s["last_in"] or "") if (s["last_in"] or "") > (s["last_out"] or "") else (s["last_out"] or ""),
        })
    addrs.sort(key=lambda x: -float(x["net_staked"]))

    # Totals
    total_in = sum(int(r["value"]) for r in in_rows)
    total_out = sum(int(r["value"]) for r in out_rows)
    active_now = sum(1 for s in by_addr.values() if s["staked"] - s["unstaked"] > 0)

    # Current contract balance — fetch live from chain to avoid stale
    # true_balance entries (the cached value was off by 60x at one point).
    try:
        import requests
        addr_padded = STAKING_ADDR.lower().replace("0x", "").rjust(64, "0")
        r = requests.post("https://mainnet.megaeth.com/rpc",
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                  "params": [{"to": MEGA_ADDR, "data": "0x70a08231" + addr_padded}, "latest"]},
            timeout=15)
        current_locked = int(r.json()["result"], 16)
    except Exception:
        # Fallback to cached if RPC fails (better than nothing)
        tb = conn.execute("SELECT balance FROM true_balance WHERE address = ?", (STAKING_ADDR,)).fetchone()
        current_locked = int(tb["balance"]) if tb else 0

    return {
        "totals": {
            "lifetime_stakers":  len(by_addr),
            "active_stakers":    active_now,
            "lifetime_staked":   _to_mega(total_in),
            "lifetime_unstaked": _to_mega(total_out),
            "currently_locked":  _to_mega(current_locked),
            "stake_events":      len(in_rows),
            "unstake_events":    len(out_rows),
        },
        "hourly":      series,
        "top_stakers": addrs,  # full list — frontend paginates
    }


def build_timeline():
    conn = connect()
    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]
    dex = {d.lower() for d in json.loads(get_state("dex_addresses") or "[]")}
    distrib = _all_claim_proxies() + batchers
    STAKING = {"0x42bfaaa203b8259270a1b5ef4576db6b8359daa1"}  # MegaStakingProxy

    buckets = {}
    for r in conn.execute(
        "SELECT timestamp, from_addr, to_addr, value FROM transfers WHERE timestamp IS NOT NULL"
    ):
        ts = r["timestamp"]
        if not ts: continue
        hour = ts[:13]
        b = buckets.setdefault(hour, {
            "claims":0, "sells":0, "buys":0, "stakes":0,
            "n_claim":0, "n_sell":0, "n_buy":0, "n_stake":0,
        })
        v = int(r["value"])
        if r["from_addr"] in distrib:
            b["claims"] += v; b["n_claim"] += 1
        elif r["to_addr"] in STAKING:
            b["stakes"] += v; b["n_stake"] += 1
        elif r["from_addr"] in dex:
            b["buys"]   += v; b["n_buy"]   += 1
        elif r["to_addr"] in dex:
            b["sells"]  += v; b["n_sell"]  += 1

    series = []
    for hour in sorted(buckets):
        b = buckets[hour]
        series.append({
            "hour":         hour,
            "claims_mega":  round(b["claims"] / 10**18, 2),
            "sells_mega":   round(b["sells"]  / 10**18, 2),
            "buys_mega":    round(b["buys"]   / 10**18, 2),
            "stakes_mega":  round(b["stakes"] / 10**18, 2),
            "n_claim":      b["n_claim"],
            "n_sell":       b["n_sell"],
            "n_buy":        b["n_buy"],
            "n_stake":      b["n_stake"],
        })
    return {"series": series}


def run(write_static=True, write_cache=False):
    """Compute everything once and write JSON snapshots.

    `write_static=True` writes /static/data/{key}.json files — the main mode
    used by the GitHub Actions cron. These files are committed to the repo
    and served directly by Vercel's CDN (zero DB cost).

    `write_cache=True` also writes to the `cache` table — kept as an option
    if you ever want to serve from a DB instead of static files.
    """
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] snapshot.run()  static={write_static}  cache={write_cache}")

    # The one heavy read — load all transfers + balances into Python memory once
    from aggregate import per_address
    print("  → per_address() (the one heavy aggregate)")
    rows_list = per_address()
    rows_dict = {r["address"]: r for r in rows_list}
    print(f"    {len(rows_list):,} addresses aggregated in {time.time()-t0:.1f}s")

    print("  → build_behavior / eligibility / timeline / summary / staking")
    beh  = build_behavior(rows_dict)
    elig = build_eligibility()
    tl   = build_timeline()
    sm   = build_summary(rows_dict, beh)
    stk  = build_staking()
    meta = {
        "built_at":  int(time.time()),
        "transfers": transfer_count(),
        "addresses": len(rows_list),
    }

    blobs = {
        "behavior":    beh,
        "summary":     sm,
        "eligibility": elig,
        "timeline":    tl,
        "staking":     stk,
        "meta":        meta,
    }

    if write_static:
        import os
        out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "data")
        os.makedirs(out_dir, exist_ok=True)
        for name, value in blobs.items():
            path = os.path.join(out_dir, f"{name}.json")
            with open(path, "w") as f:
                json.dump(value, f, separators=(",", ":"))  # minified
            sz = os.path.getsize(path)
            print(f"    wrote {path}  ({sz:,} bytes)")

    if write_cache:
        now = meta["built_at"]
        for name, value in blobs.items():
            cache_set(f"snapshot:{name}", json.dumps(value), now)
        print("    also wrote to cache table")

    print(f"[{time.strftime('%H:%M:%S')}] snapshot complete in {time.time()-t0:.1f}s")
    return {"ok": True, "elapsed": round(time.time()-t0, 1), "addresses": len(rows_list)}


if __name__ == "__main__":
    run()
