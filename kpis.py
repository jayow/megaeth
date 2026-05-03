"""KPI tracker for the MegaETH 53% reward pool.

Source of truth: MegaStaking proxy (0x42bf…) on MegaETH mainnet. We never
hard-code KPI thresholds. The flow is:

  1. sync_onchain_kpis.py pulls KpiCreated + TrancheCreated +
     TrancheStatusUpdated + TrancheAchievementDataSet events into
     static/data/onchain_kpis.json.
  2. This module loads that file, fetches the live measurement for each
     KPI from its public source, parses the threshold out of the on-chain
     tranche label, and reports met/unmet + on-chain attestation status.
  3. static/data/kpis.json is consumed by the /kpis dashboard.

If the Foundation adds a new tranche or attests one, the next sync picks
it up and the dashboard updates with no code change.
"""

import calendar
import datetime
import json
import os
import re
import time

import requests


# ─── Configuration ────────────────────────────────────────────────────────
USDM_ADDR     = "0xfafddbb3fc7688494971a79cc65dca3ef82079e7"

# USDM "treasury & escrow" exclusions per the KPI spec ("USDM circulating
# supply ≥ $X (excluding treasury & escrow)"). The carve-out applies whenever
# USDm was created via a privileged/protocol-side mint rather than organic
# user acquisition (e.g. swap, bridge from a chain where they paid for it).
USDM_TREASURY_ADDRS = {
    # Ethena-side issuer bootstrap EOA. USDm is an Ethena-issued stablecoin
    # for MegaETH (same xUSDOFTUpgradeable contract pattern Ethena uses for
    # USDtb/USDe cross-chain). This address:
    #   - Received $499,999,988 USDm via the OFT-Executor mint path (30 events)
    #   - Deposited 100% into Aave's USDm market
    #   - Still holds 100% of the aUSDm receipts ($500,002,916, exactly $2,916
    #     over the $500M tranche threshold — engineered)
    #   - Holds $140M aEthUSDtb on Ethereum L1 (USDtb is Ethena's product)
    #   - Funded by 0x2d4d… vanity wallets which directly transfer USDm to
    #     EthenaMinting (0xe34902…) and hold $200M+ in stables
    # Verdict: Ethena (USDm issuer) bootstrapping liquidity in their own
    # stablecoin on a partner chain. Not organic user adoption.
    "0xb8734a14fbd4aa2d44e6aa830405ffc861ba313c",
}

# Aave aToken for USDm — its USDm balance is the underlying for the hopper's
# aUSDm receipts. Excluding it avoids double-counting bootstrap liquidity that
# round-trips through Aave.
USDM_AAVE_ATOKEN = "0x5dF82810CB4B8f3e0Da3c031cCc9208ee9cF9500"

# Confirmed protocol-governance Safes (Gnosis Safe multisigs on MegaETH).
# Tracked for context; their USDm balances should be excluded if they ever
# hold any (currently none do, but this guards against future treasury moves).
PROTOCOL_GOVERNANCE_SAFES = {
    "0xffef73c0892ba0511ea696d638cc9d65e63c2dcf",  # main treasury (top MEGA holder, 1.98B MEGA)
    "0x141c03abc062a9d5d4238e1666b849c929e53cb2",  # USDm contract owner
    "0xb765214d479840c35555682a1204409252e1996b",  # MegaStaking contract owner
}
MEGA_RPC      = "https://mainnet.megaeth.com/rpc"
ETH_RPC       = "https://ethereum-rpc.publicnode.com"
ETH_BRIDGE    = "0x0CA3A2FBC3D770b578223FBB6b062fa875a2eE75"
ETH_GENESIS   = 1438269973           # 2015-07-30 15:26:13 UTC
GLAMSTERDAM_SHIPPED = False
HEGOTA_SHIPPED      = False
LATEST_HARDFORK     = "Pectra (May 2025)"
DUNE_API_KEY  = os.environ.get("DUNE_API_KEY", "").strip()

# 2026-04-30 06:00 EST = 10:00 UTC. Working assumption until Foundation confirms.
TGE_TIMESTAMP = calendar.timegm((2026, 4, 30, 10, 0, 0, 0, 0, 0))
TGE_ISO       = "2026-04-30T10:00:00Z"

ONCHAIN_KPIS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "static", "data", "onchain_kpis.json")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Hanyon-MEGA-KPI/2.0"})


def rpc(url, method, params):
    r = SESSION.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=30)
    r.raise_for_status()
    return r.json()


def tge_window_status(window_days=30, now=None):
    now = now if now is not None else time.time()
    elapsed = now - TGE_TIMESTAMP
    window_seconds = window_days * 86400
    pct = max(0.0, min(100.0, (elapsed / window_seconds) * 100)) if window_seconds else 0
    return {
        "elapsed_seconds": elapsed,
        "elapsed_days":    elapsed / 86400,
        "window_seconds":  window_seconds,
        "window_end_ts":   TGE_TIMESTAMP + window_seconds,
        "is_closed":       elapsed >= window_seconds,
        "percent_elapsed": pct,
    }


# ═══ Live data fetchers — one per measurement type ═══════════════════════════

def fetch_native_apps():
    """All MegaETH-native app tokens on CoinGecko, sorted by mcap desc.

    Native = `asset_platform_id == "mega-eth"` on CoinGecko. Excludes
    wrapped/bridged tokens by name and protocol tokens (USDM, MEGA).
    """
    EXCLUDE_SYMBOL      = {"usdt0", "usde", "weth", "usdm", "mega"}
    EXCLUDE_NAME_PREFIX = ("wrapped ", "bridged ", "l2 standard")
    try:
        r = SESSION.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "category": "megaeth-ecosystem",
                    "order": "market_cap_desc", "per_page": 50, "page": 1},
            timeout=15,
        )
        r.raise_for_status()
        coins = r.json()
    except Exception:
        return None

    candidates = []
    for c in coins:
        sym, name = (c.get("symbol") or "").lower(), (c.get("name") or "").lower()
        if sym in EXCLUDE_SYMBOL: continue
        if any(name.startswith(p) for p in EXCLUDE_NAME_PREFIX): continue
        if not c.get("market_cap") or c["market_cap"] <= 0: continue
        candidates.append(c)

    apps, skipped = [], []
    for c in candidates:
        try:
            d = SESSION.get(
                f"https://api.coingecko.com/api/v3/coins/{c['id']}",
                params={"localization": "false", "tickers": "false", "market_data": "false",
                        "community_data": "false", "developer_data": "false", "sparkline": "false"},
                timeout=15,
            ).json()
            platform = (d.get("asset_platform_id") or "").lower()
        except Exception:
            platform = ""
        rec = {"symbol": c["symbol"], "name": c["name"],
               "mcap_usd": c["market_cap"], "platform": platform}
        if platform in ("mega-eth", "megaeth"): apps.append(rec)
        else: skipped.append(rec)

    return {"apps": apps, "non_native_skipped": skipped}


def _usdm_balance_of(addr):
    """Read USDm balance of an address via balanceOf."""
    selector = "0x70a08231"
    arg = addr.lower().replace("0x", "").rjust(64, "0")
    res = rpc(MEGA_RPC, "eth_call", [{"to": USDM_ADDR, "data": selector + arg}, "latest"])
    if "error" in res or not res.get("result"): return 0
    return int(res["result"], 16) / 10**18


def fetch_usdm_supply():
    """Total + circulating-ex-treasury USDm supply.

    `total` is contract totalSupply() — what naive readers will see.
    `circulating_ex_treasury` excludes USDM_TREASURY_ADDRS and the Aave aToken
    (whose balance is ~entirely backed by Foundation bootstrap deposits).

    The KPI spec is "circulating supply ≥ $X (excluding treasury & escrow)" —
    so `circulating_ex_treasury` is what the threshold should compare against.
    """
    res = rpc(MEGA_RPC, "eth_call", [{"to": USDM_ADDR, "data": "0x18160ddd"}, "latest"])
    if "error" in res: return None
    total = int(res["result"], 16) / 10**18

    # Two readings of "treasury & escrow":
    #
    # STRICT (legal/literal): only tokens in protocol governance Safes or
    # vesting contracts. Aave deposits = circulating (anyone can borrow).
    # → matches what Foundation likely uses for attestation
    # → currently equals total supply (Safes hold 0 USDm)
    #
    # ECONOMIC (issuer-bootstrap excluded): also exclude tokens minted by
    # the issuer to themselves and parked in DeFi pools without organic
    # acquisition. Useful sanity check on whether the threshold reflects
    # real adoption.
    #
    # We report BOTH so users can see how big the gap is.

    strict_breakdown = []   # only protocol governance Safes
    strict_total = 0.0
    for safe in PROTOCOL_GOVERNANCE_SAFES:
        bal = _usdm_balance_of(safe)
        if bal > 0:
            strict_breakdown.append({"address": safe, "balance": bal, "kind": "governance_safe"})
            strict_total += bal

    economic_breakdown = list(strict_breakdown)
    economic_total = strict_total
    for addr in USDM_TREASURY_ADDRS:
        bal = _usdm_balance_of(addr)
        economic_breakdown.append({"address": addr, "balance": bal, "kind": "ethena_issuer_hopper"})
        economic_total += bal
    aave_bal = _usdm_balance_of(USDM_AAVE_ATOKEN)
    economic_breakdown.append({"address": USDM_AAVE_ATOKEN, "balance": aave_bal,
                               "kind": "aave_atoken_backed_by_ethena_bootstrap"})
    economic_total += aave_bal

    # Default to STRICT reading for the tranche evaluator (matches what the
    # Foundation will likely attest), but expose ECONOMIC for transparency.
    treasury_breakdown = strict_breakdown
    treasury_total = strict_total

    return {
        "total":                   total,
        "treasury_total":          treasury_total,
        "circulating_ex_treasury": max(0, total - treasury_total),
        "treasury_breakdown":      treasury_breakdown,
        # Side-by-side for the dashboard:
        "strict": {
            "excluded":    strict_total,
            "circulating": max(0, total - strict_total),
            "breakdown":   strict_breakdown,
            "definition":  "Excludes only protocol-governance Safes. Matches the literal spec language and is what the Foundation is most likely to attest.",
        },
        "economic": {
            "excluded":    economic_total,
            "circulating": max(0, total - economic_total),
            "breakdown":   economic_breakdown,
            "definition":  "Also excludes Ethena-issuer bootstrap (the $500M they minted to themselves and parked in Aave). Reflects real organic demand.",
        },
    }


def dune_query(query_id, max_age_hours=24, limit=None):
    if not DUNE_API_KEY: return None
    params = {"max_age_hours": max_age_hours}
    if limit is not None: params["limit"] = limit
    try:
        r = SESSION.get(
            f"https://api.dune.com/api/v1/query/{query_id}/results",
            headers={"X-Dune-API-Key": DUNE_API_KEY},
            params=params, timeout=15,
        )
        if r.status_code != 200: return {"error": f"Dune {r.status_code}", "rows": []}
        d = r.json()
        return {"rows": d.get("result", {}).get("rows", []), "metadata": d.get("metadata", {})}
    except Exception as e:
        return {"error": str(e), "rows": []}


def fetch_eth_bridged_dune():
    """Total ETH bridged into MegaETH per Dune query 6807132."""
    inflow  = dune_query(6807132, limit=1)
    outflow = dune_query(6818119, limit=1)
    net     = dune_query(6822951, limit=1)
    daily   = dune_query(6818279, limit=400)
    if not (inflow and inflow.get("rows")): return None
    def first(d, k):
        try: return float(d["rows"][0].get(k) or 0)
        except: return 0.0
    series = []
    for r in (daily or {}).get("rows", []) or []:
        try:
            series.append({"day": str(r.get("day", ""))[:10],
                           "inflow": float(r.get("inflow_eth") or 0),
                           "outflow": float(r.get("outflow_eth") or 0)})
        except: continue
    series.sort(key=lambda x: x["day"])
    total_in  = first(inflow, "total_inflow_eth")
    total_out = first(outflow or {}, "total_outflow_eth")
    return {
        "total_inflow_eth": total_in,
        "total_outflow_eth": total_out,
        "net_inflow_eth": first(net or {}, "net_inflow_eth") or (total_in - total_out),
        "series": series,
    }


def fetch_gas_dune():
    """Monthly TGas series from Dune query 6816766."""
    res = dune_query(6816766, limit=120)
    if not res or not res.get("rows"): return None
    months = []
    for r in res["rows"]:
        try:
            tgas = float(r.get("Gas Used (TGas)") or 0)
            months.append({"month": str(r["month"])[:10], "tgas": round(tgas, 3)})
        except: continue
    months.sort(key=lambda x: x["month"])
    if not months: return None
    today_month = time.strftime("%Y-%m-01", time.gmtime())
    completed = [m for m in months if m["month"] < today_month]
    return {
        "series": months,
        "latest_month": months[-1],
        "latest_completed": completed[-1] if completed else None,
        "month_to_evaluate": (completed[-1] if completed else months[-1])["tgas"],
    }


def fetch_block_times_dune():
    res = dune_query(6822619, limit=120)
    if not res or not res.get("rows"): return None
    months = []
    for r in res["rows"]:
        try:
            month_str = str(r.get("month", ""))[:10]
            blocks    = int(r.get("block_count") or 0)
            if not month_str or blocks <= 0: continue
            y, m, _ = [int(p) for p in month_str.split("-")]
            next_m = datetime.date(y + (1 if m == 12 else 0), 1 if m == 12 else m + 1, 1)
            secs = (next_m - datetime.date(y, m, 1)).days * 86400
            months.append({"month": month_str, "blocks": blocks,
                           "avg_block_ms": round(secs / blocks * 1000, 1)})
        except: continue
    months.sort(key=lambda x: x["month"])
    today_month = time.strftime("%Y-%m-01", time.gmtime())
    completed   = [m for m in months if m["month"] < today_month]
    latest      = completed[-1] if completed else (months[-1] if months else None)
    return {"series": months, "latest_completed": latest,
            "current_block_ms": latest["avg_block_ms"] if latest else None}


def fetch_block_gaps_dune():
    """Monthly >5sec block gap counts (Dune 6822636)."""
    res = dune_query(6822636, limit=120)
    if not res or not res.get("rows"): return None
    months = []
    for r in res["rows"]:
        try:
            month_str = str(r.get("month", ""))[:10]
            gaps = int(r.get("gap_count") or 0)
            total_gap_sec = float(r.get("total_gaps") or 0)
            if not month_str: continue
            months.append({"month": month_str, "gaps": gaps, "gap_seconds": total_gap_sec})
        except: continue
    months.sort(key=lambda x: x["month"])
    return {"series": months}


def fetch_eth_block_times_dune():
    res = dune_query(6822056, limit=1)
    if not res or not res.get("rows"): return None
    return res["rows"][0]


def fetch_l2beat_risks():
    try:
        html = SESSION.get("https://l2beat.com/scaling/projects/megaeth", timeout=15).text
    except Exception:
        return None
    out = {}
    patterns = {
        "stateValidation":  r'"stateValidation":\{"value":"([^"]+)"',
        "exitWindow":       r'"exitWindow":\{"value":"([^"]+)"',
        "dataAvailability": r'"dataAvailability":\{"value":"([^"]+)"',
        "proposerFailure":  r'"proposerFailure":\{"value":"([^"]+)"',
        "sequencerFailure": r'"sequencerFailure":\{"value":"([^"]+)"',
    }
    for k, p in patterns.items():
        m = re.search(p, html)
        out[k] = m.group(1) if m else None
    return out


def fetch_eth_uptime_years():
    return (time.time() - ETH_GENESIS) / (365.25 * 86400)


# ═══ Per-tranche evaluator ════════════════════════════════════════════════════
# Each KPI's tranches share a measurement; the threshold is parsed out of the
# on-chain tranche label. If the Foundation changes a label, this naturally
# keeps up. If a label format we don't recognize lands, we return "unparsable"
# rather than guess.

_USD_MAGNITUDE = {"M": 1_000_000, "B": 1_000_000_000}


def _parse_int(s):
    return int(s.replace(",", ""))


def evaluate_tranche(kpi_id, label, measurements):
    """Return dict: {met, percent, gap, kind, observed, reason}.

    `kind` describes what we actually checked (so the UI can label it),
    `observed` is the live value, `reason` is human prose for the caveat.
    """
    M = measurements

    # KPI 0 — Native App Mcap ─ "≥ N MegaETH-native app(s) with market cap ≥ $XM"
    if kpi_id == 0:
        m = re.search(r"≥\s*(\d+)\s*MegaETH-native apps?\s*with\s*market\s*cap\s*≥\s*\$(\d+)([MB])", label)
        if not m: return _unparsable(label)
        need_count   = int(m.group(1))
        threshold_usd = int(m.group(2)) * _USD_MAGNITUDE[m.group(3)]
        apps = (M.get("native_apps") or {}).get("apps", [])
        n = sum(1 for a in apps if a["mcap_usd"] >= threshold_usd)
        met = n >= need_count
        pct = min(100, (n / need_count) * 100) if need_count else 0
        return {"met": met, "percent": pct, "gap": max(0, need_count - n),
                "kind": "count_above_mcap", "observed": n,
                "reason": f"{n} native app(s) currently ≥ ${threshold_usd/1e6:.0f}M"}

    # KPI 1 — Block Gas ─ "X TGas in a month (avg Y MGas/sec)"
    if kpi_id == 1:
        m = re.search(r"([\d.]+)\s*TGas\s*in\s*a\s*month", label)
        if not m: return _unparsable(label)
        threshold = float(m.group(1))
        gas = M.get("gas") or {}
        observed = gas.get("month_to_evaluate")
        if observed is None: return _no_data()
        met = observed >= threshold
        pct = min(100, (observed / threshold) * 100) if threshold else 0
        latest = (gas.get("latest_completed") or {}).get("month", "n/a")
        return {"met": met, "percent": pct, "gap": max(0, threshold - observed),
                "kind": "tgas_per_month", "observed": observed,
                "reason": f"{observed} TGas in {latest[:7]} vs {threshold} target"}

    # KPI 2 — ETH bridged ─ "≥ N,NNN ETH bridged to MegaETH"
    if kpi_id == 2:
        m = re.search(r"≥\s*([\d,]+)\s*ETH\s*bridged", label)
        if not m: return _unparsable(label)
        threshold = _parse_int(m.group(1))
        bridge = M.get("bridged") or {}
        observed = bridge.get("total_inflow_eth")
        if observed is None: return _no_data()
        met = observed >= threshold
        pct = min(100, (observed / threshold) * 100) if threshold else 0
        return {"met": met, "percent": pct, "gap": max(0, threshold - observed),
                "kind": "cumulative_eth_in", "observed": observed,
                "reason": f"{observed:,.2f} ETH bridged in"}

    # KPI 3 — USDM ─ "USDM circulating supply ≥ $XM" or "≥ $X.YB"
    if kpi_id == 3:
        m = re.search(r"≥\s*\$([\d.]+)\s*([MB])", label)
        if not m: return _unparsable(label)
        threshold = float(m.group(1)) * _USD_MAGNITUDE[m.group(2)]
        usdm = M.get("usdm")
        if usdm is None: return _no_data()
        # Use circulating-ex-treasury per the spec's literal wording. The
        # Foundation seeded ~$500M into Aave from address(0); excluding that
        # gives the honest organic supply.
        # Use STRICT reading (matches likely Foundation attestation): exclude
        # only protocol-governance Safes. Aave-deposited USDm counts as
        # circulating because it's in a permissionless DeFi market.
        observed = usdm["strict"]["circulating"]
        econ     = usdm["economic"]["circulating"]
        met = observed >= threshold
        pct = min(100, (observed / threshold) * 100) if threshold else 0
        return {"met": met, "percent": pct, "gap": max(0, threshold - observed),
                "kind": "usdm_strict_ex_governance_safes", "observed": observed,
                "reason": f"Strict (literal spec): ${observed:,.0f} circulating "
                          f"(total ${usdm['total']:,.0f}, excluded ${usdm['strict']['excluded']:,.0f} "
                          f"in governance Safes). "
                          f"Economic reading (excluding Ethena $500M Aave bootstrap): ${econ:,.0f}. "
                          f"Foundation will likely attest using the strict reading."}

    # KPI 4 — Ethereum Uptime ─ "Ethereum uptime > N years"
    if kpi_id == 4:
        m = re.search(r">\s*(\d+)\s*years", label)
        if not m: return _unparsable(label)
        threshold = int(m.group(1))
        observed = M.get("eth_uptime_years")
        if observed is None: return _no_data()
        met = observed > threshold
        pct = min(100, (observed / threshold) * 100) if threshold else 0
        return {"met": met, "percent": pct, "gap": max(0, threshold - observed),
                "kind": "years_since_eth_genesis", "observed": observed,
                "reason": f"{observed:.2f} years since Ethereum genesis"}

    # KPI 5 — Hardforks ─ "<Name> hardfork completed"
    if kpi_id == 5:
        m = re.search(r"^(\w+)\s+hardfork\s+completed", label, re.IGNORECASE)
        if not m: return _unparsable(label)
        name = m.group(1).lower()
        shipped_map = {"glamsterdam": GLAMSTERDAM_SHIPPED, "hegota": HEGOTA_SHIPPED}
        met = shipped_map.get(name, False)
        return {"met": met, "percent": 100 if met else 0, "gap": None,
                "kind": "hardfork_shipped", "observed": LATEST_HARDFORK,
                "reason": f"latest shipped: {LATEST_HARDFORK}"}

    # KPI 6 — Data Availability ─ "Implement DA bridge with DA Cert Verifier (EigenDA V2)"
    if kpi_id == 6:
        risks = M.get("l2beat") or {}
        da = (risks.get("dataAvailability") or "").lower()
        # Requires literal EigenDA mention. "External" alone is not enough.
        met = "eigenda" in da
        return {"met": met, "percent": 100 if met else 0, "gap": None,
                "kind": "l2beat_da_label", "observed": risks.get("dataAvailability"),
                "reason": f"L2Beat label: {risks.get('dataAvailability') or 'n/a'}"}

    # KPI 7 — State Validation ─ permissioned/permissionless fraud proofs
    if kpi_id == 7:
        # L2Beat compact label "Fraud proofs (1R, ZK)" can't tell us actor count
        # for tranche 0 (need ≥5 actors), nor whether the system is fully open
        # for tranche 1. Mark unparsable-from-source (manual).
        risks = M.get("l2beat") or {}
        return {"met": False, "percent": 0, "gap": None,
                "kind": "manual_l2beat_insufficient",
                "observed": risks.get("stateValidation"),
                "reason": "L2Beat label does not expose actor count or "
                          "permissionless toggle — manual verification required"}

    # KPI 8 — Proposer Failure ─ rotate vs permissionless
    if kpi_id == 8:
        risks = M.get("l2beat") or {}
        pf = (risks.get("proposerFailure") or "").lower()
        # Tranche 0: "Security council can rotate proposer". Tranche 1: "Anyone can propose"
        if "anyone" in label.lower() or "permissionless" in label.lower():
            met = "self propose" in pf or "permissionless" in pf
        else:  # rotate
            met = "rotate" in pf
        return {"met": met, "percent": 100 if met else 0, "gap": None,
                "kind": "l2beat_proposer_label", "observed": risks.get("proposerFailure"),
                "reason": f"L2Beat label: {risks.get('proposerFailure') or 'n/a'}"}

    # KPI 9 — Exit Window ─ "≥Nd time delay before protocol upgrade"
    if kpi_id == 9:
        m = re.search(r"≥\s*(\d+)\s*d", label)
        if not m: return _unparsable(label)
        threshold_days = int(m.group(1))
        risks = M.get("l2beat") or {}
        ew = (risks.get("exitWindow") or "").lower()
        # L2Beat labels: "None", "<7d", e.g. "7d", "30d"
        # We can only confirm met if a number ≥ threshold appears in the label.
        m2 = re.search(r"(\d+)\s*d", ew)
        if "none" in ew or "<" in ew or not m2:
            met = False
            observed = risks.get("exitWindow") or "None"
        else:
            observed_days = int(m2.group(1))
            met = observed_days >= threshold_days
            observed = f"{observed_days}d"
        return {"met": met, "percent": 100 if met else 0, "gap": None,
                "kind": "l2beat_exit_window", "observed": observed,
                "reason": f"L2Beat exit window: {risks.get('exitWindow') or 'n/a'}"}

    # KPI 10 — Block Times ─ "X% of miniblocks at or below 12ms blocktime for Y"
    # No public Dune query exposes miniblock latency aggregates. Manual.
    if kpi_id == 10:
        return {"met": False, "percent": 0, "gap": None,
                "kind": "manual_no_miniblock_source", "observed": None,
                "reason": "Miniblock latency not exposed via Dune; uptime.megaeth.com "
                          "is the spec source but not machine-readable yet"}

    # KPI 11 — Finality ─ "X% of batches finalized within Y minutes"
    # MegaETH finality dashboard's visualizations are private to the team.
    if kpi_id == 11:
        return {"met": False, "percent": 0, "gap": None,
                "kind": "manual_finality_dashboard_private", "observed": None,
                "reason": "Foundation's finality Dune visualizations are private — "
                          "no public API access"}

    # KPI 12 — Uptime ─ "No >5-minute block production pause … in past N month(s)"
    if kpi_id == 12:
        m = re.search(r"past\s+(\d+)\s+months?", label)
        if not m: return _unparsable(label)
        n_months = int(m.group(1))
        gap_data = M.get("block_gaps")
        if not gap_data or not gap_data.get("series"): return _no_data()

        # Window: TGE → TGE + N×30d. The gap query's monthly buckets are >5sec
        # gaps. Zero >5sec gaps in a month → zero >5min pauses by transitivity.
        ws = tge_window_status(window_days=n_months * 30)
        tge_month = time.strftime("%Y-%m-01", time.gmtime(TGE_TIMESTAMP))
        end_month = time.strftime("%Y-%m-01", time.gmtime(ws["window_end_ts"]))
        touched = [r for r in gap_data["series"] if tge_month <= r["month"] <= end_month]
        any_gap = any((r.get("gaps") or 0) > 0 for r in touched)

        if not ws["is_closed"]:
            pct = min(99.0, ws["percent_elapsed"]) if not any_gap else 0
            return {"met": False, "percent": pct, "gap": None,
                    "kind": "tge_window_in_progress", "observed": "0 gaps so far" if not any_gap else "gaps detected",
                    "reason": f"TGE-anchored window {ws['elapsed_days']:.1f}d / {n_months*30}d "
                              f"({ws['percent_elapsed']:.1f}% elapsed). "
                              f"{'On-track — no >5s gaps yet.' if not any_gap else 'Gaps detected — cannot meet.'}"}

        met = (not any_gap) and len(touched) > 0
        return {"met": met, "percent": 100 if met else 0, "gap": None,
                "kind": "tge_window_closed", "observed": "0 gaps" if met else "gaps present",
                "reason": f"Window closed; {'no >5sec gaps in TGE window' if met else 'gaps detected'}"}

    return _unparsable(label)


def _unparsable(label):
    return {"met": False, "percent": 0, "gap": None, "kind": "unparsable",
            "observed": None, "reason": f"Label format unrecognized: {label[:80]}"}


def _no_data():
    return {"met": False, "percent": 0, "gap": None, "kind": "no_live_data",
            "observed": None, "reason": "Live data source unavailable this run"}


# ═══ Build ═══════════════════════════════════════════════════════════════════

def load_onchain():
    if not os.path.exists(ONCHAIN_KPIS_PATH):
        raise RuntimeError(f"{ONCHAIN_KPIS_PATH} not found — run sync_onchain_kpis.py first")
    with open(ONCHAIN_KPIS_PATH) as f:
        return json.load(f)


# Maps the on-chain `category` field to a stable display order.
CATEGORY_ORDER = ["Ecosystem Growth", "MegaETH Performance",
                  "MegaETH Decentralization", "Ethereum Decentralization"]


def build():
    print("[kpis] loading on-chain canonical data…")
    chain = load_onchain()
    print(f"  {len(chain['kpis'])} KPIs · {len(chain['tranches'])} tranches "
          f"· {chain['totals']['total_reward_mega_allocated']/1e6:.0f}M allocated")

    print("[kpis] fetching live measurements…")
    M = {}
    M["native_apps"]      = fetch_native_apps();           print(f"  native_apps:    {len((M['native_apps'] or {}).get('apps', []))} natives, {len((M['native_apps'] or {}).get('non_native_skipped', []))} skipped")
    M["usdm"]             = fetch_usdm_supply();           print(f"  usdm_supply:    {M['usdm']!r}")
    M["bridged"]          = fetch_eth_bridged_dune();      print(f"  bridged:        {(M['bridged'] or {}).get('total_inflow_eth')!r}")
    M["gas"]              = fetch_gas_dune();              print(f"  gas:            {(M['gas'] or {}).get('month_to_evaluate')!r} TGas (latest complete month)")
    M["block_times"]      = fetch_block_times_dune();      print(f"  block_times:    {(M['block_times'] or {}).get('current_block_ms')!r} ms")
    M["block_gaps"]       = fetch_block_gaps_dune();       print(f"  block_gaps:     {len((M['block_gaps'] or {}).get('series', []))} months of data")
    M["eth_block_times"]  = fetch_eth_block_times_dune();  print(f"  eth_p95:        {(M['eth_block_times'] or {}).get('p95_seconds')}s")
    M["l2beat"]           = fetch_l2beat_risks();          print(f"  l2beat:         {M['l2beat']!r}")
    M["eth_uptime_years"] = fetch_eth_uptime_years();      print(f"  eth_uptime:     {M['eth_uptime_years']:.3f} years")

    # Index KPIs and group their tranches
    kpi_by_id = {k["id"]: k for k in chain["kpis"]}
    tranches_by_kpi = {}
    for t in chain["tranches"]:
        tranches_by_kpi.setdefault(t["kpi_id"], []).append(t)
    for ts in tranches_by_kpi.values():
        ts.sort(key=lambda t: t["id"])

    # Evaluate every tranche
    out_kpis = []
    completed = 0
    locked    = 0
    unlocked  = 0
    for kid in sorted(tranches_by_kpi):
        kpi = kpi_by_id.get(kid) or {"id": kid, "name": f"KPI #{kid}", "category": "Unknown"}
        tranche_results = []
        n_done = 0
        for t in tranches_by_kpi[kid]:
            ev = evaluate_tranche(kid, t["label"], M)
            attested = t.get("onchain_status") is not None
            # On-chain attestation overrides our forecast — Foundation has spoken
            if attested:
                # Status enum is uint8; we don't know the exact enum mapping yet,
                # so treat any non-null status as "the chain has spoken" and
                # surface the status code for inspection.
                ev["onchain_status_code"] = t["onchain_status"]
                # Conservative: only treat as MET if an achievement value was also recorded
                if t.get("achievement_value") is not None:
                    ev["met"] = True
                    ev["percent"] = 100
                    ev["reason"] = f"Foundation attested on-chain (value={t['achievement_value']})"
            if ev["met"]:
                n_done += 1
                completed += 1
                unlocked += t["reward_mega"]
            else:
                locked += t["reward_mega"]
            tranche_results.append({
                "id":            t["id"],
                "label":         t["label"],
                "reward_mega":   t["reward_mega"],
                "percent":       round(ev["percent"], 2),
                "gap":           ev["gap"],
                "status":        "completed" if ev["met"] else "executing",
                "kind":          ev["kind"],
                "observed":      ev["observed"],
                "reason":        ev["reason"],
                "onchain_attested": attested,
                "onchain_status_code": t.get("onchain_status"),
                "achievement_value":   t.get("achievement_value"),
            })
        out_kpis.append({
            "id":            kid,
            "name":          kpi.get("name"),
            "description":   kpi.get("description"),
            "category":      kpi.get("category"),
            "source_url":    kpi.get("source_url"),
            "completed_tranches": n_done,
            "total_tranches":     len(tranche_results),
            "tranches":      tranche_results,
        })

    # Categories in stable order (CATEGORY_ORDER first, then anything else)
    cat_idx = {c: i for i, c in enumerate(CATEGORY_ORDER)}
    out_kpis.sort(key=lambda k: (cat_idx.get(k["category"], 99), k["id"]))

    return {
        "built_at":           int(time.time()),
        "total_tranches":     len(chain["tranches"]),
        "completed_tranches": completed,
        "locked_mega":        locked,
        "unlocked_mega":      unlocked,
        "kpis":               out_kpis,
        "onchain":            chain["totals"],
        "live_measurements": {
            "native_apps":     M["native_apps"],
            "usdm":            M["usdm"],
            "bridged":         M["bridged"],
            "gas":             M["gas"],
            "block_times":     M["block_times"],
            "block_gaps":      M["block_gaps"],
            "eth_block_times": M["eth_block_times"],
            "l2beat":          M["l2beat"],
            "eth_uptime_years": M["eth_uptime_years"],
        },
        "tge": {
            "timestamp":  TGE_TIMESTAMP,
            "iso":        TGE_ISO,
            "window_30d": tge_window_status(30),
        },
        "dune_enabled":  bool(DUNE_API_KEY),
    }


def run():
    data = build()
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "data")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "kpis.json")
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"\n[kpis] wrote {path} ({os.path.getsize(path):,} bytes)")
    print(f"  {data['completed_tranches']}/{data['total_tranches']} tranches met "
          f"·  unlocked: {data['unlocked_mega']/1e6:.0f}M MEGA "
          f"·  locked: {data['locked_mega']/1e6:.0f}M MEGA")
    return data


if __name__ == "__main__":
    run()
