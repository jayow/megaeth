"""Flask server for the MEGA token holder dashboard."""

import json


from flask import Flask, jsonify, render_template, request

from aggregate import per_address, summary
from db import connect, get_state, transfer_count


def _all_claim_proxies():
    proxies = set(json.loads(get_state("distributor_proxies") or "[]"))
    primary = (get_state("claim_contract") or "").lower()
    if primary:
        proxies.add(primary)
    return [p.lower() for p in proxies if p]

app = Flask(__name__)
_CACHE = {"rows": None}


def get_rows(force=False):
    if force or _CACHE["rows"] is None:
        _CACHE["rows"] = per_address()
    return _CACHE["rows"]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/debug")
def api_debug():
    """Expose runtime errors so we can diagnose Vercel deploy issues."""
    import os, traceback
    out = {
        "python_version": os.environ.get("VERCEL_PYTHON_VERSION", "?"),
        "has_database_url": bool(os.environ.get("DATABASE_URL")),
        "database_url_prefix": (os.environ.get("DATABASE_URL") or "")[:25],
        "checks": {},
    }
    try:
        import psycopg2
        out["checks"]["psycopg2_import"] = f"ok (v{psycopg2.__version__})"
    except Exception as e:
        out["checks"]["psycopg2_import"] = f"FAIL: {e}"
    try:
        from db import IS_POSTGRES, connect
        out["checks"]["db_module_imports"] = "ok"
        out["checks"]["IS_POSTGRES"] = IS_POSTGRES
    except Exception as e:
        out["checks"]["db_module_imports"] = f"FAIL: {e}\n{traceback.format_exc()[:500]}"
        return jsonify(out), 500
    try:
        conn = connect()
        n = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
        conn.close()
        out["checks"]["db_query"] = f"ok ({n} transfers)"
    except Exception as e:
        out["checks"]["db_query"] = f"FAIL: {type(e).__name__}: {e}\n{traceback.format_exc()[:800]}"
    return jsonify(out)


@app.route("/api/summary")
def api_summary():
    rows = get_rows()
    s = summary(rows)
    s["sync"] = {
        "transfers_in_db": transfer_count(),
        "claim_contract": get_state("claim_contract") or None,
        "dex_addresses": json.loads(get_state("dex_addresses") or "[]"),
        "page_idx": int(get_state("page_idx", "0")),
        "next_page_params": get_state("next_page_params") or None,
    }
    # Convert wei totals to MEGA strings (avoid JS bigint loss)
    s["totals_mega"] = {k: f"{v / 10**18:.4f}" for k, v in s["totals"].items()}

    sd_raw = get_state("supply_distribution")
    s["supply"] = json.loads(sd_raw) if sd_raw else None
    return jsonify(s)


@app.route("/api/holders")
def api_holders():
    """Paginated, sortable, filterable per-address list.
    Query params: status, sort (claimed|bought|sold|current|address),
    order (desc|asc), q (substring match on address), limit, offset.
    """
    rows = get_rows()
    status = request.args.get("status")
    q = (request.args.get("q") or "").lower().strip()
    sort = request.args.get("sort", "current")
    order = request.args.get("order", "desc")
    try:
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        offset = max(0, int(request.args.get("offset", 0)))
    except ValueError:
        limit, offset = 50, 0

    filt = rows
    if status:
        filt = [r for r in filt if r["status"] == status]
    if q:
        filt = [r for r in filt if q in r["address"]]

    sort_keys = {"claimed", "bought", "sold", "current", "address"}
    if sort not in sort_keys:
        sort = "current"
    reverse = order != "asc"
    if sort == "address":
        filt = sorted(filt, key=lambda r: r["address"], reverse=reverse)
    else:
        filt = sorted(filt, key=lambda r: r[sort], reverse=reverse)

    total = len(filt)
    page = filt[offset:offset + limit]

    def to_mega(v):
        return f"{v / 10**18:.4f}"

    out = [{
        "address": r["address"],
        "claimed": to_mega(r["claimed"]),
        "bought": to_mega(r["bought"]),
        "sold": to_mega(r["sold"]),
        "current": to_mega(r["current"]),
        "received_other": to_mega(r["received_other"]),
        "sent_other": to_mega(r["sent_other"]),
        "status": r["status"],
    } for r in page]

    return jsonify({"total": total, "limit": limit, "offset": offset, "rows": out})


@app.route("/api/sample_txs")
def api_sample_txs():
    """Return a recent example tx hash for each category so users can
    spot-check by clicking through to Blockscout."""
    from db import connect, get_state
    conn = connect()
    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]
    dex = {d.lower() for d in json.loads(get_state("dex_addresses") or "[]")}
    distrib_sources = _all_claim_proxies() + batchers

    out = {}

    # Most-recent claim from any distributor
    if distrib_sources:
        ph = ",".join("?" * len(distrib_sources))
        row = conn.execute(
            f"SELECT tx_hash FROM transfers WHERE from_addr IN ({ph}) "
            f"ORDER BY block_number DESC, log_index DESC LIMIT 1",
            tuple(distrib_sources),
        ).fetchone()
        if row: out["sample_claim_tx"] = row["tx_hash"]

    # Most-recent sell (any -> dex)
    if dex:
        ph = ",".join("?" * len(dex))
        row = conn.execute(
            f"SELECT tx_hash FROM transfers WHERE to_addr IN ({ph}) "
            f"ORDER BY block_number DESC, log_index DESC LIMIT 1",
            tuple(dex),
        ).fetchone()
        if row: out["sample_sell_tx"] = row["tx_hash"]

        # Most-recent buy (dex -> any)
        row = conn.execute(
            f"SELECT tx_hash FROM transfers WHERE from_addr IN ({ph}) "
            f"ORDER BY block_number DESC, log_index DESC LIMIT 1",
            tuple(dex),
        ).fetchone()
        if row: out["sample_buy_tx"] = row["tx_hash"]

    return jsonify(out)


@app.route("/api/timeline")
def api_timeline():
    """Hourly bucketization of MEGA flows since TGE.

    Returns three series: claims (from any distributor), sells (to any
    DEX), buys (from any DEX). Used to render a time-axis chart.
    """
    from db import connect, get_state
    conn = connect()
    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]
    dex = {d.lower() for d in json.loads(get_state("dex_addresses") or "[]")}
    distrib_sources = _all_claim_proxies() + batchers

    buckets = {}  # ts_hour -> {"claims": int, "sells": int, "buys": int, "n_claim": int, "n_sell": int, "n_buy": int}
    for r in conn.execute(
        "SELECT timestamp, from_addr, to_addr, value FROM transfers WHERE timestamp IS NOT NULL"
    ):
        ts = r["timestamp"]  # ISO8601 e.g. 2026-04-30T13:31:48.000000Z
        if not ts:
            continue
        hour = ts[:13]  # YYYY-MM-DDTHH
        b = buckets.setdefault(hour, {"claims": 0, "sells": 0, "buys": 0, "n_claim": 0, "n_sell": 0, "n_buy": 0})
        v = int(r["value"])
        if r["from_addr"] in distrib_sources:
            b["claims"] += v; b["n_claim"] += 1
        elif r["from_addr"] in dex:
            b["buys"]   += v; b["n_buy"]   += 1
        elif r["to_addr"] in dex:
            b["sells"]  += v; b["n_sell"]  += 1

    series = []
    for hour in sorted(buckets):
        b = buckets[hour]
        series.append({
            "hour": hour,
            "claims_mega": round(b["claims"] / 10**18, 2),
            "sells_mega":  round(b["sells"]  / 10**18, 2),
            "buys_mega":   round(b["buys"]   / 10**18, 2),
            "n_claim":     b["n_claim"],
            "n_sell":      b["n_sell"],
            "n_buy":       b["n_buy"],
        })
    return jsonify({"series": series})


@app.route("/api/wallet/<addr>")
def api_wallet(addr):
    """Full per-wallet story for an analyst lookup."""
    from db import connect, get_state
    addr = addr.lower().strip()
    if not addr.startswith("0x") or len(addr) != 42:
        return jsonify({"error": "bad address"}), 400
    conn = connect()

    # Aggregate from rows cache
    rows_map = {r["address"]: r for r in get_rows()}
    r = rows_map.get(addr)

    # True balance
    tb = conn.execute("SELECT balance FROM true_balance WHERE address = ?", (addr,)).fetchone()
    true_bal = int(tb["balance"]) if tb else None

    # Eligibility flags
    in_fluffle = bool(conn.execute("SELECT 1 FROM fluffle_owner WHERE address = ?", (addr,)).fetchone())
    eth_entities = [r["entity_id"] for r in conn.execute(
        "SELECT entity_id FROM eth_entity WHERE address = ?", (addr,))]
    has_allocation = False
    if eth_entities:
        ph = ",".join("?" * len(eth_entities))
        has_allocation = bool(conn.execute(
            f"SELECT 1 FROM eth_allocation WHERE entity_id IN ({ph})", tuple(eth_entities)
        ).fetchone())

    # Did this wallet ever appear as receiver in Distributor.Claimed?
    claimed_events = []
    if True:  # megaeth_claimed always exists (db.SCHEMA_STATEMENTS)
        for row in conn.execute(
            "SELECT block, distribution_uuid, entity_uuid, CAST(amount AS REAL)/1e18 AS m FROM megaeth_claimed WHERE receiver = ? ORDER BY block",
            (addr,),
        ):
            claimed_events.append(dict(row))

    # Recent transfers (last 10)
    recent = []
    for row in conn.execute(
        "SELECT tx_hash, block_number, timestamp, from_addr, to_addr, value FROM transfers "
        "WHERE from_addr = ? OR to_addr = ? ORDER BY block_number DESC, log_index DESC LIMIT 10",
        (addr, addr),
    ):
        recent.append({
            "tx": row["tx_hash"],
            "block": row["block_number"],
            "ts": row["timestamp"],
            "direction": "in" if row["to_addr"] == addr else "out",
            "counterparty": row["from_addr"] if row["to_addr"] == addr else row["to_addr"],
            "amount_mega": int(row["value"]) / 10**18,
        })

    # Counts
    n_in  = conn.execute("SELECT COUNT(*) FROM transfers WHERE to_addr = ?", (addr,)).fetchone()[0]
    n_out = conn.execute("SELECT COUNT(*) FROM transfers WHERE from_addr = ?", (addr,)).fetchone()[0]

    return jsonify({
        "address": addr,
        "exists_in_data": (r is not None) or (true_bal is not None) or in_fluffle or has_allocation,
        "current_mega":   (true_bal / 10**18) if true_bal is not None else None,
        "claimed_mega":   (r["claimed"] / 10**18) if r else 0,
        "bought_mega":    (r["bought"]  / 10**18) if r else 0,
        "sold_mega":      (r["sold"]    / 10**18) if r else 0,
        "n_transfers_in":  n_in,
        "n_transfers_out": n_out,
        "in_fluffle":      in_fluffle,
        "in_echo_winners": has_allocation,
        "ethereum_entities": eth_entities,
        "distributor_claims": claimed_events,
        "recent_transfers": recent,
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    get_rows(force=True)
    return jsonify({"ok": True, "addresses": len(_CACHE["rows"])})


@app.route("/api/cron/sync", methods=["GET", "POST"])
def api_cron_sync():
    """Hourly cron entry-point.

    Vercel Cron hits this with GET. Verifies the optional CRON_SECRET
    bearer token, then runs the incremental sync pipeline. Returns a
    JSON status. Designed to fit within Vercel's 60s function limit by
    only doing the lightweight steps (sync + classify + cache refresh).
    Heavy steps (balances, Claimed events) should run via a separate
    cron with longer time budget, or via GitHub Actions.
    """
    import os, time
    secret = os.environ.get("CRON_SECRET")
    if secret:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {secret}":
            return jsonify({"error": "unauthorized"}), 401

    t0 = time.time()
    summary = {"steps": []}

    def step(name, fn):
        st0 = time.time()
        try:
            result = fn()
            summary["steps"].append({"name": name, "ok": True, "elapsed": round(time.time()-st0, 2), "result": result})
        except Exception as e:
            summary["steps"].append({"name": name, "ok": False, "elapsed": round(time.time()-st0, 2), "error": str(e)})

    # Lightweight pipeline
    def run_sync():
        import sync
        sync.run()
        from db import transfer_count
        return {"transfers_in_db": transfer_count()}

    def run_classify():
        import classify
        classify.run()
        return "classified"

    def refresh_cache():
        get_rows(force=True)
        return {"addresses": len(_CACHE["rows"])}

    step("sync",     run_sync)
    step("classify", run_classify)
    step("refresh",  refresh_cache)

    summary["total_elapsed"] = round(time.time() - t0, 2)
    summary["ok"] = all(s["ok"] for s in summary["steps"])
    return jsonify(summary)


@app.route("/api/behavior")
def api_behavior():
    """Unified view: every address known to be eligible for MEGA, bucketed
    by what they did with it (sold / held / bought_more / not_claimed).

    Eligibility = union of:
      * Fluffle NFT holders on Ethereum
      * Echo MegaSale auction winners on Ethereum
      * Any Ethereum entity (from EntityInitialized) whose entityUUID
        appears in a MegaETH `Claimed` event — i.e. they claimed to a
        wallet that may differ from their Ethereum bidding wallet
      * Any address that has already received from a distributor on MegaETH
    """
    from db import connect, get_state
    conn = connect()

    fluffle = {r["address"] for r in conn.execute("SELECT address FROM fluffle_owner")}

    echo_winners = set()
    if conn.execute("SELECT COUNT(*) FROM eth_allocation").fetchone()[0] > 0:
        echo_winners = {r["address"] for r in conn.execute(
            "SELECT DISTINCT e.address FROM eth_allocation a JOIN eth_entity e ON e.entity_id = a.entity_id"
        )}

    # ALSO: any Ethereum address whose entityUUID was used in a real claim
    # — this captures users who claimed to a different MegaETH wallet.
    # megaeth_claimed always exists (created by db.SCHEMA_STATEMENTS).
    has_claims_table = True
    eth_addrs_who_claimed = set()
    receiver_wallets_via_claims = set()
    if has_claims_table:
        eth_addrs_who_claimed = {r["address"] for r in conn.execute(
            """SELECT DISTINCT e.address FROM megaeth_claimed c
               JOIN eth_entity e ON e.entity_id = c.entity_uuid"""
        )}
        receiver_wallets_via_claims = {r["receiver"] for r in conn.execute(
            "SELECT DISTINCT receiver FROM megaeth_claimed"
        )}

    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]
    sources = _all_claim_proxies() + batchers
    received_set = set()
    if sources:
        ph = ",".join("?" * len(sources))
        for r in conn.execute(
            f"SELECT DISTINCT to_addr FROM transfers WHERE from_addr IN ({ph})",
            tuple(sources),
        ):
            received_set.add(r["to_addr"])

    eligible = fluffle | echo_winners | received_set | receiver_wallets_via_claims

    # Pull per-address aggregate data
    rows = {r["address"]: r for r in get_rows()}

    buckets = {
        "sold":         [],   # claimed > 0, sold something on DEX (current < ~claimed)
        "held":         [],   # claimed > 0, didn't sell, didn't buy more (in wallet)
        "staked":       [],   # claimed > 0, sent any amount to MegaStaking
        "bought_more":  [],   # claimed > 0, bought more on DEX
        "not_claimed":  [],   # eligible but no MEGA received from any distributor
    }

    for addr in eligible:
        r = rows.get(addr)
        if not r or r["claimed"] == 0:
            buckets["not_claimed"].append(addr)
            continue
        # Staking takes priority — a staker is effectively a holder, not a seller
        if r.get("staked", 0) > 0:
            buckets["staked"].append(addr)
            continue
        if r["bought"] > r["sold"] and r["bought"] > 0:
            buckets["bought_more"].append(addr)
        elif r["sold"] > 0:
            buckets["sold"].append(addr)
        else:
            buckets["held"].append(addr)

    # Build summary + per-address detail (top N per bucket by amount)
    def to_mega(v):
        return f"{v / 10**18:.4f}"

    def detail(addr):
        r = rows.get(addr)
        # "% sold" = sold / received  (how much of what they got did they dump)
        # "% kept" = current / received
        sold_pct = None
        kept_pct = None
        if r and (r["claimed"] + r["bought"]) > 0:
            received = r["claimed"] + r["bought"]
            sold_pct = round(min(100, r["sold"] / received * 100), 1)
            kept_pct = round(min(100, r["current"] / received * 100), 1) if received else None
        # Why pending? (only relevant for not_claimed bucket)
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
        in_echo_private = addr in receiver_wallets_via_claims
        rounds = []
        if addr in fluffle:       rounds.append("fluffle")
        if addr in echo_winners:  rounds.append("conviction")
        if in_echo_private:       rounds.append("echo_private")
        if not rounds and r and r["claimed"] > 0: rounds.append("other")
        if not r:
            return {
                "address": addr, "claimed": "0", "bought": "0", "sold": "0",
                "staked": "0", "unstaked": "0",
                "current": "0",
                "sold_pct": None, "kept_pct": None,
                "in_fluffle": addr in fluffle,
                "in_echo_winners": addr in echo_winners,
                "in_echo_private": in_echo_private,
                "rounds": rounds,
                "n_rounds": len(rounds),
                "pending_reason": pending_reason,
            }
        return {
            "address": addr,
            "claimed": to_mega(r["claimed"]),
            "bought":  to_mega(r["bought"]),
            "sold":    to_mega(r["sold"]),
            "staked":  to_mega(r.get("staked", 0)),
            "unstaked":to_mega(r.get("unstaked", 0)),
            "current": to_mega(r["current"]),
            "sold_pct": sold_pct,
            "kept_pct": kept_pct,
            "in_fluffle": addr in fluffle,
            "in_echo_winners": addr in echo_winners,
            "in_echo_private": in_echo_private,
            "rounds": rounds,
            "n_rounds": len(rounds),
            "pending_reason": pending_reason,
        }

    sortkey = {
        "sold":        lambda a: -(rows[a]["sold"] if a in rows else 0),
        "held":        lambda a: -(rows[a]["current"] if a in rows else 0),
        "staked":      lambda a: -(rows[a].get("staked", 0) if a in rows else 0),
        "bought_more": lambda a: -(rows[a]["bought"] if a in rows else 0),
        "not_claimed": lambda a: a,
    }

    # OVERLAPPING cohorts — each = full membership of that round.
    other_recv = received_set - fluffle - echo_winners - receiver_wallets_via_claims
    cohorts = {
        "fluffle":         fluffle,
        "conviction":      echo_winners,
        "echo_private":    receiver_wallets_via_claims,
        "other":           other_recv,
    }
    cohort_counts = {k: len(v) for k, v in cohorts.items()}

    # Per-allocation accounting: each (wallet, round) is its OWN row.
    # A wallet in 2 rounds = 2 allocation rows. This means a wallet can
    # appear as "held" for their Fluffle 25K AND "sold" for their
    # Conviction 26K (proportional split based on receipt amount).
    # Buckets per round:
    #   held / sold / bought_more  — proportional behavior on this allocation
    #   pending      — Fluffle auto-delivery not yet sent (105 cases)
    #   locked_2027  — Conviction wallet that chose 1-yr lockup (849 cases)

    delivery_recipients = received_set  # received from any distributor on MegaETH
    bucket_addr = {b: set(addrs) for b, addrs in buckets.items()}

    def per_round_buckets(round_name, members):
        bk = {"held": 0, "sold": 0, "staked": 0, "bought_more": 0, "pending": 0, "locked_2027": 0}
        mega = {"sold": 0, "current": 0, "bought": 0, "claimed": 0, "staked": 0}
        for addr in members:
            r = rows.get(addr)
            received = (r["claimed"] + r["bought"]) if r else 0

            if (not r) or r["claimed"] == 0:
                if round_name == "conviction":
                    bk["locked_2027"] += 1
                elif round_name == "fluffle":
                    bk["pending"] += 1
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
            curr_share   = r["current"]* share
            claim_share  = r["claimed"]* share
            staked_share = r.get("staked", 0) * share

            mega["sold"]    += sold_share
            mega["current"] += curr_share
            mega["bought"]  += bought_share
            mega["claimed"] += claim_share
            mega["staked"]  += staked_share

            # Stakers take priority — sending to staking is "still holding".
            if r.get("staked", 0) > 0:
                bk["staked"] += 1
            elif bought_share > sold_share and bought_share > 0:
                bk["bought_more"] += 1
            elif sold_share > 0:
                bk["sold"] += 1
            else:
                bk["held"] += 1

        # Stakers count as holders for the rate calculation
        delivered = bk["held"] + bk["sold"] + bk["bought_more"] + bk["staked"]
        effective_held = bk["held"] + bk["staked"]
        return {
            "members": len(members),
            "buckets": bk,
            "mega": {k: f"{v/10**18:.4f}" for k, v in mega.items()},
            "hold_rate": round(effective_held / delivered * 100, 1) if delivered else 0,
        }

    rounds_data = {
        "fluffle":      per_round_buckets("fluffle",      fluffle),
        "conviction":   per_round_buckets("conviction",   echo_winners),
        "echo_private": per_round_buckets("echo_private", receiver_wallets_via_claims),
        "other":        per_round_buckets("other",        other_recv),
    }

    # Why-pending breakdown
    pending_set = set(buckets["not_claimed"])
    pending_fluffle = len(pending_set & (fluffle - receiver_wallets_via_claims))
    pending_echo    = len(pending_set & echo_winners)
    pending_other   = len(pending_set) - pending_fluffle - pending_echo
    if pending_other < 0: pending_other = 0

    out = {
        "totals": {
            "eligible":   len(eligible),
            "sold":       len(buckets["sold"]),
            "held":       len(buckets["held"]),
            "staked":     len(buckets["staked"]),
            "bought_more":len(buckets["bought_more"]),
            "not_claimed":len(buckets["not_claimed"]),
        },
        "pending_by_reason": {
            "echo_sale_not_live": pending_echo,
            "fluffle_not_delivered": pending_fluffle,
            "other": pending_other,
        },
        "sources": {
            "fluffle_holders":     len(fluffle),
            "echo_auction_winners":len(echo_winners),
            "received_from_distributor": len(received_set),
        },
        "cohorts": cohort_counts,
        "cohort_pct": {k: round(v / len(eligible) * 100, 2) if eligible else 0 for k, v in cohort_counts.items()},
        "rounds": rounds_data,
        "rows": {},
    }

    # Cohort × behavior cross-tab: % of each cohort that sold/held/bought/pending
    out["cohort_behavior"] = {}
    bucket_addr = {k: set(v) for k, v in buckets.items()}
    for cname, cset in cohorts.items():
        if not cset:
            out["cohort_behavior"][cname] = {b: 0 for b in bucket_addr}
            continue
        out["cohort_behavior"][cname] = {
            b: len(cset & bset) for b, bset in bucket_addr.items()
        }

    # MEGA totals per bucket
    out["totals_mega"] = {}
    for k, lst in buckets.items():
        s_claim = sum(rows[a]["claimed"] for a in lst if a in rows)
        s_buy   = sum(rows[a]["bought"]  for a in lst if a in rows)
        s_sell  = sum(rows[a]["sold"]    for a in lst if a in rows)
        s_curr  = sum(rows[a]["current"] for a in lst if a in rows)
        s_stak  = sum(rows[a].get("staked", 0) for a in lst if a in rows)
        out["totals_mega"][k] = {
            "claimed": to_mega(s_claim),
            "bought":  to_mega(s_buy),
            "sold":    to_mega(s_sell),
            "current": to_mega(s_curr),
            "staked":  to_mega(s_stak),
        }

    # Per-address rows (limited per bucket so payload stays small)
    limit = int(request.args.get("limit", 100))
    for k, lst in buckets.items():
        sorted_addrs = sorted(lst, key=sortkey[k])[:limit]
        out["rows"][k] = [detail(a) for a in sorted_addrs]

    return jsonify(out)


@app.route("/api/eligibility")
def api_eligibility():
    """Cross-reference Ethereum-mainnet eligibility lists with MegaETH claims."""
    from db import connect, get_state
    conn = connect()

    # Fluffle: NFT holders on Ethereum vs. recipients from MegaETH batcher
    fluffle_holders = {r["address"] for r in conn.execute("SELECT address FROM fluffle_owner")}
    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]

    fluffle_recipients = set()
    if batchers:
        ph = ",".join("?" * len(batchers))
        for r in conn.execute(
            f"SELECT DISTINCT to_addr FROM transfers WHERE from_addr IN ({ph})",
            tuple(batchers),
        ):
            fluffle_recipients.add(r["to_addr"])

    fluffle = {
        "eligible_total": len(fluffle_holders),
        "delivered": len(fluffle_holders & fluffle_recipients),
        "pending":   len(fluffle_holders - fluffle_recipients),
        "other_batcher_recipients": len(fluffle_recipients - fluffle_holders),
        "pending_sample": sorted(fluffle_holders - fluffle_recipients)[:25],
    }

    # Echo: entities with allocation that weren't refunded -> wallet addrs
    n_entities = conn.execute("SELECT COUNT(*) FROM eth_entity").fetchone()[0]
    n_alloc    = conn.execute("SELECT COUNT(*) FROM eth_allocation").fetchone()[0]
    n_ref      = conn.execute("SELECT COUNT(*) FROM eth_refunded").fetchone()[0]

    echo_eligible = []
    if n_alloc > 0 and n_entities > 0:
        # In a Dutch/clearing auction every bidder is refunded the overbid
        # USDT regardless of allocation. So eligibility = AllocationSet, not
        # AllocationSet \ Refunded.
        echo_eligible = [r["address"] for r in conn.execute(
            """SELECT DISTINCT e.address FROM eth_allocation a
               JOIN eth_entity e ON e.entity_id = a.entity_id"""
        )]

    # Conviction Round delivery is AUTOMATIC via batcher EOA, not via
    # the Echo Distributor proxy. So we union both (all proxies) as "received".
    batchers = [b.lower() for b in json.loads(get_state("batch_senders") or "[]")]
    delivery_sources = _all_claim_proxies() + batchers

    delivery_recipients = set()
    if delivery_sources:
        ph = ",".join("?" * len(delivery_sources))
        for r in conn.execute(
            f"SELECT DISTINCT to_addr FROM transfers WHERE from_addr IN ({ph})",
            tuple(delivery_sources),
        ):
            delivery_recipients.add(r["to_addr"])

    echo_set = set(echo_eligible)
    delivered = echo_set & delivery_recipients
    pending   = echo_set - delivery_recipients

    echo = {
        "entities_initialized": n_entities,
        "allocations": n_alloc,
        "refunded": n_ref,
        "eligible_addresses": len(echo_eligible),
        # Conviction Round delivery is auto, not claim. "Delivered" = received from any distributor (batcher or Distributor proxy)
        "delivered": len(delivered),
        "pending":   len(pending) if echo_eligible else None,
        # "pending" most likely = chose 1-year lockup → Apr 30 2027 delivery
        "pending_sample": sorted(pending)[:25] if echo_eligible else [],
        # back-compat
        "claimed": len(delivered),
        "claim_recipients_unmatched": 0,
    }

    return jsonify({"fluffle": fluffle, "echo": echo})


if __name__ == "__main__":
    # debug=True enables Werkzeug's auto-reloader: any save to .py / .html
    # files in this directory restarts the worker automatically.
    app.run(host="127.0.0.1", port=5050, debug=True, use_reloader=True,
            extra_files=["templates/index.html"])
