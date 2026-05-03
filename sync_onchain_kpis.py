"""Sync the canonical KPI program from MegaStaking on MegaETH mainnet.

Writes static/data/onchain_kpis.json with:
  - Every KPI registered via createKpi (name, desc, category, source URL)
  - Every tranche registered via createTranche (kpi_id, reward, label)
  - Every status update (TrancheStatusUpdated)
  - Every achievement value recorded (TrancheAchievementDataSet)

This is the authoritative source for what counts as "met." Never hand-roll
KPI thresholds — always read from this file.
"""

import json
import os
import time

import requests
from eth_abi import decode
from eth_hash.auto import keccak


PROXY = "0x42bfaaa203b8259270a1b5ef4576db6b8359daa1"
RPC   = "https://mainnet.megaeth.com/rpc"

# Event topic0 hashes (keccak256 of event signature)
TOPIC_KPI_CREATED        = "0x" + keccak(b"KpiCreated(uint32,string,string,string,string)").hex()
TOPIC_TRANCHE_CREATED    = "0x" + keccak(b"TrancheCreated(uint32,uint32,uint32,uint32,uint256,address,string)").hex()
TOPIC_TRANCHE_STATUS     = "0x" + keccak(b"TrancheStatusUpdated(uint32,uint8)").hex()
TOPIC_TRANCHE_ACHIEVEMENT = "0x" + keccak(b"TrancheAchievementDataSet(uint32,uint32,uint256)").hex()


def rpc(method, params):
    r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=60)
    r.raise_for_status()
    return r.json()


def get_logs(topic):
    res = rpc("eth_getLogs", [{
        "address":   PROXY,
        "fromBlock": "0x0",
        "toBlock":   "latest",
        "topics":    [topic],
    }])
    if "error" in res:
        raise RuntimeError(res["error"])
    return res.get("result") or []


def decode_kpi_created(logs):
    out = []
    for lg in sorted(logs, key=lambda l: int(l["topics"][1], 16)):
        kid = int(lg["topics"][1], 16)
        name, desc, category, source = decode(
            ["string", "string", "string", "string"],
            bytes.fromhex(lg["data"][2:]),
        )
        out.append({
            "id": kid, "name": name, "description": desc,
            "category": category, "source_url": source,
        })
    return out


def decode_tranche_created(logs):
    out = []
    for lg in sorted(logs, key=lambda l: int(l["topics"][1], 16)):
        tid = int(lg["topics"][1], 16)
        kid = int(lg["topics"][2], 16)
        f3, f4, reward, benef, label = decode(
            ["uint32", "uint32", "uint256", "address", "string"],
            bytes.fromhex(lg["data"][2:]),
        )
        out.append({
            "id":          tid,
            "kpi_id":      kid,
            "reward_mega": reward / 1e18,
            "beneficiary": benef,
            "label":       label,
            "extra":       {"f3": f3, "f4_status_at_create": f4},
        })
    return out


def decode_status_updates(logs):
    """Last status per tranche (events ordered by block, last write wins)."""
    by_tranche = {}
    for lg in sorted(logs, key=lambda l: (int(l["blockNumber"], 16), int(l["logIndex"], 16))):
        raw = bytes.fromhex(lg["data"][2:])
        tid    = int(raw[0:32].hex(), 16)
        status = int(raw[32:64].hex(), 16)
        by_tranche[tid] = {
            "status":     status,
            "block":      int(lg["blockNumber"], 16),
            "tx_hash":    lg["transactionHash"],
        }
    return by_tranche


def decode_achievements(logs):
    """Last achievement value per tranche."""
    by_tranche = {}
    for lg in sorted(logs, key=lambda l: (int(l["blockNumber"], 16), int(l["logIndex"], 16))):
        raw = bytes.fromhex(lg["data"][2:])
        tid = int(raw[0:32].hex(), 16)
        f2  = int(raw[32:64].hex(), 16)
        val = int(raw[64:96].hex(), 16)
        by_tranche[tid] = {
            "value":   val,
            "f2":      f2,
            "block":   int(lg["blockNumber"], 16),
            "tx_hash": lg["transactionHash"],
        }
    return by_tranche


def run():
    print("[onchain_kpis] pulling events from MegaStaking…")
    kpi_logs        = get_logs(TOPIC_KPI_CREATED)
    tranche_logs    = get_logs(TOPIC_TRANCHE_CREATED)
    status_logs     = get_logs(TOPIC_TRANCHE_STATUS)
    achievement_logs = get_logs(TOPIC_TRANCHE_ACHIEVEMENT)

    print(f"  KPIs:         {len(kpi_logs)}")
    print(f"  Tranches:     {len(tranche_logs)}")
    print(f"  Status events: {len(status_logs)}")
    print(f"  Achievements: {len(achievement_logs)}")

    kpis     = decode_kpi_created(kpi_logs)
    tranches = decode_tranche_created(tranche_logs)
    status   = decode_status_updates(status_logs)
    achievements = decode_achievements(achievement_logs)

    # Merge status + achievement onto each tranche
    for t in tranches:
        s = status.get(t["id"])
        if s:
            t["onchain_status"]     = s["status"]
            t["onchain_status_block"] = s["block"]
            t["onchain_status_tx"]    = s["tx_hash"]
        else:
            t["onchain_status"] = None
        a = achievements.get(t["id"])
        if a:
            t["achievement_value"]    = a["value"]
            t["achievement_block"]    = a["block"]
            t["achievement_tx"]       = a["tx_hash"]
        else:
            t["achievement_value"] = None

    out = {
        "synced_at":       int(time.time()),
        "source_contract": PROXY,
        "kpis":            kpis,
        "tranches":        tranches,
        "totals": {
            "kpi_count":     len(kpis),
            "tranche_count": len(tranches),
            "total_reward_mega_allocated": sum(t["reward_mega"] for t in tranches),
            "total_pool_mega":  5_330_000_000,
            "unallocated_mega": 5_330_000_000 - sum(t["reward_mega"] for t in tranches),
            "tranches_attested_onchain": sum(1 for t in tranches if t["onchain_status"] is not None),
        },
    }

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "data")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "onchain_kpis.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"[onchain_kpis] wrote {path} ({os.path.getsize(path):,} bytes)")
    print(f"  {len(kpis)} KPIs · {len(tranches)} tranches · "
          f"{out['totals']['total_reward_mega_allocated']/1e6:.0f}M allocated "
          f"({out['totals']['unallocated_mega']/1e6:.0f}M unallocated dry powder)")
    return out


if __name__ == "__main__":
    run()
