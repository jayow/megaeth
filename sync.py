"""Fetch all MEGA Transfer events from Blockscout and store in SQLite.

Resumable: tracks the next_page_params token in sync_state. Each pull
inserts ON CONFLICT IGNORE so re-runs are idempotent.
"""

import json
import sys
import time
import urllib.parse

import requests

from config import BLOCKSCOUT, TOKEN
from db import connect, get_state, set_state, transfer_count


PAGE_URL = f"{BLOCKSCOUT}/api/v2/tokens/{TOKEN}/transfers"
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "mega-analyzer/1.0"})


def fetch_page(params=None):
    for attempt in range(6):
        try:
            r = SESSION.get(PAGE_URL, params=params or {}, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"  retry {attempt+1}: {e}", file=sys.stderr)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {PAGE_URL} {params}")


def normalize(item):
    """Pull (tx_hash, log_index, block, ts, from, to, value) from a Blockscout item."""
    tx = item.get("transaction_hash") or item.get("tx_hash")
    log_index = item.get("log_index")
    block = item.get("block_number") or item.get("block")
    ts = item.get("timestamp")
    from_addr = (item.get("from") or {}).get("hash", "").lower()
    to_addr = (item.get("to") or {}).get("hash", "").lower()
    total = item.get("total") or {}
    value = total.get("value") or item.get("value") or "0"
    return (tx, log_index, block, ts, from_addr, to_addr, value)


def insert_batch(conn, rows):
    conn.executemany(
        "INSERT INTO transfers(tx_hash,log_index,block_number,timestamp,from_addr,to_addr,value) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(tx_hash, log_index) DO NOTHING",
        rows,
    )
    conn.commit()


def run():
    conn = connect()
    next_params_str = get_state("next_page_params")
    page_idx = int(get_state("page_idx", "0"))

    if next_params_str:
        params = json.loads(next_params_str)
        print(f"Resuming from page {page_idx}, params={params}")
    else:
        params = {}
        print("Starting fresh sync")

    start_count = transfer_count()
    last_log = time.time()

    while True:
        data = fetch_page(params)
        items = data.get("items") or []
        if not items:
            print("No items returned, done.")
            break

        rows = [normalize(i) for i in items]
        rows = [r for r in rows if r[0] and r[4] and r[5]]
        insert_batch(conn, rows)
        page_idx += 1

        next_params = data.get("next_page_params")
        if not next_params:
            set_state("next_page_params", "")
            set_state("page_idx", str(page_idx))
            print(f"Reached end at page {page_idx}, total transfers stored: {transfer_count()}")
            break

        set_state("next_page_params", json.dumps(next_params))
        set_state("page_idx", str(page_idx))
        params = next_params

        if time.time() - last_log > 5:
            tc = transfer_count()
            print(f"  page {page_idx}: stored {tc} transfers (+{tc - start_count} this run)")
            last_log = time.time()

    print(f"Done. Total transfers in DB: {transfer_count()}")


if __name__ == "__main__":
    run()
