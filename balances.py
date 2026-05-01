"""Fetch authoritative current MEGA balances directly from the token contract.

Blockscout's /holders endpoint is missing big constructor-minted balances
(no Transfer event was emitted on initial allocation). We instead query
`balanceOf(address)` on every address that ever appeared as `from` or
`to` in our Transfer log — this catches everyone who's ever held MEGA,
including the constructor-allocated treasury wallets.

Uses MegaETH JSON-RPC eth_call. We batch via concurrent requests for
speed (8k+ addresses).
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from db import connect


RPC = "https://mainnet.megaeth.com/rpc"
TOKEN = "0x28B7E77f82B25B95953825F1E3eA0E36c1c29861"

def init():
    # Schema lives in db.py SCHEMA_STATEMENTS; connect() creates it.
    return connect()


SESSION = requests.Session()


def balance_of(addr):
    """Single eth_call balanceOf."""
    padded = addr[2:].rjust(64, "0")
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": TOKEN, "data": "0x70a08231" + padded}, "latest"],
    }
    for attempt in range(4):
        try:
            r = SESSION.post(RPC, json=payload, timeout=15)
            r.raise_for_status()
            j = r.json()
            if "result" in j:
                return int(j["result"], 16)
            time.sleep(0.5 * (attempt + 1))
        except requests.RequestException:
            time.sleep(0.5 * (attempt + 1))
    return None


def run():
    conn = init()
    # Every address that ever appeared in transfers
    addrs = {r["a"] for r in conn.execute(
        "SELECT from_addr AS a FROM transfers UNION SELECT to_addr AS a FROM transfers"
    )}
    addrs.discard("0x0000000000000000000000000000000000000000")

    # Skip already-fetched (resume support)
    done = {r["address"] for r in conn.execute("SELECT address FROM true_balance")}
    todo = sorted(addrs - done)
    print(f"Total addresses: {len(addrs)}, already done: {len(done)}, to fetch: {len(todo)}")

    if not todo:
        summarize(conn)
        return

    now = int(time.time())
    saved = 0
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(balance_of, a): a for a in todo}
        for fut in as_completed(futures):
            a = futures[fut]
            bal = fut.result()
            if bal is None:
                continue
            conn.execute(
                "INSERT INTO true_balance(address,balance,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(address) DO UPDATE SET balance = EXCLUDED.balance, updated_at = EXCLUDED.updated_at",
                (a, str(bal), now),
            )
            saved += 1
            if saved % 250 == 0:
                conn.commit()
                print(f"  saved {saved}/{len(todo)}", file=sys.stderr)
    conn.commit()
    print(f"\nSaved {saved} balances")
    summarize(conn)


def summarize(conn):
    n     = conn.execute("SELECT COUNT(*) FROM true_balance WHERE CAST(balance AS REAL) > 0").fetchone()[0]
    total = conn.execute("SELECT SUM(CAST(balance AS REAL))/1e18 FROM true_balance").fetchone()[0] or 0
    print(f"\n=== Authoritative MEGA balances ===")
    print(f"  Holders with positive balance: {n:>6,}")
    print(f"  Sum of all balances:           {total:>18,.2f} MEGA")
    print(f"  Difference vs 10B totalSupply: {10e9 - total:>18,.2f}")

    print(f"\nTop 15 holders by balance:")
    for r in conn.execute(
        "SELECT address, CAST(balance AS REAL)/1e18 AS m FROM true_balance ORDER BY m DESC LIMIT 15"
    ):
        print(f"  {r['address']}  {r['m']:>16,.2f} MEGA")


if __name__ == "__main__":
    run()
