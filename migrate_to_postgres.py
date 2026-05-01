"""One-shot migration: copy local SQLite (mega.db) into Postgres.

Usage:
    DATABASE_URL=postgres://user:pass@host/db ./venv/bin/python migrate_to_postgres.py

Idempotent: uses INSERT ON CONFLICT DO NOTHING so re-running just tops off.
"""

import os
import sqlite3
import sys
import time

if not os.environ.get("DATABASE_URL", "").startswith(("postgres://", "postgresql://")):
    print("ERROR: set DATABASE_URL=postgres://... before running this script.")
    sys.exit(1)

# Import db AFTER setting env so it picks up Postgres mode
from db import connect, SCHEMA_STATEMENTS  # noqa: E402

import psycopg2
import psycopg2.extras


SQLITE_PATH = "mega.db"

TABLES = [
    # (table, columns, primary_key_cols)
    ("transfers",       ["tx_hash","log_index","block_number","timestamp","from_addr","to_addr","value"],          ["tx_hash","log_index"]),
    ("sync_state",      ["key","value"],                                                                            ["key"]),
    ("address_meta",    ["address","label","is_contract","note"],                                                   ["address"]),
    ("true_balance",    ["address","balance","updated_at"],                                                         ["address"]),
    ("eth_entity",      ["entity_id","address","block"],                                                            ["entity_id"]),
    ("eth_allocation",  ["entity_id","accepted_usdt","block"],                                                      ["entity_id"]),
    ("eth_bid",         ["entity_id","block"],                                                                      ["entity_id","block"]),
    ("eth_refunded",    ["entity_id","address","block"],                                                            ["entity_id"]),
    ("fluffle_owner",   ["address","n_tokens"],                                                                     ["address"]),
    ("megaeth_claimed", ["tx_hash","log_index","block","distribution_uuid","entity_uuid","receiver","amount","amount_usdc","amount_carry"], ["tx_hash","log_index"]),
]


def migrate():
    print(f"Reading from: {SQLITE_PATH}")
    print(f"Writing to:   {os.environ['DATABASE_URL'].split('@')[-1]}\n")

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn_raw = psycopg2.connect(os.environ["DATABASE_URL"], sslmode=os.environ.get("PGSSLMODE","require"))
    pg_cur = pg_conn_raw.cursor()

    # Apply schema first
    print("→ Applying schema…")
    for stmt in SCHEMA_STATEMENTS:
        pg_cur.execute(stmt)
    pg_conn_raw.commit()
    print("  ✓ schema ready\n")

    for tbl, cols, pk in TABLES:
        try:
            n = sqlite_conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except sqlite3.OperationalError:
            print(f"→ {tbl:<20} (no source table — skipping)")
            continue
        if n == 0:
            print(f"→ {tbl:<20} (empty — skipping)")
            continue

        t0 = time.time()
        col_csv = ",".join(cols)
        placeholders = ",".join(["%s"] * len(cols))
        pk_csv = ",".join(pk)
        sql = f"INSERT INTO {tbl}({col_csv}) VALUES({placeholders}) ON CONFLICT({pk_csv}) DO NOTHING"

        BATCH = 5000
        copied = 0
        rows_iter = sqlite_conn.execute(f"SELECT {col_csv} FROM {tbl}")
        batch = []
        for r in rows_iter:
            batch.append(tuple(r))
            if len(batch) >= BATCH:
                psycopg2.extras.execute_batch(pg_cur, sql, batch, page_size=BATCH)
                copied += len(batch)
                batch = []
                pg_conn_raw.commit()
                print(f"  {tbl}: {copied:,}/{n:,}", end="\r", flush=True)
        if batch:
            psycopg2.extras.execute_batch(pg_cur, sql, batch, page_size=len(batch))
            copied += len(batch)
            pg_conn_raw.commit()

        print(f"→ {tbl:<20} {copied:>8,} rows  ({time.time()-t0:.1f}s)")

    pg_cur.close()
    pg_conn_raw.close()
    sqlite_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    migrate()
