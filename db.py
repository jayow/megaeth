"""Dual-driver DB layer.

Set DATABASE_URL env var to a Postgres URL (postgres://, postgresql://) to
use Postgres. Otherwise falls back to local SQLite (mega.db). Same API for
both — every script in the project calls connect() and executes portable
SQL (using ON CONFLICT instead of INSERT OR IGNORE).
"""

import os
import sqlite3
from contextlib import contextmanager
from urllib.parse import urlparse

DB_URL = os.environ.get("DATABASE_URL", "").strip()
IS_POSTGRES = DB_URL.startswith("postgres://") or DB_URL.startswith("postgresql://")
DB_PATH = "mega.db"

# Lazy-import psycopg2 only when needed
_pg = None
def _pg_module():
    global _pg
    if _pg is None:
        import psycopg2
        import psycopg2.extras
        _pg = psycopg2
    return _pg


# ─── Schema ────────────────────────────────────────────────────────────────
# Portable SQL — works on both SQLite (>=3.24) and Postgres.
# `value`, `balance`, etc. are TEXT to preserve full uint256 precision.
SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS transfers (
        tx_hash      TEXT NOT NULL,
        log_index    INTEGER NOT NULL,
        block_number INTEGER NOT NULL,
        timestamp    TEXT,
        from_addr    TEXT NOT NULL,
        to_addr      TEXT NOT NULL,
        value        TEXT NOT NULL,
        PRIMARY KEY (tx_hash, log_index)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_transfers_from  ON transfers(from_addr)",
    "CREATE INDEX IF NOT EXISTS idx_transfers_to    ON transfers(to_addr)",
    "CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block_number)",

    """CREATE TABLE IF NOT EXISTS sync_state (
        key   TEXT PRIMARY KEY,
        value TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS address_meta (
        address     TEXT PRIMARY KEY,
        label       TEXT,
        is_contract INTEGER DEFAULT 0,
        note        TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS true_balance (
        address    TEXT PRIMARY KEY,
        balance    TEXT NOT NULL,
        updated_at INTEGER NOT NULL
    )""",

    """CREATE TABLE IF NOT EXISTS eth_entity (
        entity_id TEXT PRIMARY KEY,
        address   TEXT NOT NULL,
        block     INTEGER NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_eth_entity_addr ON eth_entity(address)",

    """CREATE TABLE IF NOT EXISTS eth_allocation (
        entity_id     TEXT PRIMARY KEY,
        accepted_usdt TEXT,
        block         INTEGER NOT NULL
    )""",

    """CREATE TABLE IF NOT EXISTS eth_bid (
        entity_id TEXT NOT NULL,
        block     INTEGER NOT NULL,
        PRIMARY KEY (entity_id, block)
    )""",

    """CREATE TABLE IF NOT EXISTS eth_refunded (
        entity_id TEXT PRIMARY KEY,
        address   TEXT NOT NULL,
        block     INTEGER NOT NULL
    )""",

    """CREATE TABLE IF NOT EXISTS fluffle_owner (
        address  TEXT PRIMARY KEY,
        n_tokens INTEGER NOT NULL
    )""",

    """CREATE TABLE IF NOT EXISTS megaeth_claimed (
        tx_hash           TEXT NOT NULL,
        log_index         INTEGER NOT NULL,
        block             INTEGER NOT NULL,
        distribution_uuid TEXT NOT NULL,
        entity_uuid       TEXT NOT NULL,
        receiver          TEXT NOT NULL,
        amount            TEXT NOT NULL,
        amount_usdc       TEXT NOT NULL,
        amount_carry      TEXT NOT NULL,
        PRIMARY KEY (tx_hash, log_index)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_claimed_entity   ON megaeth_claimed(entity_uuid)",
    "CREATE INDEX IF NOT EXISTS idx_claimed_receiver ON megaeth_claimed(receiver)",

    """CREATE TABLE IF NOT EXISTS cache (
        key        TEXT PRIMARY KEY,
        value      TEXT NOT NULL,
        updated_at INTEGER NOT NULL
    )""",
]


# ─── Connection adapter ───────────────────────────────────────────────────
class _PgRow(dict):
    """psycopg2 RealDictRow but indexable for sqlite-style row['col']."""
    pass


class _PgCursor:
    """Thin wrapper that translates SQLite '?' placeholders to Postgres '%s'."""
    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, params=None):
        if params is None:
            self._cur.execute(_translate(sql))
        else:
            self._cur.execute(_translate(sql), params)
        return self

    def executemany(self, sql, seq):
        self._cur.executemany(_translate(sql), seq)
        return self

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        # psycopg2.extras.RealDictCursor gives dicts; allow dict-style + tuple-style access
        return _PgRowAccess(row) if isinstance(row, dict) else row

    def fetchall(self):
        rows = self._cur.fetchall()
        return [_PgRowAccess(r) if isinstance(r, dict) else r for r in rows]

    def __iter__(self):
        for row in self._cur:
            yield _PgRowAccess(row) if isinstance(row, dict) else row


class _PgRowAccess:
    """Allow row['col'] and row[0] both."""
    def __init__(self, d):
        self._d = d
        self._values = list(d.values())
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._d[key]
    def __contains__(self, key):
        return key in self._d
    def get(self, key, default=None):
        return self._d.get(key, default)
    def keys(self):
        return self._d.keys()


class _PgConn:
    """Wraps a psycopg2 connection so it quacks like a sqlite3.Connection."""
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor(cursor_factory=_pg_module().extras.RealDictCursor)
        wrapped = _PgCursor(cur)
        wrapped.execute(sql, params)
        return wrapped

    def executemany(self, sql, seq):
        cur = self._conn.cursor()
        _PgCursor(cur).executemany(sql, seq)
        return self

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def executescript(self, script):
        # Only used by some legacy callers — split on ; and execute each.
        for stmt in [s.strip() for s in script.split(";") if s.strip()]:
            cur = self._conn.cursor()
            cur.execute(stmt)
            cur.close()


def _translate(sql):
    """Translate SQLite-isms to Postgres."""
    if not IS_POSTGRES:
        return sql
    # ?-placeholders → %s
    return sql.replace("?", "%s")


# ─── Public API ───────────────────────────────────────────────────────────
def connect():
    if IS_POSTGRES:
        pg = _pg_module()
        # Postgres URL — sslmode=require by default unless explicitly set
        conn = pg.connect(DB_URL, sslmode=os.environ.get("PGSSLMODE", "require"))
        wrapped = _PgConn(conn)
        # Ensure schema exists
        for stmt in SCHEMA_STATEMENTS:
            cur = conn.cursor()
            cur.execute(stmt)
            cur.close()
        conn.commit()
        return wrapped
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(stmt)
        conn.commit()
        return conn


@contextmanager
def cursor():
    conn = connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def get_state(key, default=None):
    with cursor() as c:
        row = c.execute("SELECT value FROM sync_state WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        # row['value'] works on both SQLite Row and our PgRowAccess
        return row["value"] if hasattr(row, "keys") else row[0]


def set_state(key, value):
    with cursor() as c:
        c.execute(
            "INSERT INTO sync_state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
            (key, str(value)),
        )


def upsert_meta(address, label=None, is_contract=None, note=None):
    with cursor() as c:
        c.execute(
            "INSERT INTO address_meta(address, label, is_contract, note) "
            "VALUES(?, ?, ?, ?) "
            "ON CONFLICT(address) DO UPDATE SET "
            "  label       = COALESCE(EXCLUDED.label,       address_meta.label), "
            "  is_contract = COALESCE(EXCLUDED.is_contract, address_meta.is_contract), "
            "  note        = COALESCE(EXCLUDED.note,        address_meta.note)",
            (address.lower(), label, is_contract, note),
        )


def transfer_count():
    with cursor() as c:
        return c.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]


def cache_get(key):
    """Return (value_str, updated_at) or (None, 0) if not cached."""
    with cursor() as c:
        row = c.execute("SELECT value, updated_at FROM cache WHERE key = ?", (key,)).fetchone()
        if not row:
            return None, 0
        return row["value"] if hasattr(row, "keys") else row[0], \
               (row["updated_at"] if hasattr(row, "keys") else row[1])


def cache_set(key, value, updated_at=None):
    import time
    if updated_at is None:
        updated_at = int(time.time())
    with cursor() as c:
        c.execute(
            "INSERT INTO cache(key, value, updated_at) VALUES(?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            (key, value, updated_at),
        )
