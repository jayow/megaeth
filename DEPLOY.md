# Deploy to Vercel

## 1. Provision Postgres

Pick one (all have free tiers, all give you a `postgres://` URL):

| Option | Why |
|---|---|
| **Vercel Postgres** | Easiest — zero-config, billed with Vercel |
| **Neon** | Generous free tier (3 GB), what Vercel Postgres uses under the hood |
| **Supabase** | If you also want auth/storage later |

Get the connection URL — looks like:
`postgres://user:pass@host.neon.tech/neondb?sslmode=require`

## 2. Migrate local SQLite → Postgres

One-shot copy of all current data (transfers, balances, eligibility lists):

```bash
DATABASE_URL='postgres://user:pass@host/db' ./venv/bin/python migrate_to_postgres.py
```

Verify by re-running the dashboard locally pointed at Postgres:

```bash
DATABASE_URL='postgres://user:pass@host/db' ./venv/bin/python server.py
# open http://127.0.0.1:5050
```

## 3. Deploy to Vercel

```bash
# from project root
vercel        # first time → links project, asks for DATABASE_URL
vercel --prod # promote
```

Or via the dashboard: import the repo, set env vars:

| Env var | Value |
|---|---|
| `DATABASE_URL` | your Postgres URL from step 1 |
| `CRON_SECRET` | a random string — locks down `/api/cron/sync` |
| `PGSSLMODE` | `require` (default) — set `disable` only if your provider says so |

## 4. Cron is automatic

`vercel.json` registers an hourly cron hitting `/api/cron/sync` at `:00`.
Vercel sends `Authorization: Bearer $CRON_SECRET` automatically.

The cron does the **lightweight pipeline only** (sync new transfers + classify + cache refresh) — fits in 60s.

## 5. Heavy jobs (run separately)

`balances.py` (~16k eth_calls, 5–10 min) and `megaeth_claims.py` (RPC log scan)
are too long for Vercel's 60s limit. Two clean ways:

**A. GitHub Actions** — `.github/workflows/heavy-update.yml`:
```yaml
on:
  schedule: [ { cron: '0 6 * * *' } ]   # daily 06:00 UTC
jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.13' }
      - run: pip install -r requirements.txt
      - env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: |
          python balances.py
          python megaeth_claims.py
```

**B. Cron locally on a small box** (Raspberry Pi / always-on Mac mini):
```bash
DATABASE_URL='postgres://...' ./venv/bin/python update.py
```

## 6. Local dev still works (SQLite)

If you DON'T set `DATABASE_URL`, everything falls back to local `mega.db`.
Same code, same scripts, same dashboard.
