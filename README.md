# MEGA Holder Analytics

Real-time holder behavior dashboard for the **MEGA token** on **MegaETH mainnet**.
Built by [Hanyon Analytics](https://hanyon.app).

## What it shows

- **Eligibility cross-reference** — Fluffle NFT holders + Conviction Round (SONAR) auction winners + Echo private round + Mainnet Campaign recipients, all unioned from on-chain events on both Ethereum mainnet and MegaETH.
- **Behavior buckets per round** — for every (wallet × round) allocation: held / staked / sold / bought-more / pending / locked-until-2027.
- **Per-volume vs per-wallet donuts** — what's still on-chain, what cycled out via DEX.
- **Activity timeline** — hourly claims/sells/buys with hover tooltips.
- **Searchable, sortable, paginated address table** — drill into any wallet, click through to Blockscout to verify any tx.
- **Stake-aware accounting** — sending MEGA to MegaStaking counts as "still holding", not "sold".

## Architecture

| Layer | What it does |
|---|---|
| `sync.py` | Pulls all MEGA Transfer events via Blockscout API (resumable, paginated) |
| `eth_chain.py` | Pulls Echo MegaSale (Sonar auction) entities + allocations and Fluffle NFT holders from Ethereum mainnet via JSON-RPC |
| `megaeth_claims.py` | Pulls `Claimed` events from the Distributor proxies on MegaETH (cross-chain entityUUID matching) |
| `balances.py` | Authoritative current MEGA balances via direct `balanceOf()` eth_calls (catches constructor-minted treasury wallets that don't appear in Transfer events) |
| `classify.py` | Heuristically detects: Distributor proxies, batcher EOAs, DEX pairs (excludes MegaStaking) |
| `aggregate.py` | Per-address aggregator: claimed / bought / sold / staked / current |
| `server.py` | Flask app — `/api/behavior`, `/api/eligibility`, `/api/timeline`, `/api/wallet/<addr>`, `/api/cron/sync` |
| `templates/index.html` | Single-page dashboard with WudooMono + MegaETH design system |
| `db.py` | Dual driver — SQLite locally, Postgres on Vercel via `DATABASE_URL` |

## Local dev

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/python sync.py            # initial seed (~10 min for first run)
./venv/bin/python classify.py
./venv/bin/python balances.py
./venv/bin/python megaeth_claims.py
./venv/bin/python eth_chain.py
./venv/bin/python server.py          # http://127.0.0.1:5050
```

Hourly incremental updates:
```bash
./venv/bin/python update.py
```

## Deploy to Vercel

See [DEPLOY.md](./DEPLOY.md) — provisions Postgres, migrates from SQLite, deploys Flask via `@vercel/python`, registers the hourly cron.

## Contracts referenced

| Contract | Address | Chain |
|---|---|---|
| MEGA token (ERC-20) | `0x28B7E77f82B25B95953825F1E3eA0E36c1c29861` | MegaETH |
| Echo Distributor proxy #1 | `0xcf4b83ce5273adaeb0221b645240f0b68678d7a1` | MegaETH |
| Echo Distributor proxy #2 | `0x661a638d15b78b514f22c3c8741a049edcc7a990` | MegaETH |
| Fluffle batcher EOA | `0x847f754511d10f603e950359461c78fcc72075ee` | MegaETH |
| MegaStaking proxy | `0x42bfaaa203b8259270a1b5ef4576db6b8359daa1` | MegaETH |
| Conviction (Sonar) Public Sale | `0xab02bf85a7a851b6a379ea3d5bd3b9b4f5dd8461` | Ethereum |
| Fluffle NFT (soulbound) | `0x4e502ab1bb313b3c1311eb0d11b31a6b62988b86` | Ethereum |
