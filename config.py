"""Config for the MEGA token analyzer."""

TOKEN = "0x28B7E77f82B25B95953825F1E3eA0E36c1c29861"
BLOCKSCOUT = "https://megaeth.blockscout.com"
RPC = "https://mainnet.megaeth.com/rpc"
CHAIN_ID = 4326
DEPLOY_BLOCK = 1592579

# Filled in by the classifier (or override here if known).
# Heuristic: address that has sent MEGA to the most unique recipients.
CLAIM_CONTRACT = None

# DEX pair / router addresses (auto-detected; can override).
DEX_ADDRESSES = set()

DB_PATH = "mega.db"

ZERO_ADDR = "0x0000000000000000000000000000000000000000"
