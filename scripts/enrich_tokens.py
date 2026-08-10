import csv
import json
import os
import urllib.request
import yaml
from google.cloud import bigquery

PROFILES_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "profiles.yml"))
SEED_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "seeds", "dim_tokens.csv"))

def get_project_id():
    try:
        if os.path.exists(PROFILES_PATH):
            with open(PROFILES_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return data.get("cross_chain_fund", {}).get("outputs", {}).get("dev", {}).get("project", "parabolic-eon-490218-i8")
    except Exception as e:
        print(f"Could not load project ID from profiles.yml: {e}")
    return "parabolic-eon-490218-i8"

PROJECT_ID = get_project_id()

LLAMA_CHAIN_MAP = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "base": "base",
    "avalanche": "avax",
    "polygon": "polygon",
}


# STARGATE & BRIDGE POOL OVERRIDE MAP

POOL_OVERRIDE_MAP = {
    # Stargate V1 / V2 Pools (Ethereum)
    ("0x8731d54e9d02c286767d56ac03e8037c07e01e98", "ethereum"): ("USDC", 6),
    ("0xdf0770df86a8034b3efef0a1bb3c889b8332ff56", "ethereum"): ("USDT", 6),
    ("0x72e2f48944e96f9000165715989d9c86f7b15112", "ethereum"): ("ETH", 18),
    ("0x100801d9b57e1d3b07286b244d2e0e21bd39c", "ethereum"): ("USDC", 6),

    # Stargate V1 / V2 Pools (Arbitrum)
    ("0x53bf83b8b021323808273722ab9a19c6396e95c1", "arbitrum"): ("USDC", 6),
    ("0xb6c1b3b2d8ff559f9a464529f48a778e03d03e4", "arbitrum"): ("USDT", 6),
    ("0x915a55e367452f1c437675bed47f5e1e6c2518", "arbitrum"): ("ETH", 18),

    # Stargate V1 / V2 Pools (Optimism)
    ("0xb0d502e938ed5f4df2e681fe6e419ff29631d62b", "optimism"): ("USDC", 6),
    ("0x16513b2d8ff559f9a464529f48a778e03d03e4", "optimism"): ("ETH", 18),

    # Stargate V1 / V2 Pools (Avalanche)
    ("0x1205f6a63c342713f04215014791750617334261", "avalanche"): ("USDC.e", 6),
    ("0x29e38769f23701a2e4a8ef0492e196fb0fc58e40", "avalanche"): ("USDT.e", 6),
    ("0xb57092c683226a27e7f7b3b4fdf4c6b8c8d8b6b2", "avalanche"): ("AVAX", 18),

    # Stargate V1 / V2 Pools (Polygon)
    ("0x1205f6a63c342713f04215014791750617334261", "polygon"): ("USDC", 6),
    ("0x29e38769f23701a2e4a8ef0492e196fb0fc58e40", "polygon"): ("USDT", 6),
    ("0x45a2e57449281a290757861d88113ee186367311", "polygon"): ("POL", 18),
}

def load_existing_tokens(filepath):
    existing = set()
    if not os.path.exists(filepath):
        return existing
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing.add((row["token_address"].lower(), row["blockchain"].lower()))
    return existing

def fetch_distinct_pipeline_tokens(existing_tokens):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT DISTINCT LOWER(token_address) AS token_address, blockchain FROM (
            SELECT token_deposited AS token_address, source_chain AS blockchain 
            FROM `{PROJECT_ID}`.`cross_chain_unified_flows`.`unified_bridge_flows`
            UNION DISTINCT
            SELECT token_received AS token_address, destination_chain AS blockchain 
            FROM `{PROJECT_ID}`.`cross_chain_unified_flows`.`unified_bridge_flows`
        )
        WHERE token_address IS NOT NULL 
          AND token_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        new_tokens = []
        for row in results:
            pair = (row.token_address.lower(), row.blockchain.lower())
            if pair not in existing_tokens:
                new_tokens.append(pair)
        return new_tokens
    except Exception as e:
        print(f"Error querying BigQuery unified_bridge_flows on project '{PROJECT_ID}': {e}")
        return []

def fetch_metadata(token_address, blockchain):
    # 1. Check Pool Override Map first
    pool_key = (token_address.lower(), blockchain.lower())
    if pool_key in POOL_OVERRIDE_MAP:
        symbol, decimals = POOL_OVERRIDE_MAP[pool_key]
        print(f"Found Pool Override for {token_address} on {blockchain}: {symbol} ({decimals} decimals)")
        return symbol, decimals

    # 2. External API Lookup (DefiLlama)
    llama_chain = LLAMA_CHAIN_MAP.get(blockchain, blockchain)
    coin_id = f"{llama_chain}:{token_address}"
    url = f"https://coins.llama.fi/prices/current/{coin_id}"
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            coins = data.get("coins", {})
            if coin_id in coins:
                symbol = coins[coin_id].get("symbol", "UNKNOWN")
                decimals = coins[coin_id].get("decimals", 18)
                return symbol, decimals
    except Exception as e:
        print(f"Error fetching metadata for {coin_id}: {e}")
        
    return "UNKNOWN", 18

def main():
    print(f"Running Token Enrichment using GCP Project: {PROJECT_ID}")
    existing_tokens = load_existing_tokens(SEED_PATH)
    new_pipeline_tokens = fetch_distinct_pipeline_tokens(existing_tokens)
    
    if not new_pipeline_tokens:
        print(f"No new tokens discovered. All tokens in pipeline are already saved in {SEED_PATH}.")
        return

    print(f"Discovered {len(new_pipeline_tokens)} brand-new unmapped token(s). Fetching metadata...")
    new_tokens_added = 0
    with open(SEED_PATH, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for token_address, blockchain in new_pipeline_tokens:
            print(f"Processing new token: {token_address} on {blockchain}")
            symbol, decimals = fetch_metadata(token_address, blockchain)
            writer.writerow([token_address, blockchain, symbol, decimals])
            existing_tokens.add((token_address, blockchain))
            new_tokens_added += 1
                
    print(f"Enrichment completed! {new_tokens_added} new token(s) appended to {SEED_PATH}")

if __name__ == "__main__":
    main()
