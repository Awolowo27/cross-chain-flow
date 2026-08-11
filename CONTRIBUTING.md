# Contributing to `cross-chain-flow`

Thank you for your interest in expanding **`cross-chain-flow`**! We welcome community contributions to onboard new cross-chain bridge protocols and EVM networks.

To maintain dataset consistency across the unified ledger, all Pull Requests **must strictly adhere to the Standard Protocol Schema Contract**.

---

## Architecture Overview

Adding a new bridge protocol follows a modular 4-step pipeline pattern:

```
[Raw Event Logs] 
      │
      ▼
1. Macros (macros/<protocol>/) ──► Extract & unpack hex log data
      │
      ▼
2. Protocol Model (models/protocols/<protocol>/) ──► LEFT JOIN deposits & fills on correlation_id
      │
      ▼
3. Unified Union (models/unified/unified_bridge_flows.sql) ──► UNION ALL into master ledger
      │
      ▼
4. Mandatory Schema Testing (models/protocols/schema.yml) ──► Enforce Standard Protocol Contract
```

---

## Mandatory 4-Step Protocol Onboarding Guide

### Step 1: Create Event Extraction Macros (`macros/<protocol>/`)
Create a new directory under `macros/<protocol_name>/` and implement two Jinja macros to parse source deposits and destination fulfillments from raw EVM log events:

1. **`v2_<protocol>_sent.sql`** (Source Chain Deposit Macro):
   - Filter logs by `topic0` (the event signature hash).
   - Unpack 64-character hexadecimal `data` topics using `SAFE_OFFSET` or bitwise extraction.
   - Standardize output columns: `transaction_hash`, `block_timestamp`, `correlation_id`, `source_chain`, `destination_chain`, `user_address`, `token_address`, `raw_amount`.

2. **`v2_<protocol>_received.sql`** (Destination Chain Fill Macro):
   - Filter destination logs by `topic0`.
   - Unpack parameters and standardize output columns: `transaction_hash`, `block_timestamp`, `correlation_id`, `destination_chain`, `user_address`, `token_address`, `raw_amount`.

---

### Step 2: Build the Protocol Matching Model (`models/protocols/<protocol>/`)
Create `models/protocols/<protocol>/flow_<protocol>_matched.sql` to correlate deposits and fills:

```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    schema = 'cross_chain_protocol_flows'
) }}

WITH deposits AS (
    {{ v2_mybridge_sent() }}
),
fills AS (
    {{ v2_mybridge_received() }}
)
SELECT
    'MyBridge' AS bridge_name,
    d.correlation_id,
    IF(f.transaction_hash IS NOT NULL, 'COMPLETED', 'PENDING') AS status,
    d.source_chain,
    d.destination_chain,
    d.block_timestamp AS deposit_timestamp,
    f.block_timestamp AS fill_timestamp,
    TIMESTAMP_DIFF(f.block_timestamp, d.block_timestamp, SECOND) AS time_to_fill_seconds,
    d.user_address,
    d.token_address AS token_deposited_address,
    d.raw_amount AS amount_deposited_raw,
    f.token_address AS token_received_address,
    f.raw_amount AS amount_received_raw
FROM deposits d
LEFT JOIN fills f
    ON d.correlation_id = f.correlation_id
```

---

### Step 3: Register Protocol in Unified Ledger (`models/unified/unified_bridge_flows.sql`)
Add your new protocol model to `models/unified/unified_bridge_flows.sql` via `UNION ALL`:

```sql
SELECT * FROM {{ ref('flow_across_matched') }}
UNION ALL
SELECT * FROM {{ ref('flow_debridge_matched') }}
UNION ALL
SELECT * FROM {{ ref('flow_mybridge_matched') }}  -- Your new protocol model
```

---

### Step 4: Mandatory Schema Testing (Standard Protocol Contract)
To prevent dataset corruption, **every protocol model MUST implement the exact Standard Protocol Contract** inside `models/protocols/schema.yml`. Custom status strings (like `'DONE'` or `'SUCCESS'`) are strictly prohibited.

#### Mandatory `schema.yml` Specification:
```yaml
models:
  - name: flow_mybridge_matched
    description: "MyBridge deposit and fill correlated cross-chain flows."
    columns:
      - name: correlation_id
        tests:
          - not_null
      - name: source_chain
        tests:
          - not_null
      - name: status
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['PENDING', 'COMPLETED']
```

---

## Local Verification Commands

Before opening a Pull Request, run local verification:

```bash
# 1. Install dbt packages
uv run dbt deps

# 2. Validate graph compilation
uv run dbt parse

# 3. Validate BigQuery SQL syntax (Offline)
uv run sqlfluff lint models/
```

Once all checks pass cleanly, submit your Pull Request!

---

## Review & Merge Process

1. **Automated CI Validation**: Upon opening a Pull Request, GitHub Actions automatically executes `dbt parse`, `sqlfluff lint` (BigQuery dialect), and Python syntax checks.
2. **Maintainer Code Review**: Once all CI checks pass green, project maintainers will perform a secondary review of your event topic hashes, contract addresses, and schema assertions.
3. **Approval & Merge**: Following successful review and approval, your protocol model will be merged into `main` a
