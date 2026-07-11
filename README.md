# Soft Cart — End-to-End Data Engineering Pipeline

A production-style ELT pipeline for a fictional e-commerce company, built to demonstrate a full modern data stack: cloud storage, a cloud data warehouse, transformation with testing, and automated orchestration.

## Architecture

Source data (synthetic)
        |
        v
Azure Data Lake Storage Gen2 (Bronze - raw landing zone)
        |  Storage Integration (Azure AD trust, no hardcoded keys)
        v
Snowflake - RAW schema (COPY INTO)
        |  dbt
        v
Snowflake - STAGING schema (cleaned, contract-enforced, tested)
        |  dbt
        v
Snowflake - MARTS schema (star schema: dimensions + facts)

All of the above is orchestrated end-to-end by **Apache Airflow**, running in Docker, and version controlled here on GitHub.

## Tech stack

| Layer | Tool |
|---|---|
| Data lake | Azure Data Lake Storage Gen2 |
| Data warehouse | Snowflake |
| Transformation & testing | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow (Docker Compose, CeleryExecutor) |
| Version control | Git / GitHub |
| Dev environment | GitHub Codespaces |

## Project structure

    softcart-data-pipeline/
    ├── models/
    │   ├── staging/
    │   │   ├── stg_customers.sql
    │   │   ├── stg_products.sql
    │   │   ├── stg_orders.sql
    │   │   ├── stg_order_items.sql
    │   │   ├── sources.yml
    │   │   └── stg_schema.yml
    │   └── marts/
    │       ├── dim_customers.sql
    │       ├── dim_products.sql
    │       ├── fct_orders.sql
    │       └── marts_schema.yml
    ├── macros/
    │   └── generate_schema_name.sql
    ├── dbt_project.yml
    ├── airflow/
    │   ├── docker-compose.yaml
    │   └── dags/
    │       └── softcart_pipeline.py
    └── README.md

- `models/staging/` - 1:1 cleaned models from raw source tables
- `models/staging/sources.yml` - declares raw Snowflake tables as dbt sources
- `models/staging/stg_schema.yml` - schema contracts + data quality tests
- `models/marts/` - business-facing star schema
- `macros/generate_schema_name.sql` - ensures models land in exact schema names (no prefixing)
- `airflow/docker-compose.yaml` - full Airflow stack (Postgres, Redis, webserver, scheduler, worker, triggerer, dbt runner)
- `airflow/dags/softcart_pipeline.py` - the orchestration DAG

## Data model

**Source system (simulated OLTP + synthetic generator):**
- `customers` - 500 rows
- `products` - 200 rows
- `orders` - order header, includes `payment_status` and `shipment_status`
- `order_items` - line items per order (grain of the fact table)

**Gold-layer star schema:**
- `dim_customers`
- `dim_products`
- `fct_orders` - grain: one row per order line item, joined with order header info

## Data quality & schema enforcement

- All 4 staging models have **enforced dbt contracts** - dbt will refuse to build a model if its actual output columns/types don't exactly match what's declared, catching schema drift before it reaches production.
- **17 automated data tests**, covering:
  - Primary key uniqueness and not-null checks
  - Foreign key referential integrity (`relationships` tests between orders to customers, order_items to orders/products)
  - Valid value enforcement (`accepted_values` on payment_status, shipment_status)

Run everything with: `dbt build`

## Orchestration (Airflow)

The DAG `softcart_pipeline` runs daily and chains three tasks:

1. **`generate_and_upload_orders`** - generates a new batch of synthetic orders (Faker) and uploads them as CSV to the ADLS Gen2 `bronze` container, under `incremental/`.
2. **`load_new_orders_to_snowflake`** - runs a `COPY INTO` that loads only the newly uploaded file into `softcart_db.raw.orders`, using the filename passed via Airflow XCom.
3. **`run_dbt_build`** - triggers `dbt build` (all models + all tests) in a dedicated, isolated dbt container, refreshing the staging and marts layers with the new data.

### Why a separate dbt container?
`dbt-snowflake` and Airflow's own Snowflake provider have conflicting dependency requirements (specifically around `snowflake-connector-python`) that make them impossible to install in the same Python environment. Rather than fight that, dbt runs in its own lightweight container (`python:3.12-slim` + dbt installed at startup), and Airflow triggers it via `docker exec`, using a Docker-socket mount for cross-container control.

## Running this locally / in a Codespace

    cd airflow
    mkdir -p dags logs plugins config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    docker compose up airflow-init
    docker compose up -d

Airflow UI: `http://localhost:8080` (default credentials: `airflow` / `airflow`)

**Required Airflow Connections** (set via CLI, not the web UI - see note below):
- `azure_data_lake_default` - type `adls`, with `connection_string` set in the Extra field
- `snowflake_default` - type `snowflake`, with account/warehouse/database/role configured

**Required dbt profile** (inside the `dbt` container, at `/root/.dbt/profiles.yml`):

    softcart:
      target: dev
      outputs:
        dev:
          type: snowflake
          account: your_account
          user: your_user
          password: your_password
          role: ACCOUNTADMIN
          database: softcart_db
          warehouse: softcart_wh
          schema: staging
          threads: 4

> **Note on Airflow connections:** the web UI's "Add Connection" form was unreliable for long secrets (connection strings) in testing - it silently truncated/mangled pasted values. Setting connections via `airflow connections add` on the CLI proved reliable and is the recommended approach for this project.

## Known environment notes

- This project was developed and tested in **GitHub Codespaces** rather than a local machine, after the full Airflow stack (7 containers) proved too resource-heavy for local Docker on the development laptop.
- The Airflow scheduler has shown occasional instability immediately after a full container restart in the Codespace environment (a `docker compose restart airflow-scheduler` reliably resolves it). This is an environment resource constraint, not an application bug.
- Order IDs in the synthetic generator are derived from a timestamp + row index to guarantee uniqueness across multiple pipeline runs per day.

## Status

Done:
- ADLS Gen2 + Snowflake storage integration
- dbt models, contracts, and 17 passing data quality tests
- Full 3-task Airflow DAG, tested and passing end-to-end
- Version controlled on GitHub

Not yet implemented (roadmap):
- CI/CD via GitHub Actions (dbt compile/lint checks on PR)
- dbt docs site generation
- BI/dashboard layer on top of the marts schema
- Incremental materialization for `fct_orders`
- Failure alerting (email/Slack)
