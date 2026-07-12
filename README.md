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
| CI | GitHub Actions |
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
    ├── .github/
    │   └── workflows/
    │       └── dbt-ci.yml
    └── README.md

- `models/staging/` - 1:1 cleaned models from raw source tables
- `models/staging/sources.yml` - declares raw Snowflake tables as dbt sources
- `models/staging/stg_schema.yml` - schema contracts + data quality tests
- `models/marts/` - business-facing star schema
- `macros/generate_schema_name.sql` - ensures models land in exact schema names (no prefixing)
- `airflow/docker-compose.yaml` - full Airflow stack (Postgres, Redis, webserver, scheduler, worker, triggerer, dbt runner)
- `airflow/dags/softcart_pipeline.py` - the orchestration DAG
- `.github/workflows/dbt-ci.yml` - CI workflow (dbt compile + DAG validation)

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

## Change Tracking (SCD Snapshots)

Customer and product dimensions can change over time (e.g. a customer moves cities, a product's price changes). Instead of overwriting old values, this project uses **dbt snapshots** to implement **SCD Type 2** tracking: every change is preserved as history, with each row marked by the time range during which it was the current, valid version.

- `snapshots/dim_customers_snapshot.sql` - tracks changes to `first_name`, `last_name`, `email`, `city`, `country`
- `snapshots/dim_products_snapshot.sql` - tracks changes to `product_name`, `category`, `brand`, `unit_price`

Each snapshot adds four metadata columns automatically: `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`. A row with `dbt_valid_to = NULL` is the current version; any row with a populated `dbt_valid_to` is historical.

### Why the check strategy instead of timestamp

dbt snapshots support two strategies for detecting changes:

- **timestamp** - relies on a source column (e.g. `updated_at`) that the source system updates whenever a row changes. Efficient, but only as reliable as the source's discipline in maintaining that column.
- **check** - compares the actual values of a specified list of columns between snapshot runs, with no timestamp column required.

The synthetic `customers` and `products` source tables in this project have no `updated_at` column, so the `timestamp` strategy is not available. The `check` strategy was used instead, explicitly listing the columns worth tracking for each entity. This was verified by directly updating a test row in `raw.customers` and re-running `dbt snapshot`, confirming the old value was closed out (`dbt_valid_to` populated) and a new current row was inserted.

Run snapshots manually with:

    docker exec -it airflow-dbt-1 dbt snapshot --project-dir /opt/dbt_project --profiles-dir /root/.dbt

## Clickstream Event Tracking

In addition to order data, this project tracks simulated browsing behavior (clickstream events) to model a realistic customer funnel: browsing -> viewing a product -> adding to cart -> purchasing.

### Event shape

Clickstream data is semi-structured (different event types carry different fields), so it's stored as JSON rather than CSV. Each event looks like:

    {
      "event_id": "evt_...",
      "session_id": "sess_...",
      "customer_id": 42,
      "event_type": "product_view",
      "product_id": 1234,
      "event_timestamp": "2026-07-12T14:22:03"
    }

`event_type` is one of `page_view`, `product_view`, `add_to_cart`, or `purchase`. Not every session reaches every stage, simulating realistic drop-off through the funnel.

### Pipeline

1. `generate_and_upload_clickstream` (in the DAG) generates ~30 fake browsing sessions per run and uploads them as JSON Lines to ADLS Gen2, under `clickstream/`.
2. Raw JSON is loaded into `raw.clickstream_events` (a single `VARIANT` column holding the full event, plus a `loaded_at` timestamp), using a dedicated `json_format` file format.
3. `stg_clickstream_events` flattens the JSON into clean, typed columns (`event_id`, `session_id`, `customer_id`, `event_type`, `product_id`, `event_timestamp`).
4. `fct_clickstream_events` is the final marts-layer fact table, materialized as `incremental` (new events are appended by `event_timestamp`, existing events are never updated).

### One-off setup macros

A few macros in `macros/` were used as one-time setup helpers while building this feature, since `dbt show` only supports read queries and these are schema-altering statements:

- `setup_json_format.sql` - created the `json_format` file format in Snowflake (one-time; already applied)
- `create_clickstream_table.sql` - created the `raw.clickstream_events` table (one-time; already applied)
- `load_clickstream_test.sql` - loaded a test batch of clickstream JSON into the raw table during development

These aren't part of the regular pipeline and don't need to be run again; they're kept for reference/reproducibility.

## Orchestration (Airflow)

The DAG `softcart_pipeline` runs daily and chains three tasks:

1. **`generate_and_upload_orders`** - generates a new batch of synthetic orders (Faker) and uploads them as CSV to the ADLS Gen2 `bronze` container, under `incremental/`.
2. **`load_new_orders_to_snowflake`** - runs a `COPY INTO` that loads only the newly uploaded file into `softcart_db.raw.orders`, using the filename passed via Airflow XCom.
3. **`run_dbt_build`** - triggers `dbt build` (all models + all tests) in a dedicated, isolated dbt container, refreshing the staging and marts layers with the new data.

### Why a separate dbt container?
`dbt-snowflake` and Airflow's own Snowflake provider have conflicting dependency requirements (specifically around `snowflake-connector-python`) that make them impossible to install in the same Python environment. Rather than fight that, dbt runs in its own lightweight container (`python:3.12-slim` + dbt installed at startup), and Airflow triggers it via `docker exec`, using a Docker-socket mount for cross-container control.

## Continuous Integration

A GitHub Actions workflow (`.github/workflows/dbt-ci.yml`) runs on every push and PR to `main`, with two jobs:

- **`dbt-compile`** - installs `dbt-snowflake`, builds a profile from repo secrets, and runs `dbt compile` to catch model/reference errors before merge.
- **`dag-validate`** - checks DAG Python syntax, installs a pinned Airflow + provider set matching the production container (`apache-airflow==2.10.4`, `apache-airflow-providers-snowflake==5.8.1`), and loads the DAG via `DagBag` to catch import errors.

Provider versions in this job are intentionally pinned to match what's installed in the production Airflow container, to avoid false-positive failures from upstream provider version changes (e.g. operator renames).

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
- CI via GitHub Actions (dbt compile + DAG validation on push/PR to main)
- dbt docs site generation, auto-deployed to GitHub Pages: https://abobakar-a.github.io/softcart-data-pipeline/
- Incremental materialization for `fct_orders`
- SCD Type 2 change tracking for `dim_customers` and `dim_products` via dbt snapshots
- Clickstream event tracking (JSON ingestion, staging, incremental marts fact table)
- Version controlled on GitHub

Not yet implemented (roadmap):
- BI/dashboard layer on top of the marts schema
- Failure alerting (email/Slack)
