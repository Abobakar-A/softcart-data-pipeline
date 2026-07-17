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

## Idempotency

Re-running the pipeline for the same day (e.g. after a failure and retry) should not create duplicate data. This required two fixes:

### Deterministic IDs

`generate_and_upload_orders` and `generate_and_upload_clickstream` originally generated IDs based on the exact execution timestamp (`datetime.now()`), meaning every run - including accidental re-runs for the same day - produced entirely different, non-overlapping IDs. Both functions now derive their IDs from the DAG's logical date (`context["ds_nodash"]`) instead, so re-running the pipeline for a given day regenerates the same set of IDs rather than a new one.

### Deduplication at the staging layer

Deterministic IDs alone were not sufficient: `COPY INTO` appends file contents into the raw table unconditionally, so loading the same file (or a file with the same IDs) twice results in duplicate rows in `raw.orders`. This was confirmed directly: running the DAG twice for the same day caused `unique_stg_orders_order_id` to fail with 50 duplicate rows, correctly blocking `fct_orders` from rebuilding on top of bad data.

The fix: `stg_orders` now deduplicates by `order_id` using `qualify row_number() over (partition by order_id order by order_date desc) = 1`, keeping only one row per order regardless of how many times the same ID was loaded upstream. This was verified by re-running the full `dbt build` after the fix: all 28 checks passed, and `fct_orders` correctly processed 0 new rows on the redundant run.

`fct_clickstream_events` did not require the same staging-layer fix, since its incremental `merge` on `unique_key='event_id'` handled the redundant load correctly without additional deduplication.

## Failure Alerting

The pipeline sends a Slack notification automatically whenever any task fails, so failures are visible immediately rather than requiring someone to check the Airflow UI.

### How it works

- `slack_failure_alert` (in the DAG) posts a formatted message to a Slack Incoming Webhook, including the failing DAG, task, run date, and a direct link to the task's logs.
- It's wired in once via `default_args["on_failure_callback"]`, so it automatically applies to every task in the DAG without needing to be added individually.
- The webhook URL is stored as an Airflow Variable (`slack_webhook_url`), not hardcoded in the DAG file.
- Airflow's default retry behavior (`retries: 1`) means the alert fires only after all retries are exhausted, not on the first failed attempt - avoiding noisy alerts for transient issues that self-resolve on retry.

### Setup

    docker exec -it airflow-airflow-scheduler-1 airflow variables set slack_webhook_url "<your Slack Incoming Webhook URL>"

## BI Dashboard

An interactive Power BI dashboard was built on top of the marts layer, connected via Microsoft Fabric.

**Live dashboard:** https://app.fabric.microsoft.com/links/GnSe8WPV9j?ctid=d9c25066-ba78-4b0f-8401-6b35fd17bfc9&pbi_source=linkShare

![Softcart Dashboard](docs/images/dashboard.png)

### What it shows

- **KPI summary** - Total Revenue, Total Orders, Total Customers
- **Revenue Trend** - daily revenue over time
- **Top Products by Revenue** - highest-earning products
- **Customer Conversion Funnel** - page_view -> product_view -> add_to_cart -> purchase, built from clickstream data

### How it was built

- Snowflake `marts` tables were loaded into a Fabric Lakehouse via a Dataflow Gen2 (using a Power Query Blank Query with `Snowflake.Databases(...)`, to work around a known bug in Fabric's guided Snowflake connector UI)
- A semantic model defines relationships between `fct_orders`, `fct_clickstream_events`, `dim_customers`, and `dim_products` - a galaxy schema (two fact tables sharing the same dimensions)
- DAX measures (`Total Revenue`, `Total Orders`, `Total Customers`) back the KPI cards and charts

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
- Idempotent pipeline runs (deterministic IDs + staging-layer deduplication), verified by re-running the DAG twice for the same day
- Automated Slack failure alerting for all DAG tasks
- Interactive BI dashboard (Power BI via Fabric): KPIs, revenue trend, top products, clickstream conversion funnel
- Version controlled on GitHub

## Configuration

This project needs credentials for Snowflake, Azure Data Lake, and Slack. None of these are committed to the repo.

1. Copy `.env.example` to `.env` and fill in your values.
2. Create `airflow/dbt_profile/profiles.yml` with your Snowflake connection (see `.env.example` for the fields needed).
3. Set up an Airflow connection named `azure_data_lake_default` with your storage connection string.
4. Set an Airflow Variable named `slack_webhook_url` with your Slack webhook URL.

`.env`, `airflow/dbt_profile/`, and other credential files are excluded via `.gitignore`.
## Automated Test Alerts

After every pipeline run, a Slack notification is sent summarizing the dbt test results — no need to check Airflow logs manually.

Example message:
**How it works:**
1. `dbt build` runs all models, snapshots, and data quality tests.
2. dbt writes the results to `target/run_results.json`.
3. An Airflow task (`slack_test_summary`) reads that file, counts passed/failed tests, and posts a summary to Slack via the same webhook used for failure alerts.
4. This task runs regardless of whether the pipeline succeeded or failed (`trigger_rule="all_done"`), so you always get a status update.
