# Olist ETL Pipeline (Medallion Architecture)

An end-to-end enterprise data-engineering PoC built on the Olist
Brazilian E-Commerce dataset, following Bronze → Silver → Gold,
orchestrated with Apache Airflow, loaded into PostgreSQL, and
reported on by email.

```
CSV + JSON + XML + MySQL → Extraction (parallel) → Bronze → Validation → Silver →
Transformation → SCD → Gold → PostgreSQL → Metadata → Email
```

## What's actually in this repo

Every file listed in the original spec now has real, tested logic:

| File | Responsibility |
|---|---|
| `pipeline/ingestion/*.py` | CSVIngestion / JSONIngestion / XMLIngestion / MySQLIngestion - one class per source format, each just extracts |
| `pipeline/extraction.py` | Maps each dataset to its source (file path or MySQL table) and orchestrates the four ingestion classes into one merged dict |
| `pipeline/bronze.py` | Adds lineage columns, writes immutable Parquet |
| `pipeline/validation.py` | Schema/PK/null/duplicate checks, rejects bad rows |
| `pipeline/silver.py` | Cleans strings/dates/numerics, writes Parquet |
| `pipeline/transformation.py` | Joins, derived columns, window functions, `fact_sales`/`revenue_mart`/`kpi_summary` |
| `pipeline/scd.py` | SCD Type 1 (`dim_customer`) and Type 2 (`dim_customer_history`) |
| `pipeline/gold.py` | Builds the star schema, writes Gold Parquet |
| `pipeline/postgres_loader.py` | Loads Gold tables into the warehouse DB |
| `pipeline/metadata.py` | One `pipeline_metadata` row per run |
| `pipeline/mail.py` | Success/failure email report |
| `main.py` | Runs the whole thing sequentially, handles failures |
| `dags/pipeline_DAG_.py` | Same pipeline as an Airflow DAG (10 tasks) |

I ran the full pandas portion (Extract → Gold) against your actual
CSVs in `data/raw/` while building this, so the numbers below are
real, not hypothetical:

- 99,441 customers, 99,441 orders, 118,763 `fact_sales` rows after joins
- 32,951 products, 3,095 sellers, 738,332 deduplicated geolocation rows
- Total revenue ≈ R$16.6M, average review score ≈ 4.02
- `dim_customer_history` correctly versions customers who ordered
  more than once, with proper `effective_end_date` / `is_current`

I could not run the PostgreSQL load, Airflow DAG, or email steps in
my environment (no Docker/Postgres/SMTP access there), so **please
run those yourself** with your own credentials — see below.

## Bugs I found and fixed in your original code

1. **`validation.py` was dropping almost all your data.** It ran
   `working_df.dropna()`, which deletes a row if *any* column is
   null. Since `review_comment_title`/`review_comment_message` are
   null for most real reviews, and delivery dates are null for
   any order not yet delivered, this would have silently destroyed
   most of your reviews and orders tables. Fixed to only check
   nulls on columns that actually matter per table (keys, amounts,
   core dates) — see `NULL_CHECK_COLUMNS` in `validation.py`.
2. **`silver.py` turned real missing values into the string `"nan"`**
   via `.astype(str)` on columns that still had `NaN`s. Fixed to
   only stringify non-null values.
3. **`transformation.py` didn't return what the spec asked for.**
   It only returned `sales`/`reviews`/`geolocation`/`revenue_mart`/
   `kpi_summary`; the spec requires `fact_sales` plus pass-through
   `customers`, `products`, `sellers`, `translation`, `orders`,
   `payments`, `order_items`. Fixed, and this also unblocked
   `gold.py`'s dimensions being built from the correct source
   tables instead of the fact table (so products/sellers with no
   sales aren't silently dropped from their dimensions).
4. **Bronze lineage columns (`pipeline_name`, `source_file`,
   `load_timestamp`) collided during joins.** Every table carries
   the same 3 metadata columns, so `pandas.merge` raised a
   `MergeError` the moment two tables were joined. Fixed by
   stripping them before joining.
5. **SCD Type 2 wasn't actually SCD Type 2.** Every version was
   marked `is_current=True` with `effective_end_date=NaT`. Fixed so
   `effective_end_date` is the next version's start date, and only
   a customer's latest version is `is_current=True`.
6. `main.py` and `dags/pipeline_DAG_.py` were empty; `sql/`,
   `docker-compose.yml`, `Dockerfile`, and `README.md` didn't exist.
   All added.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose) — easiest path
- OR Python 3.11+, a local PostgreSQL instance, and `pip install -r requirements.txt` if you'd rather run without Docker
- A Gmail (or other SMTP) account with an **app password** (not
  your normal password — Gmail requires this for SMTP)

## Setup

1. **Fill in your credentials.** Copy the example and edit it:
   ```bash
   cp .env.example .env
   ```
   Set at minimum: `DB_PASSWORD`, `EMAIL_SENDER`, `EMAIL_PASSWORD`
   (Gmail app password: Google Account → Security → 2-Step
   Verification → App passwords), `EMAIL_RECEIVER`.

2. **Raw data** is already in `data/raw/` (your 9 Olist CSVs) — nothing to do here.

## Option A — Run with Docker + Airflow (recommended, matches the spec)

This runs Airflow 3 (new React UI), with **your own LOCAL
PostgreSQL** as the warehouse - there is no warehouse database
container. Make sure, before starting:

- Your local PostgreSQL server is running and reachable on
  `DB_PORT`, and the `DB_NAME` database already exists.
- Your local MySQL server (`olist_db`, with `order_payments` /
  `order_reviews`) is running and reachable on `MYSQL_PORT`.
- `OLIST_SOURCE_DIR` in `.env` points at the folder containing
  `customers.csv`, `geolocation.csv`, `orders.json`,
  `order_items.json`, `products.xml`, `product_category_translation.xml`.
- `AIRFLOW_JWT_SECRET` in `.env` is set to some long random string.

```bash
docker compose up airflow-init      # first time only: creates admin/admin user
docker compose up -d                # starts airflow's own DB, api-server, scheduler, dag-processor
```

Open http://localhost:8080 (login `admin` / `admin`), find the
`olist_etl_pipeline` DAG, and trigger it manually (▶ button). Watch
the tasks run: extraction fans out into 4 parallel tasks
(`extract_csv`, `extract_json`, `extract_xml`, `extract_mysql`),
which join at `merge_extraction`, then continue
`bronze → validation → silver → transformation → scd → gold →
postgres_load → metadata → email` exactly as before.

Containers reach your local Postgres/MySQL via
`host.docker.internal` (built into Docker Desktop on Windows/Mac).

To stop everything: `docker compose down` (add `-v` to also wipe
Airflow's own metadata DB volume and start clean next time - this
does **not** touch your local Postgres warehouse data).

## Option B — Run locally without Docker/Airflow

```bash
pip install -r requirements.txt
# Make sure a local PostgreSQL server is running and DB_HOST=localhost
# in .env points at it (create the warehouse_db database first).
python main.py
```

This runs the exact same pipeline as the DAG, sequentially, in one
process — good for quick iteration/debugging before wiring up
Airflow.

## Where things land

- `data/bronze/*.parquet` — raw + lineage metadata
- `data/silver/*.parquet` — cleaned
- `data/gold/*.parquet` — star schema (10 tables)
- `data/rejected/*.csv` — rows that failed validation, with the reason in the filename
- `logs/pipeline.log` — every module logs here via `get_logger(__name__)`
- Warehouse Postgres — same 10 Gold tables + `pipeline_metadata`

## Troubleshooting

- **Email fails with an auth error**: you're almost certainly using
  your normal Gmail password instead of an app password, or 2FA
  isn't enabled (required for app passwords).
- **`psycopg2` / connection refused**: check `DB_HOST` — it should
  be `host.docker.internal` when running via Docker Compose
  (docker-compose.yml sets this for you automatically), or
  `localhost` when running `main.py` directly against your local
  Postgres. Same idea for `MYSQL_HOST`. Also make sure your local
  Postgres/MySQL are configured to accept connections from Docker's
  network, not just `127.0.0.1`.
- **`ModuleNotFoundError: No module named 'pymysql'`**: rebuild the
  image (`docker compose build`) — `pymysql` is required for
  `MySQLIngestion` and lives in `requirements.txt`.
- **DAG task fails and you get a failure email**: check
  `logs/pipeline.log` first, then the task's log in the Airflow UI —
  both show the same underlying exception.
- **Airflow can't find `pipeline`/`config` modules**: this is
  handled by `sys.path.insert(...)` at the top of
  `dags/pipeline_DAG_.py`, but if you move the project structure
  around, that path logic will need to move with it.
