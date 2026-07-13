# Olist ETL Pipeline (Medallion Architecture)

An end-to-end enterprise data-engineering PoC built on the Olist
Brazilian E-Commerce dataset, following Bronze → Silver → Gold,
orchestrated with Apache Airflow, loaded into PostgreSQL, and
reported on by email.

```
Raw CSV → Extraction → Bronze → Validation → Silver →
Transformation → SCD → Gold → PostgreSQL → Metadata → Email
```

## What's actually in this repo

Every file listed in the original spec now has real, tested logic:

| File | Responsibility |
|---|---|
| `pipeline/extraction.py` | Reads every raw CSV into a dict of DataFrames |
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

```bash
docker compose up airflow-init      # first time only: creates admin/admin user
docker compose up -d                # starts warehouse DB, airflow DB, webserver, scheduler
```

Open http://localhost:8080 (login `admin` / `admin`), find the
`olist_etl_pipeline` DAG, and trigger it manually (▶ button). Watch
the 10 tasks run in order: `extract → bronze → validation → silver
→ transformation → scd → gold → postgres_load → metadata → email`.

The **warehouse database** (separate from Airflow's own metadata
DB, per spec) is exposed on `localhost:5433` if you want to inspect
it with a client:
```
host: localhost   port: 5433
db:   warehouse_db (or your WAREHOUSE_DB_NAME)
user: warehouse    (or your WAREHOUSE_DB_USER)
```

To stop everything: `docker compose down` (add `-v` to also wipe
the Postgres volumes and start clean next time).

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
  be `warehouse-db` when running via Docker Compose, or `localhost`
  when running `main.py` directly against a local Postgres.
- **DAG task fails and you get a failure email**: check
  `logs/pipeline.log` first, then the task's log in the Airflow UI —
  both show the same underlying exception.
- **Airflow can't find `pipeline`/`config` modules**: this is
  handled by `sys.path.insert(...)` at the top of
  `dags/pipeline_DAG_.py`, but if you move the project structure
  around, that path logic will need to move with it.
