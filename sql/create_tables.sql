-- ============================================================
-- Olist ETL Pipeline - Warehouse DDL
-- Target: your LOCAL PostgreSQL instance (NOT Airflow's own
-- metadata DB, and NOT a Docker container - see docker-compose.yml,
-- which connects out to host.docker.internal:DB_PORT instead of
-- running its own warehouse Postgres container). Run this manually
-- with e.g. `psql -h localhost -U postgres -d warehouse_db -f
-- sql/create_tables.sql` if you want the schema pre-created.
--
-- Note: postgres_loader.py loads Gold tables with
-- if_exists="replace", so SQLAlchemy/pandas will (re)create
-- dim_*/fact_*/revenue_mart/kpi_summary tables automatically on
-- every run. This script is provided so you can:
--   1) Pre-create the warehouse DB schema explicitly for review
--      / grading purposes, and
--   2) Create pipeline_metadata up front, since metadata.py uses
--      if_exists="append" and expects the table (or an empty DB
--      that pandas can create the first time) to already make sense
--      with a stable schema.
-- Running main.py / the DAG does not require you to run this
-- file first - it's here for completeness and manual inspection.
-- ============================================================

-- ------------------------------------------------------------
-- Dimension: Customer (SCD Type 1 - current record only)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id         VARCHAR(64) PRIMARY KEY,
    customer_unique_id  VARCHAR(64),
    customer_city       VARCHAR(128),
    customer_state      VARCHAR(8)
);

-- ------------------------------------------------------------
-- Dimension: Customer History (SCD Type 2)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer_history (
    customer_id              VARCHAR(64),
    customer_unique_id       VARCHAR(64),
    customer_city            VARCHAR(128),
    customer_state           VARCHAR(8),
    order_purchase_timestamp TIMESTAMP,
    effective_start_date     TIMESTAMP,
    effective_end_date       TIMESTAMP,
    version                  INTEGER,
    is_current                BOOLEAN
);

-- ------------------------------------------------------------
-- Dimension: Product
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_id                     VARCHAR(64) PRIMARY KEY,
    product_category_name          VARCHAR(128),
    product_category_name_english  VARCHAR(128),
    product_weight_g               NUMERIC,
    product_length_cm              NUMERIC,
    product_height_cm              NUMERIC,
    product_width_cm               NUMERIC
);

-- ------------------------------------------------------------
-- Dimension: Seller
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id     VARCHAR(64) PRIMARY KEY,
    seller_city   VARCHAR(128),
    seller_state  VARCHAR(8)
);

-- ------------------------------------------------------------
-- Dimension: Geolocation
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_geolocation (
    geolocation_zip_code_prefix VARCHAR(16),
    geolocation_lat              NUMERIC,
    geolocation_lng              NUMERIC,
    geolocation_city             VARCHAR(128),
    geolocation_state            VARCHAR(8)
);

-- ------------------------------------------------------------
-- Dimension: Date
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date        TIMESTAMP PRIMARY KEY,
    order_year  INTEGER,
    order_month INTEGER,
    day         INTEGER,
    month_name  VARCHAR(16),
    quarter     INTEGER,
    year        INTEGER,
    weekday     VARCHAR(16)
);

-- ------------------------------------------------------------
-- Fact: Sales
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id                  VARCHAR(64),
    customer_id               VARCHAR(64),
    product_id                VARCHAR(64),
    seller_id                 VARCHAR(64),
    payment_type               VARCHAR(32),
    payment_value              NUMERIC,
    price                       NUMERIC,
    freight_value               NUMERIC,
    total_sale                  NUMERIC,
    review_score                 NUMERIC,
    order_purchase_timestamp    TIMESTAMP,
    delivery_days                 NUMERIC
);

-- ------------------------------------------------------------
-- Fact: Reviews
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_reviews (
    review_id                VARCHAR(64) PRIMARY KEY,
    order_id                 VARCHAR(64),
    review_score              NUMERIC,
    review_creation_date       TIMESTAMP,
    review_answer_timestamp    TIMESTAMP
);

-- ------------------------------------------------------------
-- Mart: Revenue by customer state
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS revenue_mart (
    customer_state       VARCHAR(8),
    total_orders          INTEGER,
    total_revenue          NUMERIC,
    average_order_value    NUMERIC
);

-- ------------------------------------------------------------
-- Mart: KPI Summary (single row per pipeline run, replaced each run)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kpi_summary (
    total_customers        INTEGER,
    total_orders             INTEGER,
    total_products             INTEGER,
    total_sellers                INTEGER,
    total_revenue                  NUMERIC,
    average_order_value             NUMERIC,
    average_review_score              NUMERIC
);

-- ------------------------------------------------------------
-- Pipeline execution metadata (one row per run, appended)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id                  SERIAL PRIMARY KEY,
    run_id                VARCHAR(64),
    pipeline_name           VARCHAR(128),
    start_time             TIMESTAMP,
    end_time                 TIMESTAMP,
    duration_seconds           NUMERIC,
    status                       VARCHAR(32),
    records_processed              INTEGER,
    records_rejected                 INTEGER,
    gold_tables_created                TEXT
);
