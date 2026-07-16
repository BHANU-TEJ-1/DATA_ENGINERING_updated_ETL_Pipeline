"""
Multi-Source Extraction Orchestrator.

The old CSV-only extraction.py has been removed. Extraction now goes
through four dedicated ingestion classes (pipeline/ingestion/):

    CSVIngestion   -> customers, geolocation, sellers
    JSONIngestion  -> orders, order_items
    XMLIngestion   -> products, product_category_translation
    MySQLIngestion -> order_payments, order_reviews (olist_db)

This module does two things:

1. Defines *where* each dataset comes from (file paths / table
   names), built from the existing config.py variables, and maps
   each source to the canonical dataset key the Bronze/Silver/Gold
   layers already expect (e.g. "olist_customers_dataset"). Keeping
   this mapping in one place means the Airflow DAG (which runs the
   four ingestion classes in PARALLEL tasks) and main.py (which runs
   them sequentially for local/manual runs) stay in sync.

2. Exposes `extract_data()`, a sequential convenience wrapper that
   calls all four ingestion classes and merges their output - this
   is what main.py imports, so main.py did not need to change.
"""

import pandas as pd

from config.config import (
    CUSTOMERS_PATH,
    GEOLOCATION_PATH,
    SELLERS_PATH,
    ORDERS_PATH,
    ORDER_ITEMS_PATH,
    PRODUCTS_PATH,
    PRODUCT_CATEGORY_TRANSLATION_PATH,
    MYSQL_CONNECTION_STRING,
)
from pipeline.ingestion.csv_ingestion import CSVIngestion
from pipeline.ingestion.json_ingestion import JSONIngestion
from pipeline.ingestion.xml_ingestion import XMLIngestion
from pipeline.ingestion.mysql_ingestion import MySQLIngestion
from pipeline.logger import get_logger

logger = get_logger(__name__)

# ------------------------------------------------------------------
# sellers was not part of the customers/geolocation CSV hand-off
# from the source system (only customers + geolocation were moved
# to CSV per the project spec). It stays on disk as a local CSV
# under data/raw, exactly as it already was, since the downstream
# Silver/Gold layers still require "olist_sellers_dataset".
# ------------------------------------------------------------------


def get_csv_paths() -> dict[str, str]:
    return {
        "olist_customers_dataset": CUSTOMERS_PATH,
        "olist_geolocation_dataset": GEOLOCATION_PATH,
        "olist_sellers_dataset": SELLERS_PATH,
    }


def get_json_paths() -> dict[str, str]:
    return {
        "olist_orders_dataset": ORDERS_PATH,
        "olist_order_items_dataset": ORDER_ITEMS_PATH,
    }


def get_xml_paths() -> dict[str, str]:
    return {
        "olist_products_dataset": PRODUCTS_PATH,
        "product_category_name_translation": PRODUCT_CATEGORY_TRANSLATION_PATH,
    }


def get_mysql_tables() -> dict[str, str]:
    """
    Maps MySQL table name -> canonical dataset key expected by the
    downstream Bronze/Silver/Gold layers.
    """
    return {
        "order_payments": "olist_order_payments_dataset",
        "order_reviews": "olist_order_reviews_dataset",
    }


def extract_data() -> dict[str, pd.DataFrame]:
    """
    Runs all four ingestion classes (sequentially) and merges their
    output into a single dict, keyed by the canonical dataset names
    the Bronze/Silver/Gold layers already expect.

    Used for local/manual runs (`python main.py`). The Airflow DAG
    (dags/pipeline_DAG_.py) runs the same four ingestion classes as
    separate, PARALLEL tasks instead, using the *_paths()/*_tables()
    helpers above, then merges them in its own merge_extraction task.
    """

    logger.info("Starting multi-source data extraction...")

    datasets: dict[str, pd.DataFrame] = {}

    datasets.update(CSVIngestion().extract(get_csv_paths()))
    datasets.update(JSONIngestion().extract(get_json_paths()))
    datasets.update(XMLIngestion().extract(get_xml_paths()))

    mysql_tables = get_mysql_tables()
    mysql_raw = MySQLIngestion().extract(
        MYSQL_CONNECTION_STRING, list(mysql_tables.keys())
    )
    for table_name, canonical_name in mysql_tables.items():
        datasets[canonical_name] = mysql_raw[table_name]

    logger.info(
        f"Multi-source extraction completed. "
        f"{len(datasets)} datasets extracted."
    )

    return datasets
