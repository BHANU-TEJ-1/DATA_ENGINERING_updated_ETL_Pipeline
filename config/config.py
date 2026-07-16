from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv(override=True)

# -------------------------------------------------------
# Pipeline
# -------------------------------------------------------

PIPELINE_NAME = os.getenv("PIPELINE_NAME")

# -------------------------------------------------------
# Project Root
# -------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

# -------------------------------------------------------
# Data Paths
# -------------------------------------------------------

BRONZE_PATH = BASE_DIR / os.getenv("BRONZE_PATH")

SILVER_PATH = BASE_DIR / os.getenv("SILVER_PATH")

GOLD_PATH = BASE_DIR / os.getenv("GOLD_PATH")

REJECTED_PATH = BASE_DIR / os.getenv("REJECTED_PATH")

# -------------------------------------------------------
# Source Dataset Paths
# -------------------------------------------------------

CUSTOMERS_PATH = os.getenv("CUSTOMERS_PATH")
GEOLOCATION_PATH = os.getenv("GEOLOCATION_PATH")

ORDERS_PATH = os.getenv("ORDERS_PATH")
ORDER_ITEMS_PATH = os.getenv("ORDER_ITEMS_PATH")

PRODUCTS_PATH = os.getenv("PRODUCTS_PATH")
PRODUCT_CATEGORY_TRANSLATION_PATH = os.getenv(
    "PRODUCT_CATEGORY_TRANSLATION_PATH"
)
# -------------------------------------------------------
# PostgreSQL Warehouse
# -------------------------------------------------------

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{DB_USER}:{DB_PASSWORD}@"
    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# -------------------------------------------------------
# MySQL Source Database
# -------------------------------------------------------

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

MYSQL_CONNECTION_STRING = (
    f"mysql+pymysql://"
    f"{MYSQL_USER}:{MYSQL_PASSWORD}@"
    f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

# -------------------------------------------------------
# Metadata
# -------------------------------------------------------

METADATA_TABLE = os.getenv("METADATA_TABLE")
AUDIT_TABLE = os.getenv("AUDIT_TABLE")

# -------------------------------------------------------
# Email
# -------------------------------------------------------

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT"))