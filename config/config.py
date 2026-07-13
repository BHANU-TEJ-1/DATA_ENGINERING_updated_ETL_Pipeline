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

RAW_DATA_PATH = BASE_DIR / os.getenv("RAW_DATA_PATH")

BRONZE_PATH = BASE_DIR / os.getenv("BRONZE_PATH")

SILVER_PATH = BASE_DIR / os.getenv("SILVER_PATH")

GOLD_PATH = BASE_DIR / os.getenv("GOLD_PATH")

REJECTED_PATH = BASE_DIR / os.getenv("REJECTED_PATH")

# -------------------------------------------------------
# Database
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
print("\n========== DATABASE CONFIG ==========")
print(f"HOST     : {DB_HOST}")
print(f"PORT     : {DB_PORT}")
print(f"DATABASE : {DB_NAME}")
print(f"USER     : {DB_USER}")
print(f"PASSWORD : {repr(DB_PASSWORD)}")
print(f"URL      : {DATABASE_URL}")
print("=====================================\n")

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