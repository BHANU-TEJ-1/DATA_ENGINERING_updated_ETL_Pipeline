"""
Olist ETL Pipeline - Airflow DAG.

Extraction is multi-source and runs in PARALLEL, then the rest of
the pipeline mirrors main.py's execution order exactly:

start_extraction -> [extract_csv, extract_json, extract_xml,
extract_mysql] (parallel) -> merge_extraction -> Bronze ->
Validation -> Silver -> Transformation -> SCD -> Gold ->
PostgreSQL -> Metadata -> Email

Each task is a separate process, so DataFrame dicts CANNOT be
passed between tasks via XCom (Airflow's default XCom backend is
JSON-only and not meant for large data). Instead, each task hands
its output to the next by pickling the dict of DataFrames to a
scratch folder (data/tmp/) - the same pattern the pipeline already
uses for the Bronze/Silver/Gold parquet layers, just for the
in-between stages that don't have a named layer of their own. The
four extraction tasks each write their own pickle (csv.pkl,
json.pkl, xml.pkl, mysql.pkl); merge_extraction combines them into
raw.pkl, which Bronze reads exactly as before.
Only small values (row counts, table names, run_id) travel through
XCom.
"""

import pickle
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make `pipeline` / `config` importable the same way main.py expects,
# regardless of where Airflow's DAG folder happens to be mounted.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import MYSQL_CONNECTION_STRING
from pipeline.extraction import (
    get_csv_paths,
    get_json_paths,
    get_xml_paths,
    get_mysql_tables,
)
from pipeline.ingestion.csv_ingestion import CSVIngestion
from pipeline.ingestion.json_ingestion import JSONIngestion
from pipeline.ingestion.xml_ingestion import XMLIngestion
from pipeline.ingestion.mysql_ingestion import MySQLIngestion
from pipeline.bronze import create_bronze_layer
from pipeline.validation import validate_data
from pipeline.silver import create_silver_layer
from pipeline.transformation import create_business_layer
from pipeline.scd import apply_scd
from pipeline.gold import create_gold_layer
from pipeline.postgres_loader import load_to_postgres
from pipeline.metadata import save_metadata
from pipeline.mail import send_email
from pipeline.logger import get_logger

logger = get_logger(__name__)

TMP_PATH = PROJECT_ROOT / "data" / "tmp"


# ----------------------------------------------------------------
# Disk-based hand-off helpers between tasks
# ----------------------------------------------------------------

def _save_stage(name: str, data: dict) -> None:
    TMP_PATH.mkdir(parents=True, exist_ok=True)
    with open(TMP_PATH / f"{name}.pkl", "wb") as f:
        pickle.dump(data, f)


def _load_stage(name: str) -> dict:
    with open(TMP_PATH / f"{name}.pkl", "rb") as f:
        return pickle.load(f)


def _clear_rejected_folder() -> None:
    from config.config import REJECTED_PATH

    REJECTED_PATH.mkdir(parents=True, exist_ok=True)
    for file in REJECTED_PATH.glob("*.csv"):
        file.unlink()


def _count_rejected() -> int:
    from config.config import REJECTED_PATH
    import pandas as pd

    if not REJECTED_PATH.exists():
        return 0

    total = 0
    for file in REJECTED_PATH.glob("*.csv"):
        try:
            total += len(pd.read_csv(file))
        except Exception:
            pass
    return total


# ----------------------------------------------------------------
# Task callables
# ----------------------------------------------------------------

def start_extraction_task(**context) -> None:
    """
    Runs once, before the four parallel extraction tasks: generates
    the run_id, records the pipeline start time, and clears out the
    rejected-records folder from any previous run.
    """
    run_id = str(uuid.uuid4())
    context["ti"].xcom_push(key="run_id", value=run_id)
    context["ti"].xcom_push(
        key="pipeline_start_time", value=datetime.now().isoformat()
    )

    _clear_rejected_folder()


def extract_csv_task(**context) -> None:
    datasets = CSVIngestion().extract(get_csv_paths())
    _save_stage("csv", datasets)


def extract_json_task(**context) -> None:
    datasets = JSONIngestion().extract(get_json_paths())
    _save_stage("json", datasets)


def extract_xml_task(**context) -> None:
    datasets = XMLIngestion().extract(get_xml_paths())
    _save_stage("xml", datasets)


def extract_mysql_task(**context) -> None:
    mysql_tables = get_mysql_tables()

    raw = MySQLIngestion().extract(
        MYSQL_CONNECTION_STRING, list(mysql_tables.keys())
    )

    datasets = {
        canonical_name: raw[table_name]
        for table_name, canonical_name in mysql_tables.items()
    }
    _save_stage("mysql", datasets)


def merge_extraction_task(**context) -> None:
    """
    Runs after all four extraction tasks complete. Merges the four
    pickled dicts into one and saves it as raw.pkl, exactly what
    Bronze expects - the rest of the pipeline is unchanged.
    """
    raw_datasets: dict = {}

    for stage_name in ("csv", "json", "xml", "mysql"):
        raw_datasets.update(_load_stage(stage_name))

    _save_stage("raw", raw_datasets)


def bronze_task(**context) -> None:
    raw_datasets = _load_stage("raw")
    bronze_datasets = create_bronze_layer(raw_datasets)
    _save_stage("bronze", bronze_datasets)


def validation_task(**context) -> None:
    bronze_datasets = _load_stage("bronze")
    validated_datasets = validate_data(bronze_datasets)
    _save_stage("validated", validated_datasets)


def silver_task(**context) -> None:
    validated_datasets = _load_stage("validated")
    silver_datasets = create_silver_layer(validated_datasets)
    _save_stage("silver", silver_datasets)


def transformation_task(**context) -> None:
    silver_datasets = _load_stage("silver")
    transformed_data = create_business_layer(silver_datasets)
    _save_stage("transformed", transformed_data)


def scd_task(**context) -> None:
    transformed_data = _load_stage("transformed")
    transformed_data = apply_scd(transformed_data)
    _save_stage("scd", transformed_data)


def gold_task(**context) -> None:
    transformed_data = _load_stage("scd")
    gold_tables = create_gold_layer(transformed_data)
    _save_stage("gold", gold_tables)

    context["ti"].xcom_push(
        key="gold_table_names", value=list(gold_tables.keys())
    )
    context["ti"].xcom_push(
        key="records_processed",
        value=sum(len(df) for df in gold_tables.values()),
    )


def postgres_task(**context) -> None:
    gold_tables = _load_stage("gold")
    load_to_postgres(gold_tables)
    context["ti"].xcom_push(key="database_status", value="Successful")


def metadata_task(**context) -> None:
    ti = context["ti"]

    run_id = ti.xcom_pull(key="run_id", task_ids="start_extraction")
    start_time = datetime.fromisoformat(
        ti.xcom_pull(key="pipeline_start_time", task_ids="start_extraction")
    )
    records_processed = ti.xcom_pull(
        key="records_processed", task_ids="gold"
    ) or 0
    gold_table_names = ti.xcom_pull(
        key="gold_table_names", task_ids="gold"
    ) or []

    records_rejected = _count_rejected()
    end_time = datetime.now()

    save_metadata(
        run_id=run_id,
        start_time=start_time,
        end_time=end_time,
        records_processed=records_processed,
        records_rejected=records_rejected,
        status="SUCCESS",
        gold_tables_created=gold_table_names,
    )

    ti.xcom_push(key="metadata_status", value="Successful")
    ti.xcom_push(key="records_rejected", value=records_rejected)
    ti.xcom_push(key="pipeline_end_time", value=end_time.isoformat())


def email_task(**context) -> None:
    ti = context["ti"]

    run_id = ti.xcom_pull(key="run_id", task_ids="start_extraction")
    start_time = datetime.fromisoformat(
        ti.xcom_pull(key="pipeline_start_time", task_ids="start_extraction")
    )
    end_time = datetime.fromisoformat(
        ti.xcom_pull(key="pipeline_end_time", task_ids="metadata")
    )
    records_processed = ti.xcom_pull(
        key="records_processed", task_ids="gold"
    ) or 0
    records_rejected = ti.xcom_pull(
        key="records_rejected", task_ids="metadata"
    ) or 0
    gold_table_names = ti.xcom_pull(
        key="gold_table_names", task_ids="gold"
    ) or []
    database_status = ti.xcom_pull(
        key="database_status", task_ids="postgres_load"
    ) or "Unknown"
    metadata_status = ti.xcom_pull(
        key="metadata_status", task_ids="metadata"
    ) or "Unknown"

    send_email(
        start_time=start_time,
        end_time=end_time,
        records_processed=records_processed,
        records_rejected=records_rejected,
        status="SUCCESS",
        run_id=run_id,
        gold_tables_created=gold_table_names,
        database_status=database_status,
        metadata_status=metadata_status,
    )


# ----------------------------------------------------------------
# Failure handling - saves failure metadata + sends a failure email
# whenever ANY task in the DAG fails, mirroring main.py's
# except-block behaviour.
# ----------------------------------------------------------------

def on_pipeline_failure(context) -> None:
    ti = context["ti"]
    dag_run = context["dag_run"]

    try:
        run_id = ti.xcom_pull(key="run_id", task_ids="start_extraction") or "unknown"
        start_time_raw = ti.xcom_pull(
            key="pipeline_start_time", task_ids="start_extraction"
        )
        start_time = (
            datetime.fromisoformat(start_time_raw)
            if start_time_raw
            else dag_run.start_date
        )
    except Exception:
        run_id = "unknown"
        start_time = dag_run.start_date

    end_time = datetime.now()
    records_rejected = _count_rejected()
    error = context.get("exception", "Unknown error")

    try:
        save_metadata(
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            records_processed=0,
            records_rejected=records_rejected,
            status="FAILED",
            gold_tables_created=[],
        )
    except Exception as e:
        logger.error(f"Failed to save failure metadata: {e}")

    try:
        send_email(
            start_time=start_time,
            end_time=end_time,
            records_processed=0,
            records_rejected=records_rejected,
            status=f"FAILED - {error}",
            run_id=run_id,
            gold_tables_created=[],
            database_status="Failed / Not Reached",
            metadata_status="Attempted",
        )
    except Exception as e:
        logger.error(f"Failed to send failure email: {e}")


# ----------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_pipeline_failure,
}

with DAG(
    dag_id="olist_etl_pipeline",
    description="End-to-end Olist e-commerce ETL: Bronze -> Silver -> Gold -> PostgreSQL",
    default_args=default_args,
    schedule=None,  # trigger manually, or set e.g. "@daily"
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["olist", "etl", "medallion"],
) as dag:

    start_extraction = PythonOperator(
        task_id="start_extraction",
        python_callable=start_extraction_task,
    )

    extract_csv = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv_task,
    )

    extract_json = PythonOperator(
        task_id="extract_json",
        python_callable=extract_json_task,
    )

    extract_xml = PythonOperator(
        task_id="extract_xml",
        python_callable=extract_xml_task,
    )

    extract_mysql = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_mysql_task,
    )

    merge_extraction = PythonOperator(
        task_id="merge_extraction",
        python_callable=merge_extraction_task,
    )

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=bronze_task,
    )

    validation = PythonOperator(
        task_id="validation",
        python_callable=validation_task,
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=silver_task,
    )

    transformation = PythonOperator(
        task_id="transformation",
        python_callable=transformation_task,
    )

    scd = PythonOperator(
        task_id="scd",
        python_callable=scd_task,
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=gold_task,
    )

    postgres_load = PythonOperator(
        task_id="postgres_load",
        python_callable=postgres_task,
    )

    metadata = PythonOperator(
        task_id="metadata",
        python_callable=metadata_task,
    )

    email = PythonOperator(
        task_id="email",
        python_callable=email_task,
    )

    start_extraction >> [extract_csv, extract_json, extract_xml, extract_mysql]

    (
        [extract_csv, extract_json, extract_xml, extract_mysql]
        >> merge_extraction
        >> bronze
        >> validation
        >> silver
        >> transformation
        >> scd
        >> gold
        >> postgres_load
        >> metadata
        >> email
    )
