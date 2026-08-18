"""
Olist ETL Pipeline - Airflow 3.x

Execution flow:

START
  |
  +--> extract_csv ----+
  |                    |
  +--> extract_json ---+
  |                    |
  +--> extract_xml ----+--> merge_extraction
  |                    |
  +--> extract_mysql --+
                         |
                         v
                      Bronze
                         |
                         v
                     Validation
                         |
                         v
                       Silver
                         |
                         v
                  Transformation
                         |
                         v
                        SCD
                         |
                         v
                       Gold
                         |
                         v
                  PostgreSQL Load
                         |
                         v
                     Metadata
                         |
                         v
                       Email

Large DataFrames are NOT passed through XCom.

Each extraction task writes its output to:

    data/tmp/csv.pkl
    data/tmp/json.pkl
    data/tmp/xml.pkl
    data/tmp/mysql.pkl

merge_extraction combines them into:

    data/tmp/raw.pkl

The remaining pipeline continues using the same disk-based
handoff mechanism.

XCom is used only for small metadata values such as:

    run_id
    pipeline_start_time
    records_processed
    gold_table_names
    database_status
    metadata_status
    records_rejected
    pipeline_end_time
"""

import pickle
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task, get_current_context


# ============================================================
# PROJECT PATH
# ============================================================

# DAG file:
#
# project/
# ├── dags/
# │   └── olist_etl_dag.py
# ├── pipeline/
# ├── config/
# └── data/
#
# Therefore:
#
# __file__ -> project/dags/olist_etl_dag.py
# parent    -> project/dags
# parent    -> project

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Make pipeline/ and config/ importable
sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# PROJECT IMPORTS
# ============================================================

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


# ============================================================
# LOGGER
# ============================================================

logger = get_logger(__name__)


# ============================================================
# TEMPORARY TASK HANDOFF LOCATION
# ============================================================

TMP_PATH = PROJECT_ROOT / "data" / "tmp"


# ============================================================
# DISK-BASED HANDOFF HELPERS
# ============================================================

def _save_stage(name: str, data: dict) -> None:
    """
    Save a dictionary of DataFrames to the temporary handoff area.
    """

    TMP_PATH.mkdir(parents=True, exist_ok=True)

    with open(TMP_PATH / f"{name}.pkl", "wb") as f:
        pickle.dump(data, f)


def _load_stage(name: str) -> dict:
    """
    Load a dictionary of DataFrames from the temporary handoff area.
    """

    with open(TMP_PATH / f"{name}.pkl", "rb") as f:
        return pickle.load(f)


def _clear_rejected_folder() -> None:
    """
    Remove rejected CSV files from the previous pipeline run.
    """

    from config.config import REJECTED_PATH

    REJECTED_PATH.mkdir(parents=True, exist_ok=True)

    for file in REJECTED_PATH.glob("*.csv"):
        file.unlink()


def _count_rejected() -> int:
    """
    Count records written to rejected CSV files.
    """

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


# ============================================================
# FAILURE CALLBACK
# ============================================================

def on_pipeline_failure(context) -> None:
    """
    Runs when a task fails.

    Attempts to:
        1. Determine the pipeline run ID.
        2. Determine pipeline start time.
        3. Save FAILED metadata.
        4. Send a failure email.

    Failure handling itself is protected so that a failure in
    logging/email does not hide the original pipeline failure.
    """

    ti = context["ti"]
    dag_run = context["dag_run"]

    try:

        run_id = (
            ti.xcom_pull(
                key="run_id",
                task_ids="start_extraction",
            )
            or "unknown"
        )

        start_time_raw = ti.xcom_pull(
            key="pipeline_start_time",
            task_ids="start_extraction",
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

    error = context.get(
        "exception",
        "Unknown error",
    )

    # --------------------------------------------------------
    # Save failure metadata
    # --------------------------------------------------------

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

        logger.error(
            f"Failed to save failure metadata: {e}"
        )

    # --------------------------------------------------------
    # Send failure email
    # --------------------------------------------------------

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

        logger.error(
            f"Failed to send failure email: {e}"
        )


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="olist_etl_pipeline",

    description=(
        "End-to-end Olist e-commerce ETL: "
        "Bronze -> Silver -> Gold -> PostgreSQL"
    ),

    schedule=None,

    start_date=datetime(2026, 1, 1),

    catchup=False,

    tags=[
        "olist",
        "etl",
        "medallion",
    ],

    default_args={
        "owner": "data-engineering",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_pipeline_failure,
    },
)
def olist_etl_pipeline():

    # ========================================================
    # START EXTRACTION
    # ========================================================

    @task(
        task_id="start_extraction"
    )
    def start_extraction():

        """
        Initializes a pipeline run.

        Responsibilities:
            - Generate application-level run ID.
            - Record pipeline start time.
            - Clear rejected records from previous run.
        """

        context = get_current_context()

        ti = context["ti"]

        run_id = str(uuid.uuid4())

        ti.xcom_push(
            key="run_id",
            value=run_id,
        )

        ti.xcom_push(
            key="pipeline_start_time",
            value=datetime.now().isoformat(),
        )

        _clear_rejected_folder()


    # ========================================================
    # PARALLEL CSV EXTRACTION
    # ========================================================

    @task(
        task_id="extract_csv"
    )
    def extract_csv():

        datasets = CSVIngestion().extract(
            get_csv_paths()
        )

        _save_stage(
            "csv",
            datasets,
        )


    # ========================================================
    # PARALLEL JSON EXTRACTION
    # ========================================================

    @task(
        task_id="extract_json"
    )
    def extract_json():

        datasets = JSONIngestion().extract(
            get_json_paths()
        )

        _save_stage(
            "json",
            datasets,
        )


    # ========================================================
    # PARALLEL XML EXTRACTION
    # ========================================================

    @task(
        task_id="extract_xml"
    )
    def extract_xml():

        datasets = XMLIngestion().extract(
            get_xml_paths()
        )

        _save_stage(
            "xml",
            datasets,
        )


    # ========================================================
    # PARALLEL MYSQL EXTRACTION
    # ========================================================

    @task(
        task_id="extract_mysql"
    )
    def extract_mysql():

        mysql_tables = get_mysql_tables()

        raw = MySQLIngestion().extract(
            MYSQL_CONNECTION_STRING,
            list(mysql_tables.keys()),
        )

        datasets = {
            canonical_name: raw[table_name]
            for table_name, canonical_name
            in mysql_tables.items()
        }

        _save_stage(
            "mysql",
            datasets,
        )


    # ========================================================
    # MERGE EXTRACTION
    # ========================================================

    @task(
        task_id="merge_extraction"
    )
    def merge_extraction():

        """
        Waits for all extraction tasks.

        Combines:

            csv.pkl
            json.pkl
            xml.pkl
            mysql.pkl

        into:

            raw.pkl
        """

        raw_datasets: dict = {}

        for stage_name in (
            "csv",
            "json",
            "xml",
            "mysql",
        ):

            raw_datasets.update(
                _load_stage(stage_name)
            )

        _save_stage(
            "raw",
            raw_datasets,
        )


    # ========================================================
    # BRONZE
    # ========================================================

    @task(
        task_id="bronze"
    )
    def bronze():

        raw_datasets = _load_stage(
            "raw"
        )

        bronze_datasets = create_bronze_layer(
            raw_datasets
        )

        _save_stage(
            "bronze",
            bronze_datasets
        )


    # ========================================================
    # VALIDATION
    # ========================================================

    @task(
        task_id="validation"
    )
    def validation():

        bronze_datasets = _load_stage(
            "bronze"
        )

        validated_datasets = validate_data(
            bronze_datasets
        )

        _save_stage(
            "validated",
            validated_datasets
        )


    # ========================================================
    # SILVER
    # ========================================================

    @task(
        task_id="silver"
    )
    def silver():

        validated_datasets = _load_stage(
            "validated"
        )

        silver_datasets = create_silver_layer(
            validated_datasets
        )

        _save_stage(
            "silver",
            silver_datasets
        )


    # ========================================================
    # BUSINESS TRANSFORMATION
    # ========================================================

    @task(
        task_id="transformation"
    )
    def transformation():

        silver_datasets = _load_stage(
            "silver"
        )

        transformed_data = create_business_layer(
            silver_datasets
        )

        _save_stage(
            "transformed",
            transformed_data
        )


    # ========================================================
    # SCD
    # ========================================================

    @task(
        task_id="scd"
    )
    def scd():

        transformed_data = _load_stage(
            "transformed"
        )

        transformed_data = apply_scd(
            transformed_data
        )

        _save_stage(
            "scd",
            transformed_data
        )


    # ========================================================
    # GOLD
    # ========================================================

    @task(
        task_id="gold"
    )
    def gold():

        transformed_data = _load_stage(
            "scd"
        )

        gold_tables = create_gold_layer(
            transformed_data
        )

        _save_stage(
            "gold",
            gold_tables
        )

        context = get_current_context()

        ti = context["ti"]

        ti.xcom_push(
            key="gold_table_names",
            value=list(gold_tables.keys()),
        )

        ti.xcom_push(
            key="records_processed",
            value=sum(
                len(df)
                for df in gold_tables.values()
            ),
        )


    # ========================================================
    # POSTGRESQL LOAD
    # ========================================================

    @task(
        task_id="postgres_load"
    )
    def postgres_load():

        gold_tables = _load_stage(
            "gold"
        )

        load_to_postgres(
            gold_tables
        )

        context = get_current_context()

        ti = context["ti"]

        ti.xcom_push(
            key="database_status",
            value="Successful",
        )


    # ========================================================
    # METADATA
    # ========================================================

    @task(
        task_id="metadata"
    )
    def metadata():

        context = get_current_context()

        ti = context["ti"]

        run_id = ti.xcom_pull(
            key="run_id",
            task_ids="start_extraction",
        )

        start_time = datetime.fromisoformat(
            ti.xcom_pull(
                key="pipeline_start_time",
                task_ids="start_extraction",
            )
        )

        records_processed = ti.xcom_pull(
            key="records_processed",
            task_ids="gold",
        ) or 0

        gold_table_names = ti.xcom_pull(
            key="gold_table_names",
            task_ids="gold",
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

        ti.xcom_push(
            key="metadata_status",
            value="Successful",
        )

        ti.xcom_push(
            key="records_rejected",
            value=records_rejected,
        )

        ti.xcom_push(
            key="pipeline_end_time",
            value=end_time.isoformat(),
        )


    # ========================================================
    # EMAIL
    # ========================================================

    @task(
        task_id="email"
    )
    def email():

        context = get_current_context()

        ti = context["ti"]

        run_id = ti.xcom_pull(
            key="run_id",
            task_ids="start_extraction",
        )

        start_time = datetime.fromisoformat(
            ti.xcom_pull(
                key="pipeline_start_time",
                task_ids="start_extraction",
            )
        )

        end_time = datetime.fromisoformat(
            ti.xcom_pull(
                key="pipeline_end_time",
                task_ids="metadata",
            )
        )

        records_processed = ti.xcom_pull(
            key="records_processed",
            task_ids="gold",
        ) or 0

        records_rejected = ti.xcom_pull(
            key="records_rejected",
            task_ids="metadata",
        ) or 0

        gold_table_names = ti.xcom_pull(
            key="gold_table_names",
            task_ids="gold",
        ) or []

        database_status = ti.xcom_pull(
            key="database_status",
            task_ids="postgres_load",
        ) or "Unknown"

        metadata_status = ti.xcom_pull(
            key="metadata_status",
            task_ids="metadata",
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


    # ========================================================
    # CREATE TASK INSTANCES
    # ========================================================

    start = start_extraction()

    csv = extract_csv()
    json = extract_json()
    xml = extract_xml()
    mysql = extract_mysql()

    merge = merge_extraction()

    bronze_task = bronze()
    validation_task = validation()
    silver_task = silver()
    transformation_task = transformation()
    scd_task = scd()
    gold_task = gold()
    postgres_task = postgres_load()
    metadata_task = metadata()
    email_task = email()


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    # Start extraction first.
    #
    # Then the four independent extraction tasks
    # can execute in parallel.

    start >> [
        csv,
        json,
        xml,
        mysql,
    ]


    # All four extraction tasks must complete
    # before merge starts.

    [
        csv,
        json,
        xml,
        mysql,
    ] >> merge


    # After merging, the remainder of the pipeline
    # executes sequentially.

    merge >> bronze_task
    bronze_task >> validation_task
    validation_task >> silver_task
    silver_task >> transformation_task
    transformation_task >> scd_task
    scd_task >> gold_task
    gold_task >> postgres_task
    postgres_task >> metadata_task
    metadata_task >> email_task


# ============================================================
# DAG REGISTRATION
# ============================================================

olist_etl_pipeline()