"""
Main Pipeline Entrypoint.

Runs the full Olist ETL pipeline end-to-end, in order:

Extract -> Bronze -> Validation -> Silver -> Transformation ->
SCD -> Gold -> PostgreSQL -> Metadata -> Email

This module is used both for local/manual runs (`python main.py`)
and is the logic that the Airflow DAG (dags/pipeline_DAG_.py)
wraps as individual tasks.
"""

import uuid
from datetime import datetime

from pipeline.extraction import extract_data
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


def run_pipeline() -> None:
    """
    Runs the complete ETL pipeline and reports the outcome via
    PostgreSQL metadata + email, whether it succeeds or fails.
    """

    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    records_processed = 0
    records_rejected = 0
    gold_table_names: list[str] = []
    database_status = "Not Attempted"
    metadata_status = "Not Attempted"

    try:

        logger.info(f"========== PIPELINE STARTED (run_id={run_id}) ==========")

        _clear_rejected_folder()

        # 1. Extract
        raw_datasets = extract_data()

        # 2. Bronze
        bronze_datasets = create_bronze_layer(raw_datasets)

        # 3. Validation
        validated_datasets = validate_data(bronze_datasets)

        # 4. Silver
        silver_datasets = create_silver_layer(validated_datasets)

        # 5. Transformation (business layer)
        transformed_data = create_business_layer(silver_datasets)

        # 6. SCD
        transformed_data = apply_scd(transformed_data)

        # 7. Gold
        gold_tables = create_gold_layer(transformed_data)
        gold_table_names = list(gold_tables.keys())

        # 8. Load to PostgreSQL (warehouse DB)
        load_to_postgres(gold_tables)
        database_status = "Successful"

        # Record counts for metadata/email
        records_processed = sum(
            len(df) for df in gold_tables.values()
        )

        records_rejected = _count_rejected()

        end_time = datetime.now()

        # 9. Metadata
        save_metadata(
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            records_processed=records_processed,
            records_rejected=records_rejected,
            status="SUCCESS",
            gold_tables_created=gold_table_names,
        )
        metadata_status = "Successful"

        # 10. Email
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

        logger.info("========== PIPELINE COMPLETED SUCCESSFULLY ==========")

    except Exception as error:

        end_time = datetime.now()

        logger.error(f"Pipeline failed: {error}", exc_info=True)

        try:
            save_metadata(
                run_id=run_id,
                start_time=start_time,
                end_time=end_time,
                records_processed=records_processed,
                records_rejected=records_rejected,
                status="FAILED",
                gold_tables_created=gold_table_names,
            )
            metadata_status = "Successful"
        except Exception as metadata_error:
            metadata_status = f"Failed - {metadata_error}"
            logger.error(
                f"Failed to save failure metadata: {metadata_error}"
            )

        try:
            send_email(
                start_time=start_time,
                end_time=end_time,
                records_processed=records_processed,
                records_rejected=records_rejected,
                status=f"FAILED - {error}",
                run_id=run_id,
                gold_tables_created=gold_table_names,
                database_status=database_status,
                metadata_status=metadata_status,
            )
        except Exception as email_error:
            logger.error(
                f"Failed to send failure email: {email_error}"
            )

        raise


def _clear_rejected_folder() -> None:
    """
    Removes rejected-record CSVs from previous runs so that
    records_rejected reflects only the current run.
    """

    from config.config import REJECTED_PATH

    REJECTED_PATH.mkdir(parents=True, exist_ok=True)

    for file in REJECTED_PATH.glob("*.csv"):
        file.unlink()


def _count_rejected() -> int:
    """
    Counts total rejected rows by summing the row counts of every
    CSV file written to the rejected-records folder during this run.
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


if __name__ == "__main__":
    run_pipeline()
