from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from config.config import (
    DATABASE_URL,
    METADATA_TABLE,
    PIPELINE_NAME,
)

from pipeline.logger import get_logger

logger = get_logger(__name__)


def save_metadata(
    run_id: str,
    start_time: datetime,
    end_time: datetime,
    records_processed: int,
    records_rejected: int,
    status: str,
    gold_tables_created: list[str] | None = None,
) -> None:
    """
    Save pipeline execution metadata. One row is appended per run.
    """

    logger.info("Saving pipeline metadata...")

    duration = (end_time - start_time).total_seconds()

    metadata = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "pipeline_name": PIPELINE_NAME,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "status": status,
                "records_processed": records_processed,
                "records_rejected": records_rejected,
                "gold_tables_created": ", ".join(gold_tables_created or []),
            }
        ]
    )

    engine = create_engine(DATABASE_URL)

    try:

        metadata.to_sql(
            METADATA_TABLE,
            engine,
            if_exists="append",
            index=False,
        )

        logger.info("Pipeline metadata saved successfully.")

    except Exception as e:

        logger.error(f"Metadata Save Failed : {e}")

        raise

    finally:

        engine.dispose()