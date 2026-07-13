import pandas as pd
from datetime import datetime

from config.config import (
    BRONZE_PATH,
    PIPELINE_NAME,
)

from pipeline.logger import get_logger

logger = get_logger(__name__)


def create_bronze_layer(
    datasets: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """
    Creates the Bronze Layer.

    - Adds ingestion metadata
    - Stores raw data as Parquet
    """

    logger.info("Creating Bronze Layer...")

    BRONZE_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    bronze_datasets = {}

    load_time = datetime.now()

    for table_name, df in datasets.items():

        logger.info(f"Processing {table_name}")

        bronze_df = df.copy()

        # -----------------------------------------
        # Metadata Columns
        # -----------------------------------------

        bronze_df["pipeline_name"] = PIPELINE_NAME

        bronze_df["source_file"] = f"{table_name}.csv"

        bronze_df["load_timestamp"] = load_time

        # -----------------------------------------
        # Save Bronze Layer
        # -----------------------------------------

        output_path = BRONZE_PATH / f"{table_name}.parquet"

        bronze_df.to_parquet(
            output_path,
            index=False,
        )

        bronze_datasets[table_name] = bronze_df

        logger.info(
            f"{table_name} Bronze Layer created ({len(bronze_df)} rows)"
        )

    logger.info("Bronze Layer completed successfully.")

    return bronze_datasets