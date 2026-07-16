"""
MySQL Data Ingestion

Extract datasets from MySQL tables.
"""

import pandas as pd
from sqlalchemy import create_engine

from pipeline.logger import get_logger

logger = get_logger(__name__)


class MySQLIngestion:
    """
    Extract datasets from MySQL tables.
    """

    def extract(
        self,
        connection_string: str,
        table_names: list[str]
    ) -> dict[str, pd.DataFrame]:

        logger.info("Starting MySQL extraction...")

        datasets = {}

        engine = create_engine(connection_string)

        for table_name in table_names:

            logger.info(f"Reading MySQL table: {table_name}")

            datasets[table_name] = pd.read_sql(
                f"SELECT * FROM {table_name}",
                con=engine
            )

            logger.info(
                f"{table_name} extracted successfully "
                f"({len(datasets[table_name])} rows)"
            )

        logger.info("MySQL extraction completed.")

        return datasets