from sqlalchemy import create_engine

from config.config import DATABASE_URL
from pipeline.logger import get_logger

logger = get_logger(__name__)


def load_to_postgres(gold_tables: dict) -> None:
    """
    Load Gold Layer tables into PostgreSQL.

    Args:
        gold_tables: Dictionary containing Gold DataFrames.
    """

    logger.info("Connecting to PostgreSQL...")

    engine = create_engine(DATABASE_URL)

    try:

        for table_name, df in gold_tables.items():

            logger.info(f"Loading {table_name}...")

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
                chunksize=5000,
                method="multi",
            )

            logger.info(
                f"{table_name} loaded successfully ({len(df)} rows)"
            )

        logger.info("All Gold tables loaded successfully.")

    except Exception as e:

        logger.error(f"Database Load Failed: {e}")

        raise

    finally:

        engine.dispose()

        logger.info("Database connection closed.")