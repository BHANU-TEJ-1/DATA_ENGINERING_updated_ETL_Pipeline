import pandas as pd
from pathlib import Path

from config.config import RAW_DATA_PATH
from pipeline.logger import get_logger

logger = get_logger(__name__)


def extract_data() -> dict[str, pd.DataFrame]:
    """
    Reads all CSV files from the raw data folder.

    Returns:
        Dictionary containing DataFrames.
    """

    logger.info("Starting data extraction...")

    datasets = {}

    csv_files = sorted(Path(RAW_DATA_PATH).glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DATA_PATH}")

    for file in csv_files:

        logger.info(f"Reading {file.name}")

        df = pd.read_csv(file)

        datasets[file.stem] = df

        logger.info(f"{file.name} loaded successfully ({len(df)} rows)")

    logger.info("Data extraction completed.")

    return datasets