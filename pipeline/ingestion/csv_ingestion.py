from pathlib import Path

import pandas as pd

from pipeline.logger import get_logger

logger = get_logger(__name__)


class CSVIngestion:

    def extract(self, file_paths: dict[str, str]) -> dict[str, pd.DataFrame]:

        logger.info("Starting CSV extraction...")

        datasets = {}

        for dataset_name, file_path in file_paths.items():

            path = Path(file_path)

            logger.info(f"Reading CSV dataset: {dataset_name}")
            logger.info(f"Path: {path.resolve()}")

            if not path.exists():
                raise FileNotFoundError(
                    f"CSV file not found:\n{path}"
                )

            logger.info(f"File size: {path.stat().st_size} bytes")

            try:
                df = pd.read_csv(path)

            except Exception:
                logger.exception(
                    f"Failed while reading CSV dataset '{dataset_name}'"
                )
                raise

            datasets[dataset_name] = df

            logger.info(
                f"{dataset_name} extracted successfully "
                f"({len(df)} rows)"
            )

        logger.info("CSV extraction completed.")

        return datasets