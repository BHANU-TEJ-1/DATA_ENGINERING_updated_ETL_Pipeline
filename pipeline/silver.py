import pandas as pd

from config.config import SILVER_PATH
from pipeline.logger import get_logger

logger = get_logger(__name__)


def create_silver_layer(
    datasets: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Creates the Silver Layer.

    Performs:

    - Column Standardization
    - String Cleaning
    - Datatype Conversion
    - Date Conversion
    - Saves Silver Parquet Files
    """

    logger.info("Creating Silver Layer...")

    SILVER_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    silver_datasets = {}

    # ------------------------------------------
    # Numeric Columns
    # ------------------------------------------

    numeric_columns = [

        "price",

        "payment_value",

        "freight_value",

        "product_weight_g",

        "product_length_cm",

        "product_height_cm",

        "product_width_cm",

        "review_score",

        "product_name_lenght",

        "product_description_lenght",

        "product_photos_qty",

    ]

    for table_name, df in datasets.items():

        logger.info(f"Processing {table_name}")

        silver_df = df.copy()

        # ------------------------------------------
        # Standardize Column Names
        # ------------------------------------------

        silver_df.columns = (

            silver_df.columns

            .str.strip()

            .str.lower()

            .str.replace(" ", "_")

        )

        # ------------------------------------------
        # Clean String Columns
        # ------------------------------------------

        string_columns = silver_df.select_dtypes(
            include="object"
        ).columns

        for column in string_columns:

            # Only stringify/strip non-null values, otherwise
            # astype(str) turns real NaNs into the literal
            # string "nan" and silently corrupts the data.
            silver_df[column] = silver_df[column].apply(
                lambda value: (
                    str(value).strip()
                    if pd.notnull(value)
                    else value
                )
            )

        # ------------------------------------------
        # Date Conversion
        # ------------------------------------------

        for column in silver_df.columns:

            if "date" in column or "timestamp" in column:

                silver_df[column] = pd.to_datetime(

                    silver_df[column],

                    errors="coerce"

                )

        # ------------------------------------------
        # Numeric Conversion
        # ------------------------------------------

        for column in numeric_columns:

            if column in silver_df.columns:

                silver_df[column] = pd.to_numeric(

                    silver_df[column],

                    errors="coerce"

                )

        # ------------------------------------------
        # Save Silver Layer
        # ------------------------------------------

        output_path = SILVER_PATH / f"{table_name}.parquet"

        silver_df.to_parquet(

            output_path,

            index=False,

        )

        silver_datasets[table_name] = silver_df

        logger.info(

            f"{table_name} Silver Layer created ({len(silver_df)} rows)"

        )

    logger.info("Silver Layer completed successfully.")

    return silver_datasets