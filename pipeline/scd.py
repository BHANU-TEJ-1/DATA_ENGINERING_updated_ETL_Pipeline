import pandas as pd
from datetime import datetime

from pipeline.logger import get_logger

logger = get_logger(__name__)


def create_customer_dimension(
    sales_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Creates the current Customer Dimension
    (SCD Type 1).
    """

    logger.info("Creating Customer Dimension (Type 1)...")

    dim_customer = (

        sales_df[
            [
                "customer_id",
                "customer_unique_id",
                "customer_city",
                "customer_state",
            ]
        ]

        .sort_values("customer_id")

        .drop_duplicates(
            subset="customer_id",
            keep="last",
        )

        .reset_index(drop=True)

    )

    logger.info(
        f"Customer Dimension created ({len(dim_customer)} rows)"
    )

    return dim_customer


def create_customer_history(
    sales_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Creates Customer History
    (SCD Type 2).
    """

    logger.info("Creating Customer History (Type 2)...")

    history = (

        sales_df[
            [
                "customer_id",
                "customer_unique_id",
                "customer_city",
                "customer_state",
                "order_purchase_timestamp",
            ]
        ]

        .sort_values(
            [
                "customer_id",
                "order_purchase_timestamp",
            ]
        )

        .copy()

    )

    history["effective_start_date"] = history[
        "order_purchase_timestamp"
    ]

    history["version"] = (

        history

        .groupby("customer_id")

        .cumcount()

        + 1

    )

    # effective_end_date = start date of the NEXT version for that
    # customer (NaT if this is the latest version). is_current is
    # True only for a customer's most recent version.
    history["effective_end_date"] = (

        history

        .groupby("customer_id")["effective_start_date"]

        .shift(-1)

    )

    history["is_current"] = history["effective_end_date"].isna()

    logger.info(
        f"Customer History created ({len(history)} rows)"
    )

    return history


def apply_scd(
    transformed_data: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Apply SCD processing.
    """

    logger.info("Applying Slowly Changing Dimensions...")

    sales = transformed_data["fact_sales"]

    transformed_data["dim_customer"] = create_customer_dimension(
        sales
    )

    transformed_data["dim_customer_history"] = create_customer_history(
        sales
    )

    logger.info("SCD completed successfully.")

    return transformed_data