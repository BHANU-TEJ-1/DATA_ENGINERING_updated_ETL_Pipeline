import pandas as pd

from config.config import REJECTED_PATH
from pipeline.logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------
# Expected Primary Keys
# --------------------------------------------------------

PRIMARY_KEYS = {
    "olist_customers_dataset": "customer_id",
    "olist_orders_dataset": "order_id",
    "olist_products_dataset": "product_id",
    "olist_sellers_dataset": "seller_id",
    "olist_order_reviews_dataset": "review_id",
}


# --------------------------------------------------------
# Expected Columns
# --------------------------------------------------------

EXPECTED_COLUMNS = {

    "olist_customers_dataset": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],

    "olist_orders_dataset": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],

    "olist_products_dataset": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],

    "olist_sellers_dataset": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],

}
# Other datasets can be added later if required.


# --------------------------------------------------------
# Columns that must NOT be null (checked per table).
#
# NOTE: We intentionally do NOT null-check every column.
# Columns like review_comment_title / review_comment_message
# or order_delivered_customer_date are legitimately null for
# a large share of real Olist rows (no comment left / order
# not yet delivered). Dropping a row because of a null in a
# non-critical column would silently destroy most of the
# dataset. Only columns that are actually required downstream
# (keys, amounts, core dates) are enforced here.
# --------------------------------------------------------

NULL_CHECK_COLUMNS = {

    "olist_customers_dataset": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],

    "olist_orders_dataset": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    ],

    "olist_order_items_dataset": [
        "order_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    ],

    "olist_order_payments_dataset": [
        "order_id",
        "payment_type",
        "payment_value",
    ],

    "olist_order_reviews_dataset": [
        "review_id",
        "order_id",
        "review_score",
    ],

    "olist_products_dataset": [
        "product_id",
    ],

    "olist_sellers_dataset": [
        "seller_id",
        "seller_city",
        "seller_state",
    ],

    "olist_geolocation_dataset": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
    ],

    "product_category_name_translation": [
        "product_category_name",
        "product_category_name_english",
    ],

}


def validate_data(
    datasets: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Validate all Bronze datasets.

    Validation Includes

    - Schema Validation
    - Duplicate Removal
    - Null Handling
    - Primary Key Validation
    - Rejected Record Storage
    """

    logger.info("Starting Validation...")

    REJECTED_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    validated = {}

    for table_name, df in datasets.items():

        logger.info(f"Validating {table_name}")

        working_df = df.copy()

        # --------------------------------------------------
        # Schema Validation
        # --------------------------------------------------

        if table_name in EXPECTED_COLUMNS:

            expected = EXPECTED_COLUMNS[table_name]

            missing = set(expected) - set(working_df.columns)

            if missing:

                logger.warning(
                    f"{table_name} Missing Columns : {missing}"
                )

        # --------------------------------------------------
        # Duplicate Rows
        # --------------------------------------------------

        duplicate_rows = working_df[
            working_df.duplicated()
        ]

        if not duplicate_rows.empty:

            duplicate_rows.to_csv(
                REJECTED_PATH / f"{table_name}_duplicates.csv",
                index=False,
            )

            working_df = working_df.drop_duplicates()

            logger.info(
                f"Removed {len(duplicate_rows)} duplicate rows."
            )

        # --------------------------------------------------
        # Null Values (only on critical columns)
        # --------------------------------------------------

        check_columns = [
            column
            for column in NULL_CHECK_COLUMNS.get(table_name, [])
            if column in working_df.columns
        ]

        if check_columns:

            null_mask = working_df[check_columns].isnull().any(axis=1)

            null_rows = working_df[null_mask]

            if not null_rows.empty:

                null_rows.to_csv(
                    REJECTED_PATH / f"{table_name}_nulls.csv",
                    index=False,
                )

                working_df = working_df[~null_mask]

                logger.info(
                    f"Removed {len(null_rows)} rows with nulls in "
                    f"critical columns {check_columns}."
                )

        # --------------------------------------------------
        # Primary Key Validation
        # --------------------------------------------------

        if table_name in PRIMARY_KEYS:

            pk = PRIMARY_KEYS[table_name]

            duplicate_pk = working_df[
                working_df.duplicated(
                    subset=pk,
                    keep=False,
                )
            ]

            if not duplicate_pk.empty:

                duplicate_pk.to_csv(
                    REJECTED_PATH / f"{table_name}_duplicate_pk.csv",
                    index=False,
                )

                working_df = working_df.drop_duplicates(
                    subset=pk
                )

                logger.info(
                    f"Removed {len(duplicate_pk)} duplicate primary keys."
                )

        # --------------------------------------------------
        # Data Type Validation
        # --------------------------------------------------

        for column in working_df.columns:

            if "date" in column or "timestamp" in column:

                working_df[column] = pd.to_datetime(
                    working_df[column],
                    errors="coerce",
                )

        validated[table_name] = working_df

        logger.info(
            f"{table_name} Validation Completed ({len(working_df)} rows)"
        )

    logger.info("Validation Completed Successfully.")

    return validated