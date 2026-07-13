import pandas as pd

from config.config import GOLD_PATH
from pipeline.logger import get_logger

logger = get_logger(__name__)


def create_gold_layer(
    transformed_data: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Creates the Gold Layer.

    Gold Layer Contains

    - Customer Dimension
    - Customer History Dimension (SCD Type 2)
    - Product Dimension
    - Seller Dimension
    - Geolocation Dimension
    - Date Dimension
    - Sales Fact
    - Reviews Fact
    - Revenue Mart
    - KPI Summary
    """

    logger.info("Creating Gold Layer...")

    GOLD_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------
    # Retrieve Transformed Data
    # --------------------------------------------------

    sales_df = transformed_data["fact_sales"]

    reviews_df = transformed_data["reviews"]

    geolocation_df = transformed_data["geolocation"]

    products_df = transformed_data["products"]

    sellers_df = transformed_data["sellers"]

    translation_df = transformed_data["translation"]

    revenue_mart = transformed_data["revenue_mart"]

    kpi_summary = transformed_data["kpi_summary"]

    dim_customer = transformed_data["dim_customer"]

    dim_customer_history = transformed_data["dim_customer_history"]

    # --------------------------------------------------
    # Product Dimension (from products dataset, per spec)
    # --------------------------------------------------

    logger.info("Creating Product Dimension...")

    dim_product = (

        products_df

        .merge(
            translation_df,
            on="product_category_name",
            how="left",
        )

        [
            [
                "product_id",
                "product_category_name",
                "product_category_name_english",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            ]
        ]

        .drop_duplicates(subset="product_id")

        .reset_index(drop=True)

    )

    # --------------------------------------------------
    # Seller Dimension (from sellers dataset, per spec)
    # --------------------------------------------------

    logger.info("Creating Seller Dimension...")

    dim_seller = (

        sellers_df[
            [
                "seller_id",
                "seller_city",
                "seller_state",
            ]
        ]

        .drop_duplicates(subset="seller_id")

        .reset_index(drop=True)

    )

    # --------------------------------------------------
    # Geolocation Dimension
    # --------------------------------------------------

    logger.info("Creating Geolocation Dimension...")

    _geo_cols = [
        c for c in geolocation_df.columns
        if c not in ("pipeline_name", "source_file", "load_timestamp")
    ]

    dim_geolocation = (

        geolocation_df[_geo_cols]

        .drop_duplicates()

        .reset_index(drop=True)

    )

    # --------------------------------------------------
    # Date Dimension
    # --------------------------------------------------

    logger.info("Creating Date Dimension...")

    dim_date = (

        sales_df[
            [
                "order_purchase_timestamp",
                "order_year",
                "order_month",
            ]
        ]

        .rename(
            columns={
                "order_purchase_timestamp": "date"
            }
        )

        .drop_duplicates()

        .sort_values("date")

        .reset_index(drop=True)

    )

    dim_date["day"] = dim_date["date"].dt.day

    dim_date["month_name"] = dim_date["date"].dt.month_name()

    dim_date["quarter"] = dim_date["date"].dt.quarter

    dim_date["year"] = dim_date["date"].dt.year

    dim_date["weekday"] = dim_date["date"].dt.day_name()

    # --------------------------------------------------
    # Sales Fact
    # --------------------------------------------------

    logger.info("Creating Sales Fact...")

    fact_sales = (

        sales_df[
            [
                "order_id",
                "customer_id",
                "product_id",
                "seller_id",
                "payment_type",
                "payment_value",
                "price",
                "freight_value",
                "total_sale",
                "review_score",
                "order_purchase_timestamp",
                "delivery_days",
            ]
        ]

        .copy()

    )

    # --------------------------------------------------
    # Reviews Fact
    # --------------------------------------------------

    logger.info("Creating Reviews Fact...")

    fact_reviews = (

        reviews_df[
            [
                "review_id",
                "order_id",
                "review_score",
                "review_creation_date",
                "review_answer_timestamp",
            ]
        ]

        .drop_duplicates()

        .reset_index(drop=True)

    )

    # --------------------------------------------------
    # Gold Tables
    # --------------------------------------------------

    gold_tables = {

        "dim_customer": dim_customer,

        "dim_customer_history": dim_customer_history,

        "dim_product": dim_product,

        "dim_seller": dim_seller,

        "dim_geolocation": dim_geolocation,

        "dim_date": dim_date,

        "fact_sales": fact_sales,

        "fact_reviews": fact_reviews,

        "revenue_mart": revenue_mart,

        "kpi_summary": kpi_summary,

    }

    # --------------------------------------------------
    # Save Gold Layer
    # --------------------------------------------------

    logger.info("Saving Gold Tables...")

    for table_name, df in gold_tables.items():

        output_path = GOLD_PATH / f"{table_name}.parquet"

        df.to_parquet(
            output_path,
            index=False,
        )

        logger.info(
            f"{table_name} saved successfully ({len(df)} rows)"
        )

    logger.info("Gold Layer created successfully.")

    return gold_tables