import pandas as pd

from pipeline.logger import get_logger

logger = get_logger(__name__)

# Bronze-layer lineage columns. Every table carries these, so they
# must be dropped before joining tables together or pandas.merge
# raises a MergeError on duplicate column names.
_BRONZE_METADATA_COLUMNS = ["pipeline_name", "source_file", "load_timestamp"]


def _drop_metadata_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drops Bronze lineage columns that aren't needed for joins."""

    return df.drop(
        columns=[c for c in _BRONZE_METADATA_COLUMNS if c in df.columns]
    )


def create_sales_dataset(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Creates the main sales dataset by joining all required tables.
    """

    logger.info("Creating sales dataset...")

    customers = _drop_metadata_columns(datasets["olist_customers_dataset"])
    orders = _drop_metadata_columns(datasets["olist_orders_dataset"])
    order_items = _drop_metadata_columns(datasets["olist_order_items_dataset"])
    payments = _drop_metadata_columns(datasets["olist_order_payments_dataset"])
    reviews = _drop_metadata_columns(datasets["olist_order_reviews_dataset"])
    products = _drop_metadata_columns(datasets["olist_products_dataset"])
    sellers = _drop_metadata_columns(datasets["olist_sellers_dataset"])
    category = _drop_metadata_columns(
        datasets["product_category_name_translation"]
    )

    sales = orders.merge(
        customers,
        on="customer_id",
        how="left"
    )

    sales = sales.merge(
        order_items,
        on="order_id",
        how="left"
    )

    sales = sales.merge(
        payments,
        on="order_id",
        how="left"
    )

    sales = sales.merge(
        reviews,
        on="order_id",
        how="left"
    )

    sales = sales.merge(
        products,
        on="product_id",
        how="left"
    )

    sales = sales.merge(
        sellers,
        on="seller_id",
        how="left"
    )

    sales = sales.merge(
        category,
        on="product_category_name",
        how="left"
    )

    logger.info(f"Sales dataset created successfully ({len(sales)} rows).")

    return sales


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds business-derived columns.
    """

    logger.info("Adding derived columns...")

    df = df.copy()

    df["total_sale"] = df["price"] + df["freight_value"]

    df["order_year"] = df["order_purchase_timestamp"].dt.year

    df["order_month"] = df["order_purchase_timestamp"].dt.month

    df["delivery_days"] = (
        df["order_delivered_customer_date"]
        - df["order_purchase_timestamp"]
    ).dt.days

    return df


def apply_window_functions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates SQL window functions using Pandas.
    """

    logger.info("Applying window functions...")

    df = df.copy()

    df["customer_order_rank"] = (
        df.groupby("customer_id")
        .cumcount()
        + 1
    )

    df["running_customer_sales"] = (
        df.groupby("customer_id")["total_sale"]
        .cumsum()
    )

    return df


def create_revenue_mart(df: pd.DataFrame) -> pd.DataFrame:
    """
    Revenue summary by customer state.
    """

    logger.info("Creating Revenue Mart...")

    revenue = (
        df.groupby("customer_state")
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_sale", "sum"),
            average_order_value=("total_sale", "mean"),
        )
        .reset_index()
    )

    return revenue


def create_kpi_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates business KPI summary.
    """

    logger.info("Creating KPI Summary...")

    kpi = pd.DataFrame({
        "total_customers": [df["customer_id"].nunique()],
        "total_orders": [df["order_id"].nunique()],
        "total_products": [df["product_id"].nunique()],
        "total_sellers": [df["seller_id"].nunique()],
        "total_revenue": [df["total_sale"].sum()],
        "average_order_value": [df["total_sale"].mean()],
        "average_review_score": [df["review_score"].mean()]
    })

    return kpi


def create_business_layer(
    datasets: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """
    Main business transformation.

    Per spec, this layer ONLY creates fact_sales, revenue_mart and
    kpi_summary (no dimensions here - those are built in the SCD /
    Gold layers). It also passes through the cleaned Silver entity
    tables unchanged so downstream layers (SCD, Gold) don't need to
    reach back into the Silver dict directly.
    """

    logger.info("Starting Business Transformations...")

    sales = create_sales_dataset(datasets)

    sales = add_derived_columns(sales)

    sales = apply_window_functions(sales)

    revenue_mart = create_revenue_mart(sales)

    kpi_summary = create_kpi_summary(sales)

    transformed_data = {

        # Pass-through entity tables (cleaned, from Silver)
        "customers": datasets["olist_customers_dataset"],

        "products": datasets["olist_products_dataset"],

        "sellers": datasets["olist_sellers_dataset"],

        "geolocation": datasets["olist_geolocation_dataset"],

        "reviews": datasets["olist_order_reviews_dataset"],

        "translation": datasets["product_category_name_translation"],

        "orders": datasets["olist_orders_dataset"],

        "payments": datasets["olist_order_payments_dataset"],

        "order_items": datasets["olist_order_items_dataset"],

        # Business tables created in this layer
        "fact_sales": sales,

        "revenue_mart": revenue_mart,

        "kpi_summary": kpi_summary,

    }

    logger.info("Business Transformations completed successfully.")

    return transformed_data