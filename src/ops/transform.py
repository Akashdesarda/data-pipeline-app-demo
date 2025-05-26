from datetime import datetime, timedelta, timezone

import polars as pl


def transform_to_star_schema(
    partner_agency_data: pl.LazyFrame,
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    # Dimension: Time
    dim_time = partner_agency_data.select(
        pl.col("departure_date").alias("time_key"),
        pl.col("departure_date").dt.hour().alias("hour"),
        pl.col("departure_date").dt.day().alias("day"),
        pl.col("departure_date").dt.month().alias("month"),
        pl.col("departure_date").dt.year().alias("year"),
    ).unique()

    # Dimension: Product (Flight details)
    dim_product = partner_agency_data.select(
        pl.col("flight_number").alias("product_key"),
        pl.col("price"),
        pl.col("currency"),
    ).unique()

    # Dimension: Customer
    dim_customer = partner_agency_data.select(
        pl.col("customer_name").alias("customer_key"),
        pl.col("agency_id"),
    ).unique()

    # Fact Table: Orders
    fact_orders = partner_agency_data.select(
        pl.col("booking_id").alias("order_id"),
        pl.col("departure_date").alias("time_key"),
        pl.col("flight_number").alias("product_key"),
        pl.col("customer_name").alias("customer_key"),
        pl.col("price"),
    )

    return dim_time, dim_product, dim_customer, fact_orders


def total_ticket_revenue_last_hour(fact_orders: pl.LazyFrame) -> pl.LazyFrame:
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    return fact_orders.filter(pl.col("time_key") >= one_hour_ago).select(
        pl.col("price").sum().alias("total_revenue")
    )


def total_ticket_revenue_last_week(fact_orders: pl.LazyFrame) -> pl.LazyFrame:
    one_hour_ago = datetime.now(timezone.utc) - timedelta(days=7)
    return fact_orders.filter(pl.col("time_key") >= one_hour_ago).select(
        pl.col("price").sum().alias("total_revenue")
    )


def active_sessions_last_15_minutes(clickstream_data: pl.LazyFrame) -> pl.LazyFrame:
    fifteen_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=1)
    return clickstream_data.filter(
        pl.col("event_timestamp") >= fifteen_minutes_ago
    ).select(pl.col("session_id").n_unique().alias("active_sessions"))


def active_sessions_last_1_day(clickstream_data: pl.LazyFrame) -> pl.LazyFrame:
    one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)
    return clickstream_data.filter(pl.col("event_timestamp") >= one_day_ago).select(
        pl.col("session_id").n_unique().alias("active_sessions")
    )
