import logging
from pathlib import Path

import polars as pl
from sqlglot import Dialects, select

from src.connector import DataLoader, DataWriter
from src.ops.consumer import Consumer
from src.ops.transform import transform_to_star_schema

logger = logging.getLogger("data-pipeline")
consume = Consumer()

# Step 1: Processing data of partner agency booking
latest_csv_file = consume.get_latest_file_from_azure(
    "abfs://dummy-container/bronze/csv/*.csv"
)
# csv
partner_data_from_csv = consume.read_csv_from_azure(latest_csv_file)
partner_data_from_csv = partner_data_from_csv.with_columns(
    (pl.col("departure_date").str.to_datetime(time_zone="UTC")).alias("departure_date"),
    (pl.col("arrival_date").str.to_datetime(time_zone="UTC")).alias("arrival_date"),
)
merge_option = DataWriter.generate_delta_table_merge_method_options(
    when_not_matched_insert_all=True, when_matched_update_all=True
)
merge_info = DataWriter.delta_table_merge_disk(
    df=partner_data_from_csv.collect(),
    path=Path("./data/bronze/partner_agency_booking_data"),
    delta_merge_options={
        "predicate": "source.booking_id = target.booking_id",  # condition to determine upsert req
        "source_alias": "source",
        "target_alias": "target",
    },
    delta_merge_method_options=merge_option,
)
logger.info(f"partner agency booking csv data merge successful with stats {merge_info}")

# postgresql
# NOTE - below query will always fetch last 50 records from the table
subq = select("count(*) - 50").from_("pipeline.partner_agency_booking")
query = (
    select("*")
    .from_("pipeline.partner_agency_booking")
    .offset(subq.subquery())
    .limit(50)
)
partner_data_from_db = consume.read_data_from_postgres(query.sql(Dialects.POSTGRES))
merge_info = DataWriter.delta_table_merge_disk(
    df=partner_data_from_csv.collect(),
    path=Path("./data/bronze/partner_agency_booking_data"),
    delta_merge_options={
        "predicate": "source.booking_id = target.booking_id",  # condition to determine upsert req
        "source_alias": "source",
        "target_alias": "target",
    },
    delta_merge_method_options=merge_option,
)
logger.info(
    f"partner agency booking postgres data merge successful with stats {merge_info}"
)

# Step 2: Processing data of clickstream data

# postgresql
# NOTE - below query will always fetch last 50 records from the table
subq = select("count(*) - 50").from_("pipeline.clickstream_data")
query = select("*").from_("pipeline.clickstream_data").offset(subq.subquery()).limit(50)
clickstream_data_from_db = consume.read_data_from_postgres(query.sql(Dialects.POSTGRES))
merge_info = DataWriter.delta_table_merge_disk(
    df=clickstream_data_from_db.collect(),
    path=Path("./data/bronze/clickstream_data_data"),
    delta_merge_options={
        "predicate": "source.event_id = target.event_id",  # condition to determine upsert req
        "source_alias": "source",
        "target_alias": "target",
    },
    delta_merge_method_options=merge_option,
)
logger.info(
    f"partner agency booking postgres data merge successful with stats {merge_info}"
)

# kafka
clickstream_data_from_kafka = consume.read_messages_from_kafka("topic_0")
clickstream_data_from_kafka = clickstream_data_from_kafka.with_columns(
    (pl.col("event_timestamp").str.to_datetime(time_zone="UTC")).alias("event_time")
)
merge_info = DataWriter.delta_table_merge_disk(
    df=clickstream_data_from_kafka.collect(),
    path=Path("./data/bronze/clickstream_data_data"),
    delta_merge_options={
        "predicate": "source.event_id = target.event_id",  # condition to determine upsert req
        "source_alias": "source",
        "target_alias": "target",
    },
    delta_merge_method_options=merge_option,
)
logger.info(f"clickstream data kafka merge successful with stats {merge_info}")

# Step 3: Transforming partner agency booking data to star schema

dim_time, dim_product, dim_customer, fact_orders = transform_to_star_schema(
    DataLoader.delta_table_from_disk(Path("./data/bronze/partner_agency_booking_data"))
)

# saving transformed data to gold layer
merge_info = DataWriter.delta_table_merge_disk(
    df=dim_time.collect(),
    path=Path("./data/gold/partner_agency_booking_data/dim_time"),
    delta_merge_options={
        "predicate": "source.time_key = target.time_key",  # condition to determine upsert req
        "source_alias": "source",
        "target_alias": "target",
    },
    delta_merge_method_options=merge_option,
)
logger.info(
    f"partner agency booking time dim data merge successful with stats {merge_info}"
)
merge_info = DataWriter.delta_table_merge_disk(
    df=dim_product.collect(),
    path=Path("./data/gold/partner_agency_booking_data/dim_product"),
    delta_merge_options={
        "predicate": "source.product_key = target.product_key AND source.price = target.price",  # condition to determine upsert req
        "source_alias": "source",
        "target_alias": "target",
    },
    delta_merge_method_options=merge_option,
)
logger.info(
    f"partner agency booking product dim data merge successful with stats {merge_info}"
)
merge_info = DataWriter.delta_table_merge_disk(
    df=dim_customer.collect(),
    path=Path("./data/gold/partner_agency_booking_data/dim_customer"),
    delta_merge_options={
        "predicate": "s.customer_key = t.customer_key AND s.agency_id = t.agency_id",  # condition to determine upsert req
        "source_alias": "s",
        "target_alias": "t",
    },
    delta_merge_method_options=merge_option,
)
logger.info(
    f"partner agency booking customer dim data merge successful with stats {merge_info}"
)
merge_info = DataWriter.delta_table_merge_disk(
    df=fact_orders.collect(),
    path=Path("./data/gold/partner_agency_booking_data/fact_orders"),
    delta_merge_options={
        "predicate": "source.order_id = target.order_id",  # condition to determine upsert req
        "source_alias": "source",
        "target_alias": "target",
    },
    delta_merge_method_options=merge_option,
)
logger.info(
    f"partner agency booking orders fact data merge successful with stats {merge_info}"
)
