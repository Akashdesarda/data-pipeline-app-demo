import logging
from pathlib import Path

import orjson
import polars as pl
from quixstreams import Application

from src.connector import DataWriter

logger = logging.getLogger("data-pipeline")
merge_option = DataWriter.generate_delta_table_merge_method_options(
    when_not_matched_insert_all=True, when_matched_update_all=True
)
app = Application(
    broker_address="localhost:9092",
    consumer_group="ecomm_customer_report_group",
    auto_offset_reset="earliest",
    consumer_extra_config={
        "enable.auto.commit": True,  # Automatically commit offsets
    },
    loglevel="DEBUG",
)

with app.get_consumer() as consumer:
    consumer.subscribe(topics=["ecomm_customer_journey"])

    # Starting the 'Forever consuming consumer'
    while True:
        message = consumer.poll(0.5)
        if message is None:
            continue
        elif message.error():
            logger.error("Kafka error:", message.error())
            continue

        value = message.value()

        # Merge data into Delta lake table
        df = pl.from_dict(orjson.loads(value))
        merge_stats = DataWriter.delta_table_merge_disk(
            df=df,
            path=Path(__file__).parent.parent.parent
            / "data/gold/ecomm_customer_journey",
            delta_merge_options={
                "predicate": "source.event_id = target.event_id",  # condition to determine upsert req
                "source_alias": "source",
                "target_alias": "target",
            },
            delta_merge_method_options=merge_option,
        )
        logger.info(f"merge successfully with stats: {merge_stats}")

        consumer.store_offsets(message=message)
