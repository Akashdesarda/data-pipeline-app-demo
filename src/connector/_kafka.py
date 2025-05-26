import logging
import os

import orjson
import polars as pl
from kafka import KafkaConsumer, KafkaProducer

from src.config import get_config

logger = logging.getLogger("data-pipeline")
config = get_config(os.getenv("ENV", "local"))

kafka_common_config = {
    "bootstrap_servers": config.kafka_bootstrap_server,
    "sasl_plain_username": config.kafka_user_name,
    "sasl_plain_password": config.KAFKA_CLIENT_SECRET,
    "sasl_mechanism": "PLAIN",
    "security_protocol": "SASL_SSL",
    "request_timeout_ms": 60000,
}


def publish_message(data: pl.DataFrame, topic: str, key_name: str | None = None):
    _extra_config = {
        "client_id": "ccloud-python-client-44f033c9-7bfc-4b8f-8d50-39d55837d2cc",
        "key_serializer": lambda x: orjson.dumps(x),
        "value_serializer": lambda x: orjson.dumps(x),
    }
    publisher_client = KafkaProducer(**{**kafka_common_config, **_extra_config})

    # sending the message
    try:
        for row in data.iter_rows(named=True):
            key = row[key_name] if key_name else None
            publisher_client.send(topic=topic, key=key, value=row)
            logger.debug(f"send message with key {key} to topic {topic}")
        publisher_client.flush()
        logger.info(f"successfully publish given data to topic - {topic}")
    except Exception as e:
        logger.error(f"Error publishing message: {e}")
    finally:
        publisher_client.close()


def consume_message(topic) -> pl.DataFrame:
    messages = []
    _extra_config = {
        "group_id": "clickstream_group",  # NOTE - do not use " " 0r - in group id
        "auto_offset_reset": "earliest",  # Start from the beginning if no offset exists
        "enable_auto_commit": True,  # Ensure offsets are committed
        "value_deserializer": lambda x: orjson.loads(x),
    }
    consumer_client = KafkaConsumer(**{**kafka_common_config, **_extra_config})
    consumer_client.subscribe([topic])

    try:
        while True:
            # Poll for messages with a timeout (in milliseconds)
            records = consumer_client.poll(timeout_ms=3000)
            if not records:  # No messages received within the timeout
                logger.info("No more messages to consume. Exiting...")
                break

            for _, record_batch in records.items():
                for message in record_batch:
                    messages.append(pl.from_dict(message.value))
                    logger.debug(
                        f"Polled message with key: {message.key} from topic: {topic}"
                    )

    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        consumer_client.close()

    return pl.concat(messages, how="diagonal_relaxed") if messages else pl.DataFrame()
