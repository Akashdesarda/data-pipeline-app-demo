import logging

from quixstreams import Application

from src.ops.generator import generate_dummy_e_commerce_data

logger = logging.getLogger("data-pipeline")
app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",
    producer_extra_config={
        "linger.ms": "300",  # Wait up to 300ms for more messages before sending
        "compression.type": "gzip",  # Use gzip compression for messages
    },
)
product_view_topic = app.topic(
    "ecomm_product_view", value_serializer="json", key_serializer="string"
)
cart_topic = app.topic("ecomm_cart", value_serializer="json", key_serializer="string")
buy_topic = app.topic("ecomm_buy", value_serializer="json", key_serializer="string")


with app.get_producer() as producer:
    for _ in range(200):
        logger.info(f"current iteration: {_}")
        # generate dummy data
        event = generate_dummy_e_commerce_data()

        # sending the event to respective topics
        # Product view event
        product_view_msg = product_view_topic.serialize(
            key=event["product_view"].user_id,
            value=event["product_view"].model_dump(mode="json"),
        )
        producer.produce(
            product_view_topic.name,
            value=product_view_msg.value,
            key=product_view_msg.key,
        )
        logger.debug(f"producing product view event: {event['product_view'].event_id}")

        # Add to cart event
        # NOTE - It is possible that the add_to_cart and purchase events are None
        if event["add_to_cart"]:
            cart_msg = cart_topic.serialize(
                key=event["add_to_cart"].user_id,
                value=event["add_to_cart"].model_dump(mode="json"),
            )
            producer.produce(cart_topic.name, value=cart_msg.value, key=cart_msg.key)
            logger.debug(
                f"producing product add to cart event: {event['add_to_cart'].event_id}"
            )

        if event["purchase"]:
            buy_msg = buy_topic.serialize(
                key=event["purchase"].user_id,
                value=event["purchase"].model_dump(mode="json"),
            )
            producer.produce(buy_topic.name, value=buy_msg.value, key=buy_msg.key)
            logger.debug(f"producing buy event: {event['purchase'].event_id}")
