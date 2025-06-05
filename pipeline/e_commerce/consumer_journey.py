from quixstreams import Application

app = Application(
    broker_address="localhost:9092",
    consumer_group="ecomm_sync_group",
    auto_offset_reset="earliest",
    consumer_extra_config={
        "auto.offset.reset": "earliest",  # Start reading from the earliest message
        "enable.auto.commit": "true",  # Automatically commit offsets
    },
    loglevel="DEBUG",
)

# add topics to consume
product_view_topic = app.topic(
    "ecomm_product_view", value_serializer="json", key_serializer="string"
)
cart_topic = app.topic("ecomm_cart", value_deserializer="json", key_serializer="string")
buy_topic = app.topic("ecomm_buy", value_deserializer="json", key_serializer="string")
# Output topic for customer journey
customer_journey_topic = app.topic(
    "ecomm_customer_journey",
    value_serializer="json",
)

# Streaming dataframe consumers
product_view_sdf = app.dataframe(product_view_topic)
cart_sdf = app.dataframe(cart_topic)
buy_sdf = app.dataframe(buy_topic)

# Stream processing
# Join product view and cart dataframes on user_id
joined_view_cart = cart_sdf.join_asof(
    right=product_view_sdf, how="left", on_merge="keep-left"
)

joined_buy = buy_sdf.join_asof(right=joined_view_cart, how="left", on_merge="keep-left")
joined_buy.to_topic(customer_journey_topic)

app.run()
