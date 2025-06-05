import logging
from datetime import timezone
from random import choice, uniform
from typing import TypedDict

from faker import Faker

from src.models import (
    AddToCartEvent,
    ClickstreamEvent,
    PartnerAgencyBooking,
    ProductViewEvent,
    PurchaseEvent,
)

logger = logging.getLogger("data-pipeline")
fake = Faker()

# Store previously generated events to allow duplicates
generated_partner_agency_events = []
generated_clickstream_events = []
generated_e_commerce_events = []


def generate_dummy_partner_agency_booking(
    allow_duplicates: bool = True,
) -> PartnerAgencyBooking:
    global generated_partner_agency_events
    logger.debug("generating dummy data for partner agency booking")
    new_event = PartnerAgencyBooking(
        booking_id=fake.uuid4(),
        agency_id=choice(["agency1", "agency2", "agency3", "agency4"]),
        customer_name=fake.name(),
        flight_number=f"VA{fake.random_int(min=1000, max=9999)}",
        departure_date=fake.date_time_between(
            start_date="-1d", end_date="+30d", tzinfo=timezone.utc
        ),
        arrival_date=fake.date_time_between(
            start_date="+1d", end_date="+31d", tzinfo=timezone.utc
        ),
        price=round(uniform(100.0, 1000.0), 2),
        currency=choice(["USD", "EUR", "GBP", "INR"]),
        status=choice(["confirmed", "canceled", "pending"]),
    )
    # Decide whether to return a duplicate
    if allow_duplicates and fake.random_int(min=1, max=100) <= 30:  # 30% chance
        if generated_partner_agency_events:  # Ensure there are events to duplicate
            logger.debug("using past data for duplicating event")
            return choice(generated_clickstream_events)

    # Otherwise, return the new event and store it
    generated_partner_agency_events.append(new_event)
    logger.debug("generating dummy data for partner booking agency event")
    return new_event


def generate_dummy_clickstream_event(allow_duplicates=True) -> ClickstreamEvent:
    global generated_clickstream_events

    # Generate a new event
    new_event = ClickstreamEvent(
        event_id=fake.uuid4(),
        user_id=fake.uuid4() if fake.boolean() else None,
        session_id=fake.uuid4(),
        event_type=choice(["page_view", "button_click", "form_submit"]),
        event_timestamp=fake.date_time_between(
            start_date="-30d", end_date="now", tzinfo=timezone.utc
        ),
        page_url=fake.url(),
        referrer_url=fake.url() if fake.boolean() else None,
        user_agent=fake.user_agent(),
        ip_address=fake.ipv4() if fake.boolean() else None,
    )

    # Decide whether to return a duplicate
    if allow_duplicates and fake.random_int(min=1, max=100) <= 30:  # 30% chance
        if generated_clickstream_events:  # Ensure there are events to duplicate
            logger.debug("using past data for duplicating event")
            return choice(generated_clickstream_events)

    # Otherwise, return the new event and store it
    generated_clickstream_events.append(new_event)
    logger.debug("generating dummy data for clickstream event")
    return new_event


class ecomm_event(TypedDict):
    product_view: ProductViewEvent
    add_to_cart: AddToCartEvent | None
    purchase: PurchaseEvent | None


def generate_dummy_e_commerce_data(allow_duplicate: bool = True) -> ecomm_event:
    global generated_e_commerce_events

    product_id = ["product1", "product2", "product3", "product4"]
    product_name = ["Product A", "Product B", "Product C", "Product D"]
    category = ["Electronics", "Clothing", "Books", "Home"]
    users = [f"user_{i}" for i in range(1, 80)]  # Simulating 100 users

    session = fake.uuid4()

    view_event = ProductViewEvent(
        event_id=fake.uuid4(),
        user_id=choice(users),
        view_product_id=choice(product_id),
        view_product_name=choice(product_name),
        view_category=choice(category),
        timestamp=fake.date_time_between(
            start_date="-30d", end_date="now", tzinfo=timezone.utc
        ),
    )
    cart_event = AddToCartEvent(
        event_id=fake.uuid4(),
        user_id=choice(users),
        cart_product_id=choice(product_id),
        cart_product_name=choice(product_name),
        cart_quantity=fake.random_int(min=1, max=5),
        price=round(uniform(10.0, 500.0), 2),
        timestamp=fake.date_time_between(
            start_date="-15d", end_date="now", tzinfo=timezone.utc
        ),
    )
    buy_event = PurchaseEvent(
        event_id=fake.uuid4(),
        user_id=choice(users),
        order_id=fake.uuid4(),
        purchase_product_id=choice(product_id),
        purchase_product_name=choice(product_name),
        purchase_quantity=fake.random_int(min=1, max=5),
        price=round(uniform(10.0, 500.0), 2),
        timestamp=fake.date_time_between(
            start_date="-10d", end_date="now", tzinfo=timezone.utc
        ),
        total_amount=round(uniform(10.0, 500.0), 2),
        payment_method=choice(
            ["credit_card", "upi", "bank_transfer", "cash_on_delivery"]
        ),
    )

    # Decide whether to return a duplicate
    if allow_duplicate and fake.random_int(min=1, max=100) <= 30:  # 30% chance
        if generated_e_commerce_events:  # Ensure there are events to duplicate
            logger.debug("using past data for duplicating event")
            return choice(generated_e_commerce_events)

    # Otherwise, return the new event and store it
    new_event: ecomm_event = {
        "product_view": view_event,
        "add_to_cart": cart_event if fake.boolean(chance_of_getting_true=80) else None,
        "purchase": buy_event if fake.boolean(chance_of_getting_true=60) else None,
    }
    generated_e_commerce_events.append(new_event)
    logger.debug("generating dummy data for e-commerce event")
    return new_event
