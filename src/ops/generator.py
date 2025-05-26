import logging
from datetime import timezone
from random import choice, uniform

from faker import Faker

from src.models import ClickstreamEvent, PartnerAgencyBooking

logger = logging.getLogger("data-pipeline")
fake = Faker()


def generate_dummy_partner_agency_booking() -> PartnerAgencyBooking:
    logger.debug("generating dummy data for partner agency booking")
    return PartnerAgencyBooking(
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


# Store previously generated events to allow duplicates
generated_events = []


def generate_dummy_clickstream_event(allow_duplicates=False) -> ClickstreamEvent:
    global generated_events

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
        if generated_events:  # Ensure there are events to duplicate
            logger.debug("using past data for duplicating event")
            return choice(generated_events)

    # Otherwise, return the new event and store it
    generated_events.append(new_event)
    logger.debug("generating dummy data for clickstream event")
    return new_event
