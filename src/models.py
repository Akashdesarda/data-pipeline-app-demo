from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class KPIType(Enum):
    total_revenue = "total_revenue"
    active_sessions = "active_sessions"


# Model for partner-agency booking system
class PartnerAgencyBooking(BaseModel):
    booking_id: str = Field(..., description="Unique identifier for the booking")
    agency_id: str = Field(..., description="Identifier for the partner agency")
    customer_name: str = Field(..., description="Name of the customer")
    flight_number: str = Field(
        ..., description="Flight number associated with the booking"
    )
    departure_date: datetime = Field(..., description="Date and time of departure")
    arrival_date: datetime = Field(..., description="Date and time of arrival")
    price: float = Field(..., description="Price of the booking")
    currency: str = Field(..., description="Currency of the price")
    status: str = Field(
        ..., description="Status of the booking (e.g., confirmed, canceled)"
    )


# Model for clickstream collector
class ClickstreamEvent(BaseModel):
    event_id: str = Field(
        ..., description="Unique identifier for the clickstream event"
    )
    user_id: str | None = Field(
        None, description="Identifier for the user (if available)"
    )
    session_id: str = Field(..., description="Session identifier for the user")
    event_type: str = Field(
        ..., description="Type of the event (e.g., page_view, button_click)"
    )
    event_timestamp: datetime = Field(..., description="Timestamp of the event")
    page_url: str = Field(..., description="URL of the page where the event occurred")
    referrer_url: str | None = Field(None, description="Referrer URL (if available)")
    user_agent: str | None = Field(None, description="User agent string of the browser")
    ip_address: str | None = Field(None, description="IP address of the user")


# Model for e-commerce events
class ProductViewEvent(BaseModel):
    event_id: str = Field(..., description="Unique identifier for the event")
    user_id: str | None = Field(
        None, description="Identifier for the user (if available)"
    )
    view_product_id: str = Field(..., description="Identifier for the product viewed")
    view_product_name: str = Field(..., description="Name of the product viewed")
    view_category: str = Field(..., description="Category of the product")
    timestamp: datetime = Field(..., description="Timestamp of the event")


class AddToCartEvent(BaseModel):
    event_id: str = Field(..., description="Unique identifier for the event")
    user_id: str | None = Field(
        None, description="Identifier for the user (if available)"
    )
    cart_product_id: str = Field(..., description="Identifier for the product added")
    cart_product_name: str = Field(..., description="Name of the product added")
    cart_quantity: int = Field(..., description="Quantity of the product added")
    price: float = Field(..., description="Price of the product at the time of adding")
    timestamp: datetime = Field(..., description="Timestamp of the event")


class PurchaseEvent(BaseModel):
    event_id: str = Field(..., description="Unique identifier for the event")
    user_id: str | None = Field(
        None, description="Identifier for the user (if available)"
    )
    order_id: str = Field(..., description="Identifier for the order")
    purchase_product_id: str = Field(
        ..., description="Identifier for the product purchased"
    )
    purchase_product_name: str = Field(..., description="Name of the product purchased")
    purchase_quantity: int = Field(..., description="Quantity purchased")
    price: float = Field(
        ..., description="Price of the product at the time of purchase"
    )
    timestamp: datetime = Field(..., description="Timestamp of the event")
    total_amount: float = Field(..., description="Total amount of the purchase")
    payment_method: str = Field(..., description="Payment method used")
