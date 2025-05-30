from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from src.connector import DataLoader
from src.models import KPIType
from src.ops.transform import (
    active_sessions_last_1_day,
    total_ticket_revenue_last_week,
)

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "API is running"}


@app.get("/v1/kpi")
async def calculate_kpi(name: KPIType) -> ORJSONResponse:
    """Calculate the specified KPI."""
    parent_folder_path = Path(".").parent.parent.parent
    if name == KPIType.total_revenue:
        fact_order = DataLoader.delta_table_from_disk(
            parent_folder_path / "data/gold/partner_agency_booking_data/fact_orders"
        )
        # using async to collect the result &  avoid blocking the event loop
        result = await total_ticket_revenue_last_week(fact_order).collect_async()

    if name == KPIType.active_sessions:
        clickstream_data = DataLoader.delta_table_from_disk(
            parent_folder_path / "data/bronze/clickstream_data_data"
        )
        # using async to collect the result &  avoid blocking the event loop
        result = await active_sessions_last_1_day(clickstream_data).collect_async()

    return ORJSONResponse({"kpi": name.value, "value": result.item(0, 0)})
