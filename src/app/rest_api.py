from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from src.connector import DataLoader
from src.models import KPIType
from src.ops.transform import (
    active_sessions_last_15_minutes,
    total_ticket_revenue_last_hour,
)

app = FastAPI()


@app.get("/v1/kpi")
async def calculate_kpi(name: KPIType) -> ORJSONResponse:
    """Calculate the specified KPI."""
    parent_folder_path = Path(".").parent.parent.parent
    if name == KPIType.total_revenue:
        fact_order = DataLoader.delta_table_from_disk(
            parent_folder_path / "data/gold/partner_agency_booking_data/fact_orders"
        )
        result = await total_ticket_revenue_last_hour(fact_order).collect_async()

    if name == KPIType.active_sessions:
        clickstream_data = DataLoader.delta_table_from_disk(
            parent_folder_path / "data/bronze/clickstream_data_data"
        )
        result = await active_sessions_last_15_minutes(clickstream_data).collect_async()

    return ORJSONResponse({"kpi": name.value, "value": result.item(0, 0)})
