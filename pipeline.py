import logging
from datetime import datetime, timezone

import polars as pl

from src.ops.generator import (
    generate_dummy_clickstream_event,
    generate_dummy_partner_agency_booking,
)
from src.ops.publisher import Publisher

logger = logging.getLogger("data-pipeline")
now = datetime.now(timezone.utc)
# Step 1: Generating fake/dummy data

logger.info("generating dummy data for partner agency booking")
# generating 50 records
dummy_partner_agency = pl.concat(
    [
        # NOTE - using polars LazyFrame to save memory till it's actual required
        pl.LazyFrame(generate_dummy_partner_agency_booking().model_dump())
        for _ in range(50)
    ],
    how="vertical_relaxed",
)

logger.info("generating dummy data for clickstream")
# generating 50 records
dummy_clickstream = pl.concat(
    [
        # NOTE - using polars LazyFrame to save memory till it's actual required
        pl.LazyFrame(
            generate_dummy_clickstream_event(allow_duplicates=True).model_dump()
        )
        for _ in range(50)
    ],
    how="vertical_relaxed",
)

# Step 2: Publishing data
logger.info("publishing partner agency booking data")
partner_agency_publisher = Publisher(dummy_partner_agency.collect())
# csv
partner_agency_publisher.csv_to_azure(
    f"abfs://dummy-container/bronze/csv/partner-agency-{now}.csv"
)
# parquet
partner_agency_publisher.parquet_to_azure(
    f"abfs://dummy-container/bronze/parquet/partner-agency-{now}.parquet"
)
# postgres db
partner_agency_publisher.data_to_postgres("pipeline.partner_agency_booking")

logger.info("publishing partner clickstream data")
clickstream_publisher = Publisher(dummy_clickstream.collect())
# postgres db
clickstream_publisher.data_to_postgres("pipeline.partner_agency_booking")
