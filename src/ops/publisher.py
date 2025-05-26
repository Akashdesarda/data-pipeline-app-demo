import logging
import os
from dataclasses import dataclass

import polars as pl
from adbc_driver_postgresql.dbapi import connect

from src.config import get_config
from src.connector import DataWriter

logger = logging.getLogger("data-pipeline")
config = get_config(os.getenv("ENV", "local"))


@dataclass
class Publisher:
    """Class to publish data to various destinations"""

    data: pl.DataFrame

    def __post_init__(self):
        self._storage_option = {
            "account_name": config.azure_storage_account_name,
            "connection_string": config.AZURITE_CONNECTION_STRING,
        }

    def csv_to_azure(self, blob_path: str):
        """Publish data to Azure Blob Storage as CSV

        Parameters
        ----------
        blob_path : str
            azure container storage path to save as csv file
        """
        DataWriter.csv_to_blob(self.data, blob_path, self._storage_option)
        logger.debug(f"successfully saved csv file to azure at {blob_path}")

    def parquet_to_azure(self, blob_path: str):
        """Publish data to Azure Blob Storage as parquet

        Parameters
        ----------
        blob_path : str
            azure container storage path to save as parquet file
        """
        DataWriter.parquet_to_blob(self.data, blob_path, self._storage_option)
        logger.debug(f"successfully saved parquet file to azure at {blob_path}")

    def message_to_kafka(self, topic: str, key_name: str | None):
        """Publish data to Kafka topic

        Parameters
        ----------
        topic : str
            kafka topic name to publish data as messages
        key_name : str | None
            key name to whose value need to be published as kry
        """
        DataWriter.json_to_topic(self.data, topic, key_name)
        logger.debug(f"successfully published messages to kafka topic {topic}")

    def data_to_postgres(self, table: str):
        """Writes data to given table in postgresql db

        Parameters
        ----------
        table : str
            table name to write data
        """
        with connect(
            f"postgresql://{config.postgres_username}:{config.POSTGRES_PASSWORD}@localhost:{config.postgres_port}/playground"
        ) as conn:
            self.data.write_database(
                table_name=table, connection=conn, if_table_exists="append"
            )
        logger.debug(f"successfully written data to postgresql table {table}")
