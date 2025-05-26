import logging
import os
from dataclasses import dataclass

import polars as pl
from adbc_driver_postgresql.dbapi import connect

from src.config import get_config
from src.connector import DataLoader

logger = logging.getLogger("data-pipeline")
config = get_config(os.getenv("ENV", "local"))


@dataclass
class Consumer:
    """Class to consume data from various destinations"""

    def __post_init__(self):
        self._storage_option = {
            "account_name": config.azure_storage_account_name,
            "connection_string": config.AZURITE_CONNECTION_STRING,
        }

    def get_latest_file_from_azure(self, path: str) -> str:
        """Get latest file based on path regex pattern

        Parameters
        ----------
        path : str
            file path regex pattern

        Returns
        -------
        str
            path of latest created file
        """
        files = DataLoader.ls_files_from_blob(path, self._storage_option)
        files.sort()
        return files[-1]

    def read_csv_from_azure(self, blob_path: str) -> pl.LazyFrame:
        """Read CSV data from Azure Blob Storage

        Parameters
        ----------
        blob_path : str
            path of csv file located in azure blob storage

        Returns
        -------
        pl.LazyFrame
            data as polars LazyFrame
        """
        logger.debug(f"Reading CSV data from Azure Blob Storage at {blob_path}")
        return DataLoader.csv_from_blob(blob_path, self._storage_option)

    def read_parquet_from_azure(self, blob_path: str) -> pl.LazyFrame:
        """Read parquet data from Azure Blob Storage

        Parameters
        ----------
        blob_path : str
            path of parquet file located inn azure blob storage

        Returns
        -------
        pl.LazyFrame
            data as polars LazyFrame
        """
        logger.debug(f"Reading parquet data from Azure Blob Storage at {blob_path}")
        return DataLoader.parquet_from_blob(blob_path, self._storage_option)

    def read_messages_from_kafka(self, topic: str) -> pl.LazyFrame:
        """Read messages from Kafka topic

        Parameters
        ----------
        topic : str
            name of kafka topic to consume messages

        Returns
        -------
        pl.LazyFrame
            data as polars LazyFrame
        """
        logger.debug(f"Reading messages from Kafka topic {topic}")
        return DataLoader.message_from_topic(topic).lazy()

    def read_data_from_postgres(self, query: str) -> pl.LazyFrame:
        """Read table data from postgresql db

        Parameters
        ----------
        query : str
            SQL query to fetch data from table

        Returns
        -------
        pl.LazyFrame
            data as polars LazyFrame
        """
        with connect(
            f"postgresql://{config.postgres_username}:{config.POSTGRES_PASSWORD}@localhost:{config.postgres_port}/playground"
        ) as conn:
            logger.debug("Reading table data from postgresql table")
            return pl.read_database(query=query, connection=conn).lazy()
