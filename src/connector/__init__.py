from dataclasses import dataclass

import polars as pl

from ._azure import BlobStorageClient
from ._file import DeltaTableLoader, DeltaTableWriter, FileDataLoader, FileDataWriter
from ._kafka import consume_message, publish_message


@dataclass
class DataLoader(DeltaTableLoader, FileDataLoader):
    """Class to load & read data from variety of sources"""

    @classmethod
    def message_from_topic(cls, topic: str) -> pl.DataFrame:
        """Read all the messages currently present in given kafka topic

        Parameters
        ----------
        topic : str
            name of kafka topic to read messages

        Returns
        -------
        pl.DataFrame
            all messages as dataframe
        """
        return consume_message(topic)


@dataclass
class DataWriter(DeltaTableWriter, FileDataWriter):
    """Class to write data from variety of sources"""

    @classmethod
    def json_to_topic(cls, df: pl.DataFrame, topic: str, key_name: str | None):
        """Publishes json/table like data data to given kafka topic

        Parameters
        ----------
        df : pl.DataFrame
            table like data to publish to kafka topic
        topic : str
            name of topic to publish DEPRECATED -
        key_name : str | None
            name of key (if present in data itself)
        """
        publish_message(df, topic, key_name)


__all__ = ["BlobStorageClient", "DataLoader", "DataWriter"]
