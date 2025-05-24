from dataclasses import dataclass

from ._azure import BlobStorageClient
from ._file import DeltaTableLoader, DeltaTableWriter, FileDataLoader, FileDataWriter


@dataclass
class DataLoader(DeltaTableLoader, FileDataLoader):
    """Class to load & read data from variety of sources"""

    pass


@dataclass
class DataWriter(DeltaTableWriter, FileDataWriter):
    """Class to write data from variety of sources"""

    pass


__all__ = ["BlobStorageClient", "DataLoader", "DataWriter"]
