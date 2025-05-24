import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import polars as pl
import ujson
from adlfs import AzureBlobFileSystem
from deltalake.table import TableMerger

from src.exceptions import DataLoaderError, DataWriterError


@dataclass
class DeltaTableLoader:
    """Create a data pipeline to ingest Delta tables."""

    @classmethod
    def delta_table_from_disk(
        cls, path: str | Path, lazy: bool = True, **kwargs
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read delta table from disk

        Parameters
        ----------
        path : str | Path
            Disk location to read delta table from.
        lazy : bool, optional
            Load entire data at once or lazily. Defaults to True

        Note
        ----
        For all other options refer to
        1. Lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_delta.html
        2. Not lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.read_delta.html#polars.read_delta)

        Examples
        --------
        >>> from src.data import DataLoader

            # Reading delta table without any additional config
        >>> data = DataLoader.delta_table_from_disk("path/to/delta-table")

            # Using pyarrow_options params of polars dataframe & other additional params
        >>> pyarrow_options = {"parquet_read_options": {"coerce_int96_timestamp_unit": "ms"}}
        >>> data = DataLoader.delta_table_from_disk(
        ...     "path/to/delta-table",
        ...     pyarrow_options=pyarrow_options,
        ...     version=datetime(2020, 1, 1, tzinfo=timezone.utc)
        ... )

        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Complete data at once, if lazy is False. A lazy frame, if lazy is True.
        """
        path = path.as_posix() if isinstance(path, Path) else path
        return pl.scan_delta(path, **kwargs) if lazy else pl.read_delta(path, **kwargs)

    @classmethod
    def delta_table_from_blob(
        cls, blob_path: str, storage_option: dict[str, str], lazy: bool = True, **kwargs
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read delta table from azure blob storage

        Parameters
        ----------
        blob_path : str
            Blob path where delta table is located. Supported types of paths are:
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
            Storage options to read delta table from azure blob storage. See supported options
            at https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
        lazy : bool, optional
            Load entire data at once or lazily. Defaults to True.

        Note
        ----
        For all other options refer to
        1. Lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_delta.html
        2. Not lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.read_delta.html#polars.read_delta .

        Returns
        -------
        pl.LazyFrame | pl. DataFrame
            Complete data at once, if lazy is False. A lazy frame, if lazy is True.

        Example
        -------
        >>> from src.data import DataLoader
        >>> storage_options = {"account_name":"<storage_account_name>", "sas_token":"<token>"}

            # Reading delta table without any additional config
        >>> data = DataLoader.delta_table_from_blob(
        ...     "abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/delta-table",
        ...     storage_option=storage_option
        ... )

            # Using pyarrow_options params of polars dataframe & other additional params
        >>> pyarrow_options = {"parquet_read_options": {"coerce_int96_timestamp_unit": "ms"}}
            # NOTE – here account name must present in 'storage_option'
        >>> data = DataLoader.delta_table_from_blob(
        ...     "az://eds/testdir/akash/delta-table",
        ...     storage_option=storage_option,
        ...     pyarrow_options=pyarrow_options,
        ...     version=1
        ... )
        """
        return (
            pl.scan_delta(blob_path, storage_options=storage_option, **kwargs)
            if lazy
            else pl.read_delta(blob_path, storage_options=storage_option, **kwargs)
        )


@dataclass
class DeltaTableWriter:
    """Create a data pipeline to write Delta tables."""

    @property
    def get_delta_merge_methods(self) -> list[str]:
        return [
            method
            for method in dir(TableMerger)
            if not method.startswith("__")
            and method not in ["execute", "with_writer_properties"]
        ]

    @classmethod
    def delta_table_to_disk(cls, df: pl.DataFrame, path: str | Path, **kwargs) -> None:
        """Write delta table to disk

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to write to disk as delta table.
        path : str | Path
            Disk location to write delta table.

        Note
        ----
        For all other options refer to
        https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_delta.html

        Example
        -------
        >>> from src.data import DataWriter

            # write the table with 'overwrite' mode
        >>> DataWriter.delta_table_to_disk(df, "path/to/delta-table", mode="overwrite")
        """
        # NOTE - making sure mode is not set to 'merge'
        if "mode" in kwargs and kwargs["mode"] == "merge":
            raise DataWriterError(
                "'merge' not available here, instead use 'delta_table_merge_disk' method"
            )
        # NOTE - making sure storge options not set for azure blob storage
        if "storage_options" in kwargs:
            raise DataWriterError(
                "storage options not available here, instead use 'delta_table_to_blob' method"
            )

        df.write_delta(path, **kwargs)

    @classmethod
    def delta_table_to_blob(
        cls, df: pl.DataFrame, blob_path: str, storage_option: dict[str, str], **kwargs
    ):
        """Read delta table from azure blob storage

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to write as delta table to blob.
        blob_path : str
            Blob path where delta table is located. The following types of table paths are supported,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
            Storage options to read delta table from azure blob storage. See supported options
            at https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants

        Note
        ----
        For all other options refer to
        https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_delta.html

        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Complete data at once, if lazy is False. A lazy frame, if lazy is True.

        Example
        -------
        >>> from src.data import DataWriter
        >>> import pyarrow as pa

        >>> storage_options = {"account_name":"<storage_account_name>", "sas_token":"<token>"}
        >>> data = DataWriter.delta_table_to_blob(
        ...     df,
        ...     blob_path="abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/delta-table",
        ...     storage_option=storage_options,
        ...     mode="error"
        ... )

            # Using pyarrow_options params of polars dataframe & other additional params
            # NOTE – here account name must be present in 'storage_option'
        >>> data = DataWriter.delta_table_to_blob(
        ...     df, "az://eds/testdir/akash/delta-table", storage_option,
        ...     delta_write_options={"schema": pa.schema([pa.field("foo", pa.int64(), nullable=False)])}
        ... )
        ```
        """
        # NOTE - making sure mode is not set to 'merge'
        if "mode" in kwargs and kwargs["mode"] == "merge":
            raise DataWriterError(
                "'merge' not available here, instead use 'delta_table_merge_disk' method"
            )

        df.write_delta(blob_path, storage_options=storage_option, **kwargs)

    @classmethod
    def delta_table_merge_disk(
        cls,
        df: pl.DataFrame,
        path: str | Path,
        delta_merge_method_options: dict[str, dict],
        **kwargs,
    ) -> dict:
        """
        Merge current dataframe table with the delta table in the disk using `Delta Merge` ops.

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to write as delta table to disk.
        path : str | Path
            Disk location to merge table.
        delta_merge_method_options : dict[str, dict]
            Merge method options that will be used to merge the delta table.

        Note
        ----
        For all other options refer to
        https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_delta.html

        Returns
        -------
        Dict
            Merge operation results.

        Example
        -------
        >>> from src.data import DataWriter
        >>> merge_options = DataWriter.generate_delta_table_merge_method_options(
        ...     when_not_matched_insert_all=True, when_matched_update_all=True,
        ... )
        >>> data = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> results = DataWriter.delta_table_merge_disk(
        ...               data,
        ...               path="path/to/delta-table",
        ...               delta_merge_options={
        ...                   "predicate": source.x = target.x, # condition to determine upsert req
        ...                   "source_alias": "source",
        ...                   "target_alias": "target",
        ...               },
        ...               delta_merge_method_options=merge_options
        ...           )
        """
        # NOTE - making sure storage options not set for azure blob storage
        if "storage_options" in kwargs:
            raise DataWriterError(
                "storage options not available here, instead use 'delta_table_merge_blob' method"
            )
        available_merge_method = DeltaTableWriter().get_delta_merge_methods
        # validating merge method options
        for key in delta_merge_method_options:
            if key not in available_merge_method:
                raise DataWriterError(
                    f"invalid merge method: {key} \n"
                    f"valid methods are: {cls.get_delta_merge_methods}"
                )

        table_merger = df.write_delta(path, mode="merge", **kwargs)
        # crete merge option based on user input
        table_merger = cls._chain_merge_ops(table_merger, delta_merge_method_options)
        # execute the merge operation
        return table_merger.execute()

    @classmethod
    def delta_table_merge_blob(
        cls,
        df: pl.DataFrame,
        blob_path: str,
        storage_option: dict[str, str],
        delta_merge_method_options: dict[str, dict],
        **kwargs,
    ) -> dict:
        """
        Merge current delta table with the delta table in the blob storage using Delta Merge ops.

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to write as delta table to blob.
        blob_path : str
            Blob path where delta table is located. The following types of table paths are supported
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
            Storage options to read delta table from azure blob storage. See supported options
            at https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
        delta_merge_method_options : dict[str, dict]
            Merge method options that will be used to merge the delta table.

        Note
        ----
        For all other options refer to
        https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_delta.html


        Returns
        -------
        Dict
            Merge operation results.

        Example
        -------
        >>> from src.data import DataWriter
        >>> merge_options = DataWriter.generate_delta_table_merge_method_options(
            when_not_matched_insert_all=True, when_matched_update_all=True,
            )
        >>> storage_options = {"account_name":"<storage_account_name>", "sas_token":"<token>"}
        >>> data = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> results = DataWriter.delta_table_merge_blob(
        ...               data,
        ...               blob_path="abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/delta-table",
        ...               delta_merge_options={
        ...                   "predicate": source.x = target.x, # condition to determine upsert req
        ...                   "source_alias": "source",
        ...                   "target_alias": "target",
        ...               },
        ...               delta_merge_method_options=merge_options,
        ...               storage_options=storage_options
        ...           )
        """

        # validating merge method options
        available_merge_method = DeltaTableWriter().get_delta_merge_methods
        for key in delta_merge_method_options:
            if key not in available_merge_method:
                raise DataWriterError(
                    f"invalid merge method: {key} \n"
                    f"valid methods are: {cls.get_delta_merge_methods}"
                )

        table_merger = df.write_delta(
            blob_path, mode="merge", storage_options=storage_option, **kwargs
        )
        # crete merge option based on user input
        table_merger = cls._chain_merge_ops(table_merger, delta_merge_method_options)
        # execute the merge operation
        return table_merger.execute()

    @staticmethod
    def generate_delta_table_merge_method_options(
        when_matched_delete: bool = False,
        when_matched_update: bool = False,
        when_matched_update_params: dict | None = None,
        when_matched_update_all: bool = False,
        when_not_matched_by_source_delete: bool = False,
        when_not_matched_by_source_update: bool = False,
        when_not_matched_by_source_update_params: dict | None = None,
        when_not_matched_insert: bool = False,
        when_not_matched_insert_params: dict | None = None,
        when_not_matched_insert_all: bool = False,
    ):
        """
        Generate merge method options for delta table merge operation.

        Parameters
        ----------
        when_matched_delete : bool, optional
            Delete the row when predicate is matched. Defaults to False.
        when_matched_update : bool, optional
            Update the row when predicate is matched. Defaults to False.
        when_matched_update_params : dict, optional
            A mapping of column name to update SQL expression. Defaults to None.
        when_matched_update_all : bool, optional
            Update all the rows when predicate is matched. Defaults to False.
        when_not_matched_by_source_delete : bool, optional
            Delete the row when not source matches predicate. Defaults to False.
        when_not_matched_by_source_update : bool, optional
            Update the row when the source does not match predicate. Defaults to False.
        when_not_matched_by_source_update_params : dict, optional
            A mapping of column name to update SQL expression. Defaults to None.
        when_not_matched_insert : bool, optional
            Insert the row when predicate is not matched. Defaults to False.
        when_not_matched_insert_params : dict, optional
            A mapping of column name to insert SQL expression. Defaults to None.
        when_not_matched_insert_all : bool, optional
            Insert all the rows when not matched. Defaults to False.

        Returns
        -------
        dict
            Merge method options for delta table merge operation.

        Example
        -------
        >>> DataWriter.delta_table_merge_disk(
        ...    df,
        ...    "path/to/delta-table",
        ...    DataWriter.generate_delta_table_merge_method_options(
        ...            when_not_matched_insert_all=True,
        ...            when_matched_update=True,
        ...            when_matched_update_params={"x": "source.x", "`1y`": "source.`1y`"},
        ...        )
        ...    )
        """
        merge_method = {}
        if when_matched_delete:
            merge_method["when_matched_delete"] = {"predicate": None}
        if when_matched_update:
            if when_matched_update_params:
                merge_method["when_matched_update"] = {
                    "updates": when_matched_update_params
                }
            else:
                raise DataWriterError(
                    "when_matched_update_params is required when when_matched_update is True"
                )
        if when_matched_update_all:
            merge_method["when_matched_update_all"] = {"predicate": None}
        if when_not_matched_by_source_delete:
            merge_method["when_not_matched_by_source_delete"] = {"predicate": None}
        if when_not_matched_by_source_update:
            if when_not_matched_by_source_update_params:
                merge_method["when_not_matched_by_source_update"] = {
                    "updates": when_not_matched_by_source_update_params
                }
            else:
                raise DataWriterError(
                    "when_not_matched_by_source_update_params is required when "
                    "when_not_matched_by_source_update is True"
                )
        if when_not_matched_insert:
            if when_not_matched_insert_params:
                merge_method["when_not_matched_insert"] = {
                    "updates": when_not_matched_insert_params
                }
            else:
                raise DataWriterError(
                    "when_not_matched_insert_params is required when "
                    "when_not_matched_insert is True"
                )
        if when_not_matched_insert_all:
            merge_method["when_not_matched_insert_all"] = {"predicate": None}

        return merge_method

    @staticmethod
    def _chain_merge_ops(
        table_merge_obj: TableMerger, table_merge_mtd: dict[str, dict]
    ):
        # chain all user specified merge operations programmatically
        for op in table_merge_mtd:
            # getting method from TableMerger class
            method = getattr(table_merge_obj, op)
            # calling the method with user specified arguments
            table_merge_obj = method(**table_merge_mtd[op])
        return table_merge_obj


@dataclass
class FileDataLoader:
    """It enables you to create a data pipeline to ingest data."""

    @classmethod
    def csv_from_disk(
        cls, file_path: str | Path, lazy: bool = True, **kwargs
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read csv file from disk

        Parameters
        ----------
        file_path : str | Path
            Read csv file from.
        lazy : bool, optional
            Load entire data at once or lazily. Defaults to True.

         Note
        ----
        For all other options refer to
        1. Lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_csv.html
        2. Not lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.read_csv.html
        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Complete data at once, if lazy is False. A lazy frame, if lazy is True.

        Example
        -------
        >>> from src.data import DataLoader
        >>> data = DataLoader.csv_from_disk("path/to/xyz.csv")
        """
        if "storage_option" in kwargs:
            raise DataLoaderError(
                "storage_option is not supported for local csv files, "
                "use csv_from_blob instead"
            )
        return (
            pl.scan_csv(file_path, **kwargs)
            if lazy
            else pl.read_csv(file_path, **kwargs)
        )

    @classmethod
    def csv_from_blob(
        cls, blob_path: str, storage_option: dict[str, str], lazy: bool = True, **kwargs
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read csv file from azure blob storage

         Parameters
         ----------
        blob_path : str
             Blob path where csv is located. The following types of table paths are supported,
             az://<container>/<path>
             adl://<container>/<path>
             abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
             Storage options to read csv from azure blob storage. See supported options
             at https://github.com/fsspec/adlfs#details
        lazy : bool, optional
             Load entire data at once or lazily. Defaults to True.

        Note
        ----
        For all other options refer to
        1. Lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_csv.html
        2. Not lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.read_csv.html

        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Complete data at once, if lazy is False. A lazy frame, if lazy is True.

         Example
         -------
        >>> from src.data import DataLoader
        >>> data = DataLoader.csv_from_blob(
        ...     "abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/dummy.csv",storage_option
        ... )
             # Note – here account name must be present in 'storage_option'
        >>> data = DataLoader.csv_from_blob(
        ...     "az://eds/testdir/akash/dummy.csv", storage_option
        ... )
        """

        fs = AzureBlobFileSystem(**storage_option)
        if lazy:
            # first download the file to a temporary location
            temp_file = tempfile.mkstemp(suffix=".csv")
            fs.download(blob_path, temp_file[1])
            # using polars' lazy frame
            return pl.scan_csv(temp_file[1], **kwargs)

        # directly read the file from blob storage
        with fs.open(blob_path) as blob:
            return pl.read_csv(blob, **kwargs)

    @classmethod
    def json_from_disk(cls, file_path: str | Path) -> dict:
        """Read json file from disk


        Parameters
        ----------
        file_path : str | Path
            Read json file from given location on disk

        Returns
        -------
        dict
           json file data

        Example
        -------
        >>> from src.data import DataLoader
        >>> data = DataLoader.json_from_disk("path/to/xyz.json")
        """
        with open(file_path) as json_file:
            return ujson.load(json_file)

    @classmethod
    def json_from_blob(cls, blob_path: str, storage_option: dict[str, str]) -> dict:
        """Read json file from azure blob storage

        Parameters
        ----------
        blob_path : str
            Blob path where json is located. The following types of table paths are supported,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
            Storage options to read json from azure blob storage. See supported options
            at https://github.com/fsspec/adlfs#details

        Example
        -------
        >>> from src.data import DataLoader
        >>> d1 = DataLoader.json_from_blob(
        ...     "abfss://eds@devstoreaccount1.dfs.core.windows.net/us/testdir/akash/dummy.json", storage_option
        ... )
        >>> d2 = DataLoader.json_from_blob("az://eds/us/testdir/akash/dummy.json", storage_option)
        """
        fs = AzureBlobFileSystem(**storage_option)
        with fs.open(blob_path, "r") as f:
            return ujson.load(f)

    @classmethod
    def parquet_from_disk(
        cls, file_path: str | Path | list[str] | list[Path], lazy: bool = True, **kwargs
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read parquet file from disk

        Parameters
        ----------
        file_path : str | Path | list[str] | list[Path]
            Location of parquet file on disk.
        lazy : bool, optional
            Load entire data at once or lazily, by default True

        Note
        ----
        For all other options refer to
        1. Lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_parquet.html
        2. Not lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.read_parquet.html

        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Complete data at once, if lazy is False. A lazy frame, if lazy is True.

        Example
        -------
        >>> from src.data import DataLoader
        >>> data = DataLoader.parquet_from_disk("path/to/xyz.parquet")
            # with some other polars options
        >>> data = DataLoader.parquet_from_disk("path/to/xyz.parquet", lazy=False, low_memory=True)
        """
        if "storage_option" in kwargs:
            raise DataLoaderError(
                "storage_option is not supported for local parquet files, "
                "use parquet_from_blob instead"
            )
        return (
            pl.scan_parquet(file_path, **kwargs)
            if lazy
            else pl.read_parquet(file_path, **kwargs)
        )

    @classmethod
    def parquet_from_blob(
        cls,
        blob_path: str | Path | list[str] | list[Path],
        storage_option: dict,
        lazy: bool = True,
        **kwargs,
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read parquet file from azure blob storage

        Parameters
        ----------
        blob_path : str | Path | list[str] | list[Path]
            Blob path where parquet is located. The following types of table paths are supported,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict
            Storage options to read parquet from azure blob storage. See supported options
            at https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html
        lazy : bool, optional
            Load entire at once or lazily, by default True

        Note
        ----
        For all other options refer to
        1. Lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_parquet.html
        2. Not lazy: https://docs.pola.rs/py-polars/html/reference/api/polars.read_parquet.html

        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Load entire data at once, if lazy is False. A lazy frame, if lazy is True.

        Example
        -------
        >>> from src.data import DataLoader
        >>> data = DataLoader.parquet_from_blob(
        ...     "abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/dummy.parquet",storage_option
        ... )

            # with some other polars options
        >>> data = DataLoader.parquet_from_blob(
        ...     "abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/dummy.parquet", storage_option, lazy=False, low_memory=True
        ... )
        """
        return (
            pl.scan_parquet(blob_path, storage_options=storage_option, **kwargs)
            if lazy
            else pl.read_parquet(blob_path, storage_options=storage_option, **kwargs)
        )

    @classmethod
    def ndjson_from_disk(
        cls,
        file_path: str | Path,
        lazy: bool = True,
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read ndjson file from disk

        Parameters
        ----------
        file_path : str | Path
            Location of ndjson file on disk.
        lazy : bool, optional
            Load entire data at once or lazily, by default True

        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Load entire data at once, if lazy is False. A lazy frame, if lazy is True.

        Example
        -------
        >>> from src.data import DataLoader
        >>> data = DataLoader.ndjson_from_disk("path/to/xyz.ndjson")
        """
        return pl.scan_ndjson(file_path) if lazy else pl.read_ndjson(file_path)

    @classmethod
    def ndjson_from_blob(
        cls, blob_path: str, storage_option: dict, lazy: bool = True, **kwargs
    ) -> pl.LazyFrame | pl.DataFrame:
        """Read ndjson file from azure blob storage

        Parameters
        ----------
        blob_path : str
            Blob path where ndjson is located. The following types of table paths are supported,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict
            Storage options to read ndjson from azure blob storage. See supported options
            at https://github.com/fsspec/adlfs#details
        lazy : bool, optional
            Load entire data at once or lazily, by default True

        Returns
        -------
        pl.LazyFrame | pl.DataFrame
            Load entire data at once, if lazy is False. A lazy frame, if lazy is True.

        Example
        -------
        >>> from src.data import DataLoader
        >>> data = DataLoader.ndjson_from_blob("abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/dummy.ndjson",storage_option)
        """
        # first download the file to a temporary location
        temp_file = tempfile.mkstemp(suffix=".ndjson")
        fs = AzureBlobFileSystem(**storage_option)
        fs.download(blob_path, temp_file[1])

        return (
            pl.scan_ndjson(temp_file[1], **kwargs)
            if lazy
            else pl.read_ndjson(temp_file[1], **kwargs)
        )

    @classmethod
    def text_from_disk(
        cls, file_path: str | Path, lazy: bool = False
    ) -> str | Generator[str, None, None]:
        """
        Read and then stream content from text file

        Parameters
        ----------
        file_path : Union[str, Path]
            Path where the file is located
        lazy : bool, optional
            If True, then a generator will yield one data point at a time. If False, everything will be returned immediately. Defaults to False


        Returns
        -------
        str or Generator[str]
            Complete data at once, if lazy is False. A python generator that yields unit data point at a time, if lazy is True


        Examples
        --------
        >>> from src.data import DataLoader
            # this is entire string
        >>> sample_data_local = DataLoader.text_from_disk("tests/sample.txt")
            # this is a generator
        >>> sample_data_local = DataLoader.text_from_disk("tests/sample.txt", True)
        """

        def inner_lazy():
            with open(file_path) as data:
                yield from data

        if not lazy:
            return Path(file_path).read_text()
        return inner_lazy()

    @classmethod
    def ls_files_from_disk(
        cls, file_parent_path: str, pattern: str
    ) -> Generator[Path, None, None]:
        """Gives a list of all files based on file pattern


        Parameters
        ----------
        file_parent_path : str
            file path till the parent location
        pattern : str
            File pattern to locate all the files. This pattern supports all types of `glob` pattern.


        Yields:
            Generator[str, None, None]: generator with all the files discoverd based on file pattern


        Example
        -------
        >>> f = DataLoader.ls_files_from_disk("path/to/some/directory","*.json") # eg 1
        >>> f = DataLoader.ls_files_from_disk("/path/to/some/directory","**/*.json") # eg 2
        """
        return Path(file_parent_path).glob(pattern)


@dataclass
class FileDataWriter:
    """It enables you to create a data pipeline to write data."""

    @classmethod
    def csv_to_disk(cls, df: pl.DataFrame, save_path: str | Path, **kwargs):
        """Write dataframe to csv file


        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to be written to csv.
        save_path : str | Path
            Location to save csv file.


        Note
        ----
        For all other options refer to https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_csv.html


        Raises
        ------
        FileNotFoundError
            save_path must be a csv file


        Example
        -------
        >>> from src.data import DataWriter
        >>> DataWriter.csv_to_disk(df, "path/to/xyz.csv")


            # using polars' other options
        >>> DataWriter.csv_to_disk(df, "path/to/xyz.csv", delimiter=",", with_headers=False)
        """
        if Path(save_path).suffix != ".csv":
            raise FileNotFoundError("Given save path is not a csv file")

        df.write_csv(save_path, **kwargs)

    @classmethod
    def csv_to_blob(
        cls, df: pl.DataFrame, blob_path: str, storage_option: dict[str, str], **kwargs
    ):
        """Write dataframe to azure blob storage


        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to be written to azure blob storage
        blob_path : str
            Blob path to write csv file. Supported blob paths are,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
            Storage options to read delta table from azure blob storage. See supported options
            at https://github.com/fsspec/adlfs#details


        Note
        ----
        For all other options refer to https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_csv.html


        Example
        -------
        >>> from src.data import DataWriter
        >>> data = DataWriter.csv_to_blob(
        ...     "abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/delta-table", storage_option
        ... )


            # Note – here account name must be present in 'storage_option'
        >>> data = DataWriter.csv_to_blob("az://eds/testdir/akash/delta-table", storage_option)
        """
        if Path(blob_path).suffix != ".csv":
            raise FileNotFoundError("Given save path is not a csv file")

        fs = AzureBlobFileSystem(**storage_option)
        with fs.open(blob_path, "w") as blob:
            df.write_csv(blob, **kwargs)

    @classmethod
    def json_to_disk(cls, data: dict | list[dict], save_path: str | Path):
        """Write a dict as json file


        Parameters
        ----------
        data : dict | list[dict]
            Data to be saved as json
        save_path : str
            Location on disk to save json


        Example
        -------
        >>> from src.data import DataWriter
        >>> data = {"city": "mumbai", "country": IN}
        >>> DataWriter.json_to_disk(data, "path/to/xyz.json")
        """
        if Path(save_path).suffix != ".json":
            raise FileNotFoundError("Given save path is not a json file")

        # indent=4 will save json data in a nice formatted way
        with open(save_path, "w") as json_file:
            ujson.dump(data, json_file, indent=4)

    @classmethod
    def json_to_blob(
        cls, data: dict | list[dict], blob_path: str, storage_option: dict[str, str]
    ):
        """Write a dict as json file to azure blob storage


        Parameters
        ----------
        data : dict | list[dict]
            data to be saved as json
        blob_path : str
            Blob path to write json file. Supported blob paths are,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
            Storage options to read delta table from azure blob storage. See supported options
            at https://github.com/fsspec/adlfs#details


        Example
        -------
        >>> from src.data import DataWriter
        >>> data = {"city": "mumbai", "country": "IN"}
        >>> DataWriter.json_to_blob(
        ...     data, "abfss://eds@devstoreaccount1.dfs.core.windows.net/testdir/akash/dummy.json", storage_option
        ... )
        """
        fs = AzureBlobFileSystem(**storage_option)
        with fs.open(blob_path, "w") as blob:
            ujson.dump(data, blob, indent=4)

    @classmethod
    def ndjson_to_disk(cls, df: pl.DataFrame, save_path: str | Path):
        """Write dataframe to ndjson file


        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to be written to ndjson.
        save_path : str | Path
            Location to save ndjson file.


        Example
        -------
        >>> from src.data import DataWriter
        >>> DataWriter.ndjson_to_disk(df, "path/to/xyz.ndjson")
        """
        if Path(save_path).suffix != ".ndjson":
            raise FileNotFoundError("Given save path is not a ndjson file")

        df.write_ndjson(save_path)

    @classmethod
    def ndjson_to_blob(
        cls, df: pl.DataFrame, blob_path: str, storage_option: dict[str, str]
    ):
        """Write dataframe to ndjson file to azure blob storage


        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to be written to ndjson.
        blob_path : str
            Blob path to write ndjson file. Supported blob paths are,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict[str, str]
            Storage options to read delta table from azure blob storage. See supported options
            at https://github.com/fsspec/adlfs#details


        Example
        -------
        >>> from src.data import DataWriter
        >>> DataWriter.ndjson_to_blob(df, "az://eds/testdir/akash/dummy.ndjson", storage_option)
        """
        fs = AzureBlobFileSystem(**storage_option)
        with fs.open(blob_path, "wb") as blob:
            df.write_ndjson(blob)

    @classmethod
    def parquet_to_disk(cls, df: pl.DataFrame, save_path: str | Path, **kwargs):
        """Write dataframe to parquet file


        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to be written to parquet.
        save_path : str | Path
            Location to save parquet file.


        Note
        ----
        For all other options refer to https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_parquet.html


        Example
        -------
        >>> from src.data import DataWriter
        >>> DataWriter.parquet_to_disk(df, "path/to/xyz.parquet")


            # using polars' other options
        >>> DataWriter.parquet_to_disk(
        ...     df, "path/to/xyz.parquet", compression="snappy", use_pyarrow=False
        ... )
        """
        if "storage_option" in kwargs:
            raise DataWriterError(
                "storage_option is not supported for local parquet files, "
                "use parquet_to_blob instead"
            )
        df.write_parquet(save_path, **kwargs)

    @classmethod
    def parquet_to_blob(
        cls, df: pl.DataFrame, blob_path: str, storage_option: dict, **kwargs
    ):
        """Write dataframe to parquet file to azure blob storage


        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to be written to parquet.
        blob_path : str
            Blob path to write parquet file. Supported blob paths are,
            az://<container>/<path>
            adl://<container>/<path>
            abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
        storage_option : dict
            Storage options to read parquet from azure blob storage. See supported options
            at https://github.com/fsspec/adlfs#details


        Example
        -------
        >>> from src.data import DataWriter
        >>> DataWriter.parquet_to_blob(df, "az://eds/testdir/akash/dummy.parquet", storage_option)


            # using polars' other options
        >>> DataWriter.parquet_to_blob(
        ...     df, "az://eds/testdir/akash/dummy.parquet", storage_option, compression="snappy", use_pyarrow=False
        ... )
        """
        fs = AzureBlobFileSystem(**storage_option)
        with fs.open(blob_path, "wb") as blob:
            df.write_parquet(blob, **kwargs)

    @classmethod
    def str_to_txt(cls, data: str, save_path: str):
        """Write string data to a txt file


        Args:
            data (str): text/string data to be written
            save_path (str): path to save txt file


        Raises:
            FileNotFoundError: save_path must be txt file
        """
        if Path(save_path).suffix != ".txt":
            raise FileNotFoundError("Given save path is not a txt file")

        Path(save_path).write_text(data)

    @classmethod
    def str_append_to_txt(
        cls, data: str, sep: str = "\n", save_path: str | None = None
    ):
        """Write string data to a txt file in appending mode


        Args:
            data (str): text/string data to be appended
            sep (str, optional): a separator between two data point. Defaults to "/n".
            save_path (str): path to save txt file
        Raises:
            FileNotFoundError: save_path must be txt file
        """
        if Path(save_path).suffix != ".txt":
            raise FileNotFoundError("Given save path is not a txt file")

        with open(save_path, "a") as append_file:
            append_file.write(data + sep)
