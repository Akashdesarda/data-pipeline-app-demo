import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from azure.storage.blob import (
    BlobClient,
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)
from joblib import Parallel, delayed

logger = logging.getLogger("data-pipeline")


@dataclass
class BlobStorageClient:
    """
    This class packs all utilities to work with any required Azure storage account's Blob resource.

    Parameters
    ----------
    connection_string : str | None, optional
        Connection string to connect respective resource. If it is used, then auth_token and storage_account_name can be skipped. Defaults to None.
    auth_token : str | AzureSasCredential | AzureNamedKeyCredential | ClientSecretCredential | None, optional
        Any of the azure auth token to connect the respective resource. If it used then connection_string can be skipped but storage_account_name must be used. Defaults to None.
    storage_account_name : str | None, optional
        Name of storage account. Must be used if auth_token is used. Defaults to None.

    Raises
    ------
    ValueError
        Either connection_string or auth_token must be provided
        If auth_token is used than storage_account_name must be provided

    Examples
    --------
    >>> from kroll.identity import GenerateSASToken, ResourceTypesTemplate, SASPermissionsTemplate
    >>> from src.connector import BlobStorageClient

        # Connection string
    >>> blob_utility = BlobStorageClient(connection_string="***")

        # auth token using sas key
    >>> res = ResourceTypesTemplate("blob", service=True, container=False)
    >>> per = SASPermissionsTemplate("blob", read=True, write=True, permanent_delete=True)
    >>> gen_sas = GenerateSASToken("dev-ussc-data-key")
    >>> sas_key = gen_sas.generate_sas_token_for_blob("dummy-st-account","dummy-st-account-key",res,per,)
    >>> blob_utility = BlobStorageClient(auth_token=sas_key, storage_account_name='dummy-st-account')
    """

    connection_string: str
    # NOTE - In real world, it is better to use below auth_token based authentication instead of connection_string
    # auth_token: (
    #     str
    #     | AzureSasCredential
    #     | AzureNamedKeyCredential
    #     | ClientSecretCredential
    #     | None
    # ) = None
    storage_account_name: str

    def __post_init__(self):
        # NOTE - Below commented code should be used when other auth method is available to use
        # if (self.connection_string is None) & (self.auth_token is None):
        #     raise ValueError("Either connection_string or auth_token must be provided")
        # if (self.auth_token is not None) & (self.storage_account_name is None):
        #     raise ValueError(
        #         "If auth_token is used than storage_account_name must be provided"
        #     )
        # if self.connection_string:
        logger.warning(
            "Using Connection string for authentication. "
            "It's better to use Auth token (e.g. SAS token)"
        )
        self._blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        # else:
        #     self._blob_service_client = BlobServiceClient(
        #         account_url=f"https://{self.storage_account_name}.blob.core.windows.net",
        #         credential=self.auth_token,
        #     )

    @property
    def account_name(self) -> str:
        """
        Get storage account name

        Returns
        -------
        str
            The name of the storage account
        """
        return self._blob_service_client.account_name

    @property
    def blob_service_client(self) -> BlobServiceClient:
        """
        Get the blob service client

        Returns
        -------
        BlobServiceClient
            The blob service client
        """
        return self._blob_service_client

    def complete_container_utility(self, container_name: str) -> ContainerClient:
        """
        Create container client for interacting with complete given container

        Parameters
        ----------
        container_name : str
            Name of the container

        Returns
        -------
        ContainerClient
            Client of a given container

        Examples
        --------
        >>> from src.connector import BlobStorageClient
        >>> blob_utility = BlobStorageClient(auth_token=***, storage_account_name='dummy-st-account')
        >>> container = blob_utility.complete_container_utility(container_name="bronze")
        """
        return self._blob_service_client.get_container_client(container_name)

    def complete_blob_tree(
        self, container_name: str, blob_path: str
    ) -> Iterator[BlobProperties]:
        """
        Complete absolute path tree of every blob present in given container

        Parameters
        ----------
        container_name : str
            Name of the container
        blob_path : str
            Complete path to return walk the blob tree

        Yields
        ------
        Iterator[BlobProperties]
            'Walk the blob tree' functionality in lazy form

        Examples
        --------
        >>> from src.connector import BlobStorageClient
        >>> blob_utility = BlobStorageClient(auth_token="***", storage_account_name='dummy-st-account')
        >>> container = blob_utility.complete_blob_tree(container_name="bronze", "bronze"'products/some-folder/')
        """
        container = self.complete_container_utility(container_name)
        return container.list_blobs(blob_path)

    def unit_blob_utility(self, absolute_blob_path: str) -> BlobClient:
        """
        Create a client to interact with a specific blob

        Parameters
        ----------
        absolute_blob_path : str
            Absolute path of blob that starts with the name of container

        Returns
        -------
        BlobClient
            Blob client to interact with

        Examples
        --------
        >>> from src.connector import BlobStorageClient
        >>> blob_utility = BlobStorageClient(auth_token="***", storage_account_name='dummy-st-account')
        >>> container = blob_utility.unit_blob_utility(absolute_blob_path='curated/product/xyz.csv')
        """
        return (
            BlobClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=str(Path(absolute_blob_path).parent),
                blob_name=Path(absolute_blob_path).name,
            )
            if self.connection_string is not None
            else BlobClient(
                account_url=f"https://{self.storage_account_name}.blob.core.windows.net",
                container_name=str(Path(absolute_blob_path).parent),
                blob_name=Path(absolute_blob_path).name,
                credential=self.auth_token,
            )
        )

    def download_blob(
        self,
        absolute_blob_path: str,
        disk_path: str | None = None,
    ) -> None | StorageStreamDownloader:
        """
        Download a blob or load its content into an object present at Storage account

        Parameters
        ----------
        absolute_blob_path : str
            Absolute path of blob that starts with the name of container
        disk_path : str | None
            Path on disk to download, to be used only when the requirement is to download blob. Defaults to None

        Returns
        -------
        None | StorageStreamDownloader
            If `disk_path` is not None then blob just downloaded, if `disk_path` is None then `StorageStreamDownloader` is return that can be used an object

        Examples
        --------
        >>> from src.connector import BlobStorageClient
        >>> blob_utility = BlobStorageClient(auth_token="***", storage_account_name='dummy-st-account')
            # just downloading
        >>> blob_utility.download_blob(
        ...     absolute_blob_path="container_name/path/to/some_file.csv",
        ...     disk_path="/path/to/some_file_on_disk.csv",
        ... )

            # load content into an object
        >>> file = json.loads(blob_utility.download_blob("container_name/path/to/some_file.json"))
        """
        # making a connection with blob
        blob = self.unit_blob_utility(absolute_blob_path=absolute_blob_path)

        if isinstance(disk_path, str):
            # creating required folder if absent
            Path(disk_path).parent.mkdir(parents=True, exist_ok=True)
            with open(disk_path, "wb") as blob_file:
                # saving to disk
                blob_file.write(blob.download_blob().readall())
        else:
            logger.warning("Make sure you have enough space available in memory")
            return blob.download_blob().readall()  # load as bytes contents to an object

    def upload_blob(
        self,
        absolute_blob_path: str,
        data: bytes | str,
        overwrite: bool = False,
        metadata: dict[str, str] | None = None,
    ):
        """
        Upload blob to specified location at Storage account. If directly passing `bytes object` then do not specify `disk_path`

        Parameters
        ----------
        absolute_blob_path : str
            Absolute path of blob that starts with the name of container
        data : bytes | str
            Data to be uploaded. It can a direct bytes object or can be a file path on disk
        overwrite : bool, optional
            Overwrite if already exists. Defaults to False.
        metadata : Dict[str, str], optional
            Add new custom metadata. Defaults to None

        Examples
        --------
        >>> from src.connector import BlobStorageClient
        >>> blob_utility = BlobStorageClient(auth_token="***", storage_account_name='dummy-st-account')
            # uploading a file present on disk
        >>> blob_utility.upload_blob(
        >>>     absolute_blob_path="container_name/path/to/some_file.csv",
        >>>     data="path/to/some_file_on_disk.csv",
        >>>     overwrite=True
        >>> )

            # uploading data already present & read as bytes
        >>> blob_utility.upload_blob(
        >>>     absolute_blob_path="container_name/path/to/some_file.csv",
        >>>     data=bytes_data,
        >>>     overwrite=True
        >>> )
        """
        # making a connection with blob where file will be uploaded
        blob = self.unit_blob_utility(absolute_blob_path=absolute_blob_path)
        if isinstance(data, bytes):
            blob.upload_blob(data, overwrite=overwrite, metadata=metadata)
        # uploading file to storage account
        elif isinstance(data, str):
            with open(data, "rb") as blob_file:
                blob.upload_blob(blob_file, overwrite=overwrite, metadata=metadata)

    def bulk_download(self, blob_tree: Iterator, disk_directory_path: str):
        """
        Download multiple blob files in bulk.

        Parameters
        ----------
        blob_tree : Iterator
            An iterator that contains the list of absolute paths of all blobs to be downloaded.
        disk_directory_path : str
            The path of the directory where all blobs must be downloaded.

        Examples
        --------
        >>> from src.connector import BlobStorageClient
        >>> blob_utility = BlobStorageClient(auth_token="***", storage_account_name='dummy-st-account')
        >>> blob_tree = blob_utility.complete_blob_tree(
        ...    container_name="bronze", "bronze"'products/purchase-price-allocation/'
        ... )
        >>> blob_utility.bulk_download(blob_tree, "some/directory")
        """
        Path(disk_directory_path).mkdir(parents=True, exist_ok=True)
        Parallel(n_jobs=-1, backend="threading")(
            delayed(self.download_blob)(
                unit_blob_tree, f"{disk_directory_path}/{unit_blob_tree.name}"
            )
            for unit_blob_tree in blob_tree
        )

    def bulk_upload(
        self,
        disk_path_tree: Iterator,
        absolute_blob_path_tree: Iterator,
        overwrite: bool = False,
    ):
        """
        Upload blobs to specified location in bulk.

        Parameters
        ----------
        disk_path_tree : Iterator
            An iterator that contains the disk paths of all files to be uploaded.
        absolute_blob_path_tree : Iterator
            An iterator that contains the absolute blob paths for respective blobs to be uploaded.
        overwrite : bool, optional
            If True, overwrite if the blob already exists. Defaults to False.

        Examples
        --------
        >>> from src.connector import BlobStorageClient
        >>> blob_utility = BlobStorageClient(auth_token="***", storage_account_name='dummy-st-account')
        >>> disk_tree = list(Path("some/directory").glob("*.csv"))
        >>> abs_blob_tree = [f"curated/products/{file.name}" for file in disk_tree]
        >>> blob_utility.bulk_upload(disk_tree, abs_blob_tree)
        """
        Parallel(n_jobs=-1, backend="threading")(
            delayed(self.upload_blob)(abs_blob_path, disk_path, overwrite)
            for (abs_blob_path, disk_path) in zip(
                absolute_blob_path_tree, disk_path_tree
            )
        )
