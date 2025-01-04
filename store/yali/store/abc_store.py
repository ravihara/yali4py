import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Annotated, Any, AsyncGenerator, Callable, List, Literal, Tuple, Union

import urllib3
from minio.credentials.providers import Provider as CredentialsProvider
from pydantic import Field, SecretStr, field_serializer
from pydantic_settings import BaseSettings, SettingsConfigDict

from yali.core.typings import (
    BytesIO,
    ErrorOrBytesIO,
    ErrorOrStr,
    FlexiTypesModel,
    NonEmptyStr,
)

## NOTE: BulkPutEntry is used for `put_objects`
# It is a tuple of (key, data, overwrite)
BulkPutEntry = Tuple[str, BytesIO, bool]


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file_encoding="utf-8", case_sensitive=True)

    max_object_cache_size: int = Field(128, ge=1)
    max_object_cache_ttl: int = Field(300, ge=10)
    max_storage_concurrancy: int = Field(16, ge=1)
    stores_config_fpath: str = Field(
        "~/.yali/stores.yaml", validation_alias="YALI_STORES_CONFIG_FPATH"
    )


__storage_settings: StorageSettings | None = None


def storage_settings():
    global __storage_settings

    if __storage_settings is None:
        __storage_settings = StorageSettings()

    return __storage_settings


class UnixFsStoreConfig(FlexiTypesModel):
    stype: Literal["unix-fs"] = "unix-fs"
    sroot: NonEmptyStr
    is_readonly: bool = False

    @field_serializer("sroot", when_used="unless-none")
    @staticmethod
    def sroot_serializer(sroot: NonEmptyStr):
        if not sroot.startswith("/"):
            raise ValueError(f"Store's root folder path must be absolute: {sroot}")

        if sroot.endswith("/"):
            raise ValueError(
                f"Store's root folder path must not end with a slash: {sroot}"
            )

        return sroot


class AwsS3StoreConfig(FlexiTypesModel):
    stype: Literal["aws-s3"] = "aws-s3"
    endpoint: NonEmptyStr = "s3.amazonaws.com"
    tls_enabled: bool = True
    check_certs: bool = True
    region: str | None = None
    session_token: str | None = None
    pooled_http: urllib3.PoolManager | None = None
    creds_provider: CredentialsProvider | None = None
    bucket_name: NonEmptyStr
    access_key: NonEmptyStr
    secret_key: SecretStr

    @field_serializer("secret_key", when_used="unless-none")
    @staticmethod
    def secret_key_serializer(secret_key: SecretStr):
        return secret_key.get_secret_value()


class AzureBlobStoreConfig(FlexiTypesModel):
    stype: Literal["azure-blob"] = "azure-blob"
    container_name: NonEmptyStr
    connection_string: SecretStr

    @field_serializer("connection_string", when_used="unless-none")
    @staticmethod
    def connection_string_serializer(connection_string: SecretStr):
        return connection_string.get_secret_value()


StoreConfig = Annotated[
    Union[UnixFsStoreConfig, AwsS3StoreConfig, AzureBlobStoreConfig],
    Field(discriminator="stype"),
]


class AbstractStore(ABC):
    _logger = logging.getLogger(__name__)

    @abstractmethod
    def __init__(self, config: StoreConfig, aio_loop: asyncio.AbstractEventLoop):
        self._aio_loop = aio_loop
        self._settings = storage_settings()
        self._store_id = self._gen_store_id(config=config)
        self._thread_executor = ThreadPoolExecutor(
            max_workers=self._settings.max_storage_concurrancy,
            thread_name_prefix=f"store-{self._store_id}",
        )

    @property
    def store_id(self):
        return self._store_id

    def _gen_store_id(self, config: StoreConfig):
        config_str = config.model_dump_json(exclude_none=True)
        return hashlib.md5(config_str.encode("utf-8")).hexdigest()

    def object_basename(self, key: str) -> str:
        return key.split("/")[-1]

    def object_dirname(self, key: str) -> str:
        return "/".join(key.split("/")[:-1])

    def add_thread_pool_task(self, func: Callable, *fargs, **fkwargs):
        wrapped_fn = partial(func, *fargs, **fkwargs)
        return self._aio_loop.run_in_executor(self._thread_executor, wrapped_fn)

    @abstractmethod
    def object_store_path(self, key: str) -> str:
        """Compute the path of the object in the store."""
        raise NotImplementedError()

    @abstractmethod
    async def close(self):
        """Close the store."""
        await asyncio.sleep(0)
        self._thread_executor.shutdown(wait=True)
        self._thread_executor = None

    @abstractmethod
    async def get_object(self, key: str) -> ErrorOrBytesIO:
        """Read the object from the store."""
        raise NotImplementedError()

    @abstractmethod
    async def put_object(
        self, key: str, data: BytesIO, overwrite: bool = False
    ) -> ErrorOrStr:
        """Write the object to the store."""
        raise NotImplementedError()

    @abstractmethod
    async def delete_object(self, key: str) -> ErrorOrStr:
        """Delete the object from the store."""
        raise NotImplementedError()

    @abstractmethod
    async def get_objects(self, keys: List[str]) -> AsyncGenerator[ErrorOrBytesIO, Any]:
        """
        Read the objects from the store.

        Parameters
        ----------
        keys : List[str]
            The keys of the objects to read.

        Returns
        -------
        AsyncGenerator[ErrorOrBytesIO, Any]
            The objects read from the store.
        """
        raise NotImplementedError()

    @abstractmethod
    async def put_objects(
        self, entries: List[BulkPutEntry]
    ) -> AsyncGenerator[ErrorOrStr, Any]:
        """
        Write the objects to the store.

        Parameters
        ----------
        entries : List[BulkPutEntry]
            The objects to write.

        Returns
        -------
        AsyncGenerator[ErrorOrStr, Any]
            The results of the writes.
        """
        raise NotImplementedError()

    @abstractmethod
    async def delete_objects(self, keys: List[str]) -> AsyncGenerator[ErrorOrStr, Any]:
        """
        Delete the objects from the store.

        Parameters
        ----------
        keys : List[str]
            The keys of the objects to delete.

        Returns
        -------
        AsyncGenerator[ErrorOrStr, Any]
            The results of the deletes.
        """
        raise NotImplementedError()

    @abstractmethod
    async def object_exists(self, key: str) -> bool:
        """Check if the object exists in the store."""
        raise NotImplementedError()

    @abstractmethod
    async def total_objects(self) -> int:
        """Get the total number of objects in the store."""
        raise NotImplementedError()
