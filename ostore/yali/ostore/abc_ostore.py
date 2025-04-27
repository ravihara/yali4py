import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from io import BytesIO
from typing import Annotated, Any, AsyncGenerator, Callable, List, Literal, Tuple

import urllib3
from minio.credentials.providers import Provider as CredentialsProvider

from yali.core.codecs import JSONNode
from yali.core.errors import ErrorOrBytesIO, ErrorOrStr
from yali.core.models import BaseModel, field_specs
from yali.core.settings import env_config
from yali.core.typebase import Constraint, NonEmptyStr, SecretStr

_env_config = env_config()

## NOTE: BulkPutEntry is used for `put_objects`
# It is a tuple of (key, data, overwrite)
BulkPutEntry = Tuple[str, BytesIO, bool]

CacheSizeInt = Annotated[int, Constraint.as_number(ge=1)]
CacheTTLInt = Annotated[int, Constraint.as_number(ge=10)]
StorageConcurrancyInt = Annotated[int, Constraint.as_number(ge=1)]


class StorageSettings(BaseModel):
    max_object_cache_size: CacheSizeInt = field_specs(default=128)
    max_object_cache_ttl: CacheTTLInt = field_specs(default=300)
    max_storage_concurrancy: StorageConcurrancyInt = field_specs(default=16)
    stores_config_fpath: str = _env_config(
        "YALI_STORES_CONFIG_FPATH", default="~/.yali/stores.yaml"
    )


__storage_settings: StorageSettings | None = None


def storage_settings():
    global __storage_settings

    if __storage_settings is None:
        __storage_settings = StorageSettings()

    return __storage_settings


class UnixFsStoreConfig(BaseModel):
    stype: Literal["unix-fs"] = "unix-fs"
    sroot: NonEmptyStr
    is_readonly: bool = False

    def __post_init__(self):
        if not self.sroot.startswith("/"):
            raise ValueError(f"Store's root folder path must be absolute: {self.sroot}")

        if self.sroot.endswith("/"):
            raise ValueError(
                f"Store's root folder path must not end with a slash: {self.sroot}"
            )


class AwsS3StoreConfig(BaseModel):
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


class AzureBlobStoreConfig(BaseModel):
    stype: Literal["azure-blob"] = "azure-blob"
    container_name: NonEmptyStr
    connection_string: SecretStr


StoreConfig = UnixFsStoreConfig | AwsS3StoreConfig | AzureBlobStoreConfig


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
        config_bytes = JSONNode.dump_bytes(data=config)
        return hashlib.md5(config_bytes).hexdigest()

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
