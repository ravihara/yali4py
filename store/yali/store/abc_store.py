import hashlib

from abc import ABC, abstractmethod
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Union, Annotated, Tuple, List
from yali.core.typings import FlexiTypesModel, NonEmptyStr
from pydantic import SecretStr, field_serializer, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

## NOTE: BulkObjectEntry is used for `put_objects`
# It is a tuple of (key, data, overwrite)
BulkObjectEntry = Tuple[str, bytes, bool]


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

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


class FsMountStoreConfig(FlexiTypesModel):
    stype: Literal["fs-mount"] = "fs-mount"
    mount_point: NonEmptyStr


class AwsS3StoreConfig(FlexiTypesModel):
    stype: Literal["aws-s3"] = "aws-s3"
    endpoint: NonEmptyStr = "s3.amazonaws.com"
    tls_enabled: bool = True
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
    Union[FsMountStoreConfig, AwsS3StoreConfig, AzureBlobStoreConfig], Field(discriminator="stype")
]


class AbstractStore(ABC):
    _logger = logging.getLogger(__name__)

    @abstractmethod
    def __init__(self, config: StoreConfig):
        self._config = config
        self._settings = storage_settings()
        self._store_id = self._gen_store_id()
        self._thread_executor = ThreadPoolExecutor(
            max_workers=self._settings.max_storage_concurrancy
        )

    def _gen_store_id(self):
        config_str = self._config.model_dump_json(exclude_none=True)
        return hashlib.md5(config_str.encode("utf-8")).hexdigest()

    def object_basename(self, key: str) -> str:
        return key.split("/")[-1]

    def object_dirname(self, key: str) -> str:
        return "/".join(key.split("/")[:-1])

    @property
    def store_id(self):
        return self._store_id

    @abstractmethod
    def is_ready(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_object(self, key: str):
        raise NotImplementedError()

    @abstractmethod
    def put_object(self, key: str, data: bytes, overwrite: bool = False):
        raise NotImplementedError()

    @abstractmethod
    def delete_object(self, key: str):
        raise NotImplementedError()

    @abstractmethod
    def get_objects(self, keys: List[str]):
        raise NotImplementedError()

    @abstractmethod
    def put_objects(self, entries: List[BulkObjectEntry]):
        raise NotImplementedError()

    @abstractmethod
    def delete_objects(self, keys: List[str]):
        raise NotImplementedError()

    @abstractmethod
    def object_exists(self, key: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_objects_by_pattern(self, pattern: str):
        raise NotImplementedError()

    @abstractmethod
    def delete_objects_by_pattern(self, pattern: str):
        raise NotImplementedError()

    @abstractmethod
    def total_objects(self) -> int:
        raise NotImplementedError()
