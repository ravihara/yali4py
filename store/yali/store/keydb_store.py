import logging
from typing import Any, List

import redis.asyncio as aio_redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError
from redis.retry import Retry

from yali.core.codecs import data_from_json, data_to_json
from yali.core.metatypes import DataType, NonEmptyStr, PositiveInt, SecretStr
from yali.core.models import BaseModel, field_specs
from yali.core.settings import env_config

_env_config = env_config()


class KeydbSettings(BaseModel):
    host: str = _env_config("KEY_STORE_HOST", default="localhost")
    port: PositiveInt = _env_config("KEY_STORE_PORT", default=6379, cast=int)
    password: SecretStr | None = _env_config("KEY_STORE_PASSWORD", default=None)
    key_prefix: NonEmptyStr = _env_config(
        "KEY_PREFIX", default="yali|", cast=NonEmptyStr
    )
    db_id: int = _env_config("DB_ID", default=0, cast=int)
    resp_proto: int = _env_config("RESP_PROTOCOL", default=2, cast=int)


class KeydbEntry(BaseModel):
    key: NonEmptyStr
    value: DataType
    ttl: PositiveInt | None = field_specs(default=None)


class KeyWithType(BaseModel):
    key: NonEmptyStr
    val_type: DataType | None = None


class KeydbStore:
    _logger = logging.getLogger("yali.store.keydb_store")

    def __init__(self, settings: KeydbSettings):
        keydb_pass = settings.password.get_secret_value() if settings.password else None
        keydb_retry = Retry(
            backoff=ExponentialBackoff(),
            retries=3,
            supported_errors=(ConnectionError, TimeoutError, BusyLoadingError),
        )

        self.__pfx = settings.key_prefix
        self.__client = aio_redis.Redis(
            host=settings.host,
            port=settings.port,
            db=settings.db_id,
            protocol=settings.resp_proto,
            password=keydb_pass,
            retry=keydb_retry,
        )

    @property
    def aio_client(self):
        return self.__client

    def _pfx_key(self, key: str):
        return f"{self.__pfx}{key}"

    async def _ensure_connection(self):
        try:
            res = await self.__client.ping()
            if res:
                self._logger.debug(f"Keydb is connected: {res}")
                return True
        except aio_redis.ConnectionError as ex:
            self._logger.error("Keydb connection error", exc_info=ex)
        except Exception as ex:
            self._logger.error("Error while connecting to Keydb", exc_info=ex)

        return False

    def _decoded_value(self, val: Any, val_type: DataType | None = None):
        if not val_type:
            return data_from_json(data=val)

        return data_from_json(data=val, dec_type=val_type)

    async def close(self):
        await self.__client.aclose()

    async def save(self, entry: KeydbEntry):
        if not await self._ensure_connection():
            return None

        key = self._pfx_key(key=entry.key)
        val = (
            data_to_json(data=entry.value, as_string=True)
            if not isinstance(entry.value, str)
            else entry.value
        )

        if entry.ttl:
            return await self.__client.setex(key, entry.ttl, val)

        return await self.__client.set(key, val)

    async def safe_save(self, entry: KeydbEntry):
        if not await self._ensure_connection():
            return None

        results = []

        async with self.__client.pipeline(transaction=True) as pipe:
            try:
                key = self._pfx_key(key=entry.key)
                val = (
                    data_to_json(data=entry.value, as_string=True)
                    if not isinstance(entry.value, str)
                    else entry.value
                )

                if entry.ttl:
                    pipe.setex(key, entry.ttl, val)
                else:
                    pipe.set(key, val)

                results = await pipe.execute()
            except Exception as ex:
                self._logger.error(f"Error while saving key: {key}", exc_info=ex)

        if not results:
            self._logger.error("No results from pipeline execution")
            return None

        return results[0]

    async def fetch(self, key: str, *, val_type: DataType | None = None):
        if not await self._ensure_connection():
            return None

        try:
            key = self._pfx_key(key=key)
            val = await self.__client.get(key)

            return self._decoded_value(val=val, val_type=val_type)
        except aio_redis.ResponseError as ex:
            self._logger.error(
                f"Keydb response error while fetching key: {key}", exc_info=ex
            )
        except aio_redis.ReadOnlyError as ex:
            self._logger.error(
                f"Keydb read-only error while fetching key: {key}", exc_info=ex
            )
        except Exception as ex:
            self._logger.error(f"Error while fetching key: {key}", exc_info=ex)

        return None

    async def fetch_and_remove(self, key: str, *, val_type: DataType | None = None):
        if not await self._ensure_connection():
            return None

        async with self.__client.pipeline(transaction=True) as pipe:
            try:
                key = self._pfx_key(key=key)
                pipe.get(key)
                pipe.delete(key)
                results = await pipe.execute()

                if len(results) < 2:
                    self._logger.error(
                        f"Not enough results from pipeline execution for key: {key}"
                    )

                    return None

                value = results[0]  # Result of `get`
                delete_result = results[1]  # Result of `delete`

                # Output the results
                self._logger.debug(
                    f"Key: {key}, Value: {value}, Delete Result: {delete_result}"
                )

                return self._decoded_value(val=value, val_type=val_type)
            except Exception as ex:
                self._logger.error(
                    f"An error occurred while fetching and removing key: {key}",
                    exc_info=ex,
                )

                return None

    async def remove(self, key: str):
        if not await self._ensure_connection():
            return None

        key = self._pfx_key(key=key)
        return await self.__client.delete(key)

    async def key_exists(self, key: str):
        if not await self._ensure_connection():
            return False

        key = self._pfx_key(key=key)
        res = await self.__client.exists(key)

        return bool(res)

    async def multi_save(self, entries: List[KeydbEntry]):
        if not await self._ensure_connection():
            return None

        async with self.__client.pipeline(transaction=True) as pipe:
            for entry in entries:
                key = self._pfx_key(key=entry.key)
                val = (
                    data_to_json(data=entry.value, as_string=True)
                    if not isinstance(entry.value, str)
                    else entry.value
                )

                if entry.ttl:
                    pipe.setex(key, entry.ttl, val)
                else:
                    pipe.set(key, val)

            return await pipe.execute()

    async def multi_fetch(self, entries: List[KeyWithType]):
        if not await self._ensure_connection():
            return None

        results = []

        async with self.__client.pipeline(transaction=True) as pipe:
            for entry in entries:
                key = self._pfx_key(key=entry.key)
                pipe.get(key)

            results = await pipe.execute()

        if len(results) != len(entries):
            self._logger.error(
                f"Mismatch in results obtained for total of {len(entries)} entries"
            )
            return []

        for idx, entry in enumerate(entries):
            if entry.val_type:
                results[idx] = self._decoded_value(
                    val=results[idx], val_type=entry.val_type
                )
            else:
                results[idx] = self._decoded_value(val=results[idx])

        return results
