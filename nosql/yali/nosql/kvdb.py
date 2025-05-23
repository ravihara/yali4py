import logging
from typing import Any, List

import redis.asyncio as aio_redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError
from redis.retry import Retry

from yali.core.appconf import env_config
from yali.core.codecs import JSONNode
from yali.core.models import BaseModel, field_specs
from yali.core.typebase import DataType, NonEmptyStr, PositiveInt, SecretStr

_env_config = env_config()


class KVStoreSettings(BaseModel):
    host: str = _env_config("KV_STORE_HOST", default="localhost")
    port: PositiveInt = _env_config("KV_STORE_PORT", default=6379, cast=int)
    password: SecretStr | None = _env_config("KV_STORE_PASSWORD", default=None)
    key_prefix: NonEmptyStr = _env_config(
        "KV_PREFIX", default="yali|", cast=NonEmptyStr
    )
    db_id: int = _env_config("KV_ID", default=0, cast=int)
    resp_proto: int = _env_config("KV_RESP_PROTOCOL", default=2, cast=int)
    retry_base_sec: float = _env_config("KV_RETRY_BASE_SEC", default=5, cast=float)
    retry_cap_sec: float = _env_config("KV_RETRY_CAP_SEC", default=30, cast=float)
    max_retries: int = _env_config("KV_MAX_RETRIES", default=3, cast=int)


class KVEntry(BaseModel):
    key: NonEmptyStr
    value: DataType
    ttl: PositiveInt | None = field_specs(default=None)


class KeyWithType(BaseModel):
    key: NonEmptyStr
    val_type: DataType | None = None


class KVStore:
    _logger = logging.getLogger("yali.nosql.kvdb")

    def __init__(self, settings: KVStoreSettings):
        keydb_pass = settings.password.get_secret_value() if settings.password else None
        keydb_retry = Retry(
            backoff=ExponentialBackoff(
                cap=settings.retry_cap_sec, base=settings.retry_base_sec
            ),
            retries=settings.max_retries,
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
                self._logger.debug(f"KVStore is connected: {res}")
                return True
        except aio_redis.ConnectionError as ex:
            self._logger.error("KVStore connection error", exc_info=ex)
        except Exception as ex:
            self._logger.error("Error while connecting to KVStore", exc_info=ex)

        return False

    def _decoded_value(self, val: Any, val_type: DataType | None = None):
        if not val_type:
            return JSONNode.load_data(data=val)

        return JSONNode.load_data(data=val, dec_type=val_type)

    async def close(self):
        await self.__client.aclose()

    async def save(self, entry: KVEntry):
        if not await self._ensure_connection():
            return None

        key = self._pfx_key(key=entry.key)
        val = (
            JSONNode.dump_str(data=entry.value)
            if not isinstance(entry.value, str)
            else entry.value
        )

        if entry.ttl:
            return await self.__client.setex(key, entry.ttl, val)

        return await self.__client.set(key, val)

    async def safe_save(self, entry: KVEntry):
        if not await self._ensure_connection():
            return None

        results = []

        async with self.__client.pipeline(transaction=True) as pipe:
            try:
                key = self._pfx_key(key=entry.key)
                val = (
                    JSONNode.dump_str(data=entry.value)
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
                f"KVStore response error while fetching key: {key}", exc_info=ex
            )
        except aio_redis.ReadOnlyError as ex:
            self._logger.error(
                f"KVStore read-only error while fetching key: {key}", exc_info=ex
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

    async def multi_save(self, entries: List[KVEntry]):
        if not await self._ensure_connection():
            return None

        async with self.__client.pipeline(transaction=True) as pipe:
            for entry in entries:
                key = self._pfx_key(key=entry.key)
                val = (
                    JSONNode.dump_str(data=entry.value)
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
