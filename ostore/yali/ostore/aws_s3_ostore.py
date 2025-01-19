import asyncio
from io import BytesIO
from typing import Any, AsyncGenerator, List

from minio import Minio
from minio.error import S3Error

from yali.core.errors import ErrorOrBytesIO, ErrorOrStr, YaliError

from .abc_ostore import AbstractStore, AwsS3StoreConfig, BulkPutEntry


class AwsS3Store(AbstractStore):
    def __init__(
        self, *, config: AwsS3StoreConfig, aio_loop: asyncio.AbstractEventLoop
    ):
        self._config = config
        super().__init__(config, aio_loop)

        self._client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key.get_secret_value(),
            session_token=config.session_token,
            secure=config.tls_enabled,
            region=config.region,
            http_client=config.pooled_http,
            credentials=config.creds_provider,
            cert_check=config.check_certs,
        )

        if not self._client.bucket_exists(bucket_name=config.bucket_name):
            self._client.make_bucket(bucket_name=config.bucket_name)
            self._logger.info(f"Created bucket: {config.bucket_name}")

    def object_store_path(self, key: str):
        return f"{self._config.bucket_name}/{key}"

    async def close(self):
        self._logger.info(f"Closing {self._config.stype} store: {self._store_id}")
        self._client._http.clear()
        await super().close()

    def __get_object(self, key: str):
        http_response = self._client.get_object(
            bucket_name=self._config.bucket_name, object_name=key
        )

        odata = http_response.read()
        http_response.close()

        return BytesIO(odata)

    async def get_object(self, key: str) -> ErrorOrBytesIO:
        try:
            odata = await self.add_thread_pool_task(self.__get_object, key)

            assert isinstance(odata, BytesIO)
            return odata
        except Exception as ex:
            error_mesg = f"Failed to read object: {key} from {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)

    def __put_object(self, key: str, data: BytesIO, overwrite: bool = False):
        if not overwrite and self.object_exists(key):
            return -1

        data_len = data.getbuffer().nbytes

        self._client.put_object(
            bucket_name=self._config.bucket_name,
            object_name=key,
            data=data,
            length=data_len,
            content_type="application/octet-stream",
        )

        return data_len

    async def put_object(
        self, key: str, data: BytesIO, overwrite: bool = False
    ) -> ErrorOrStr:
        try:
            result = await self.add_thread_pool_task(
                self.__put_object, key, data, overwrite
            )
            assert isinstance(result, int)

            if result == -1:
                return YaliError(
                    f"Object: {key} already exists in store: {self._store_id}"
                )

            return f"Written {result} bytes for object: {key}"
        except Exception as ex:
            error_mesg = f"Failed to write object: {key} to {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)

    def __delete_object(self, key: str):
        self._client.remove_object(
            bucket_name=self._config.bucket_name, object_name=key
        )

    async def delete_object(self, key: str) -> ErrorOrStr:
        try:
            await self.add_thread_pool_task(self.__delete_object, key)

            return f"Deleted object: {key}"
        except Exception as ex:
            error_mesg = f"Failed to delete object: {key} from {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)

    async def get_objects(self, keys: List[str]) -> AsyncGenerator[ErrorOrBytesIO, Any]:
        if not keys:
            return YaliError("Keys list is empty")

        for okey in keys:
            result = await self.get_object(key=okey)
            yield result

    async def put_objects(
        self, entries: List[BulkPutEntry]
    ) -> AsyncGenerator[ErrorOrStr, Any]:
        if not entries:
            return YaliError("Entries list is empty")

        for okey, data, overwrite in entries:
            result = await self.put_object(key=okey, data=data, overwrite=overwrite)
            yield result

    async def delete_objects(self, keys: List[str]) -> AsyncGenerator[ErrorOrStr, Any]:
        if not keys:
            return YaliError("Keys list is empty")

        for okey in keys:
            result = await self.delete_object(key=okey)
            yield result

    async def object_exists(self, key: str) -> bool:
        await asyncio.sleep(0)

        try:
            obj_stat = self._client.stat_object(
                bucket_name=self._config.bucket_name, object_name=key
            )
            self._logger.debug(
                f"Object: {key} exists in store: {self._store_id} for bucket: {self._config.bucket_name}, with stats: {obj_stat}"
            )
            return True
        except S3Error as ex:
            if ex.code == "NoSuchKey":
                self._logger.debug(
                    f"Object: {key} does not exist in store: {self._store_id} for bucket: {self._config.bucket_name}"
                )
            else:
                self._logger.error(
                    f"Failed to check if object: {key} exists in store: {self._store_id} for bucket: {self._config.bucket_name}"
                )
                raise ex

        return False

    async def total_objects(self) -> int:
        await asyncio.sleep(0)
        count = 0

        for _ in self._client.list_objects(
            bucket_name=self._config.bucket_name, recursive=True
        ):
            count += 1

        return count
