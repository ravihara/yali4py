import asyncio
from typing import List

from minio import Minio
from minio.error import S3Error

from .abc_store import AbstractStore, AwsS3StoreConfig, BulkPutEntry


class AwsS3Store(AbstractStore):
    def __init__(self, config: AwsS3StoreConfig):
        self._config = config
        super().__init__(config)

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

    def is_accessible(self):
        try:
            if self._client.bucket_exists(bucket_name=self._config.bucket_name):
                self._logger.debug(f"Bucket: {self._config.bucket_name} exists")
                return True

            return False
        except Exception as ex:
            self._logger.error(f"Failed to access bucket: {self._config.bucket_name}", exc_info=ex)
            return False

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

        return odata

    async def get_object(self, key: str):
        try:
            aio_future = self.add_thread_pool_task(self.__get_object, key)
            odata = await aio_future

            assert isinstance(odata, bytes)
            return odata
        except Exception as ex:
            self._logger.error(
                f"Failed to read object: {key} from {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}",
                exc_info=ex,
            )

        return None

    def __put_object(self, key: str, data: bytes, overwrite: bool = False):
        if not overwrite and self.object_exists(key):
            return -1

        data_len = len(data)

        self._client.put_object(
            bucket_name=self._config.bucket_name,
            object_name=key,
            data=data,
            length=data_len,
            content_type="application/octet-stream",
        )

        return data_len

    async def put_object(self, key: str, data: bytes, overwrite: bool = False):
        try:
            aio_future = self.add_thread_pool_task(self.__put_object, key, data, overwrite)
            result = await aio_future

            if result == -1:
                self._logger.warning(f"Object: {key} already exists in store: {self._store_id}")
        except Exception as ex:
            self._logger.error(
                f"Failed to write object: {key} to {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}",
                exc_info=ex,
            )

    def __delete_object(self, key: str):
        self._client.remove_object(bucket_name=self._config.bucket_name, object_name=key)

    async def delete_object(self, key: str):
        try:
            aio_future = self.add_thread_pool_task(self.__delete_object, key)
            await aio_future
        except Exception as ex:
            self._logger.error(
                f"Failed to delete object: {key} from {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}",
                exc_info=ex,
            )

    async def get_objects(self, keys: List[str]):
        aio_futures = []

        for okey in keys:
            aio_futures.append(self.add_thread_pool_task(self.__get_object, okey))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)
        files_data: List[bytes] = []

        for okey, result in zip(keys, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to read object: {okey} from {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}",
                    exc_info=result,
                )
                results[keys.index(okey)] = None
            else:
                files_data.append(result)

        results.clear()
        aio_futures.clear()

        return files_data

    async def put_objects(self, entries: List[BulkPutEntry]):
        aio_futures = []

        for okey, data, overwrite in entries:
            aio_futures.append(self.add_thread_pool_task(self.__put_object, okey, data, overwrite))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)

        for okey, result in zip(entries, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to write object: {okey} to {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}",
                    exc_info=result,
                )
            elif result == -1:
                self._logger.warning(f"Object: {okey} already exists in store: {self._store_id}")

        results.clear()
        aio_futures.clear()

    async def delete_objects(self, keys: List[str]):
        aio_futures = []

        for okey in keys:
            aio_futures.append(self.add_thread_pool_task(self.__delete_object, okey))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)

        for okey, result in zip(keys, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to delete object: {okey} from {self._config.stype} store: {self._store_id} for bucket: {self._config.bucket_name}",
                    exc_info=result,
                )

        results.clear()
        aio_futures.clear()

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

        for _ in self._client.list_objects(bucket_name=self._config.bucket_name, recursive=True):
            count += 1

        return count
