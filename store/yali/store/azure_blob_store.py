import asyncio
from typing import List

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob.aio import (BlobClient, BlobServiceClient,
                                    ContainerClient)
from yali.core.typings import YaliError

from .abc_store import AbstractStore, AzureBlobStoreConfig, BulkPutEntry


class AzureBlobStore(AbstractStore):
    def __init__(self, config: AzureBlobStoreConfig):
        self._config = config
        super().__init__(config)

        try:
            self._service_client = BlobServiceClient.from_connection_string(
                conn_str=self._config.connection_string.get_secret_value()
            )
            self._container_client = self._aio_loop.run_until_complete(self.__ensure_container())
        except Exception as ex:
            self._logger.error(
                f"Failed to initialize store: {self._store_id} for container: {self._config.container_name}",
                exc_info=ex,
            )

        if not self._container_client:
            self._aio_loop.run_until_complete(self._service_client.close())
            raise YaliError(f"Failed to initialize store: {self._store_id}")

    async def __ensure_container(self):
        container_client: ContainerClient = None

        try:
            container_client = self._service_client.get_container_client(
                container=self._config.container_name
            )
        except ResourceNotFoundError:
            container_client = await self._service_client.create_container(
                name=self._config.container_name
            )
            self._logger.info(f"Created container: {self._config.container_name}")
        except Exception as ex:
            self._logger.error(
                f"Failed to ensure container: {self._config.container_name} in store: {self._store_id}",
                exc_info=ex,
            )

            return None

        return container_client

    def object_store_path(self, key: str):
        return f"{self._config.container_name}/{key}"

    def is_accessible(self):
        try:
            _ = self._aio_loop.run_until_complete(self._container_client.get_container_properties())
            return True
        except Exception as ex:
            self._logger.error(
                f"Failed to access container: {self._config.container_name} in store: {self._store_id}",
                exc_info=ex,
            )

        return False

    async def close(self):
        self._logger.info(f"Closing {self._config.stype} store: {self._store_id}")

        if self._container_client:
            await self._container_client.close()

        await self._service_client.close()
        await super().close()

    async def get_object(self, key: str):
        try:
            blob_client = self._container_client.get_blob_client(blob=key)
            stream_dl = await blob_client.download_blob()
            odata = await stream_dl.readall()

            await blob_client.close()

            return odata
        except Exception as ex:
            self._logger.error(
                f"Failed to read object: {key} from {self._config.stype} store: {self._store_id} for container: {self._config.container_name}",
                exc_info=ex,
            )

        return None

    async def put_object(self, key: str, data: bytes, overwrite: bool = False):
        blob_client: BlobClient = None
        result = 0

        try:
            blob_client = self._container_client.get_blob_client(blob=key)
            ul_result = await blob_client.upload_blob(data, overwrite=overwrite)
            result = len(data)

            self._logger.debug(ul_result)
        except ResourceExistsError:
            self._logger.warning(f"Object: {key} already exists in store: {self._store_id}")
            result = -1
        except Exception as ex:
            self._logger.error(
                f"Failed to write object: {key} to {self._config.stype} store: {self._store_id} for container: {self._config.container_name}",
                exc_info=ex,
            )
        finally:
            if blob_client:
                await blob_client.close()

        return result

    async def delete_object(self, key: str):
        try:
            blob_client = self._container_client.get_blob_client(blob=key)
            await blob_client.delete_blob()
            await blob_client.close()
        except Exception as ex:
            self._logger.error(
                f"Failed to delete object: {key} from {self._config.stype} store: {self._store_id} for container: {self._config.container_name}",
                exc_info=ex,
            )

    async def get_objects(self, keys: List[str]):
        aio_futures = []

        for okey in keys:
            aio_futures.append(self.get_object(key=okey))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)
        files_data: List[bytes] = []

        for okey, result in zip(keys, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to read object: {okey} from {self._config.stype} store: {self._store_id} for container: {self._config.container_name}",
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
            aio_futures.append(self.put_object(key=okey, data=data, overwrite=overwrite))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)

        for okey, result in zip(entries, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to write object: {okey} to {self._config.stype} store: {self._store_id} for container: {self._config.container_name}",
                    exc_info=result,
                )
            elif result == -1:
                self._logger.warning(f"Object: {okey} already exists in store: {self._store_id}")

        results.clear()
        aio_futures.clear()

    async def delete_objects(self, keys: List[str]):
        aio_futures = []

        for okey in keys:
            aio_futures.append(self.delete_object(key=okey))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)

        for okey, result in zip(keys, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to delete object: {okey} from {self._config.stype} store: {self._store_id} for container: {self._config.container_name}",
                    exc_info=result,
                )

        results.clear()
        aio_futures.clear()

    async def object_exists(self, key: str):
        try:
            blob_client = self._container_client.get_blob_client(blob=key)
            blob_exists = await blob_client.exists()

            await blob_client.close()
            return blob_exists
        except Exception as ex:
            self._logger.error(
                f"Failed to check if object: {key} exists in {self._config.stype} store: {self._store_id} for container: {self._config.container_name}",
                exc_info=ex,
            )

        return False

    async def total_objects(self):
        count = 0

        async for _ in self._container_client.list_blobs():
            count += 1

        return count
