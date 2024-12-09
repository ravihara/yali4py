import asyncio
from typing import Any, AsyncGenerator, List

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob.aio import BlobClient, BlobServiceClient, ContainerClient
from yali.core.typings import YaliError

from .abc_store import (
    AbstractStore,
    AzureBlobStoreConfig,
    BulkPutEntry,
    BytesIO,
    ErrorOrBytesIO,
    ErrorOrStr,
)


class AzureBlobStore(AbstractStore):
    def __init__(self, *, config: AzureBlobStoreConfig, aio_loop: asyncio.AbstractEventLoop):
        self._config = config
        super().__init__(config, aio_loop)

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

    async def close(self):
        self._logger.info(f"Closing {self._config.stype} store: {self._store_id}")

        if self._container_client:
            await self._container_client.close()

        await self._service_client.close()
        await super().close()

    async def get_object(self, key: str) -> ErrorOrBytesIO:
        try:
            blob_client = self._container_client.get_blob_client(blob=key)
            stream_dl = await blob_client.download_blob()
            odata = await stream_dl.readall()

            await blob_client.close()

            return BytesIO(odata)
        except Exception as ex:
            error_mesg = f"Failed to read object: {key} from {self._config.stype} store: {self._store_id} for container: {self._config.container_name}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)

    async def put_object(self, key: str, data: BytesIO, overwrite: bool = False) -> ErrorOrStr:
        blob_client: BlobClient = None

        try:
            blob_client = self._container_client.get_blob_client(blob=key)
            ul_result = await blob_client.upload_blob(data.getvalue(), overwrite=overwrite)

            self._logger.debug(ul_result)
            return f"Written {data.getbuffer().nbytes} bytes for object: {key}"
        except ResourceExistsError:
            return YaliError(f"Object: {key} already exists in store: {self._store_id}")
        except Exception as ex:
            error_mesg = f"Failed to write object: {key} to {self._config.stype} store: {self._store_id} for container: {self._config.container_name}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)
        finally:
            if blob_client:
                await blob_client.close()

    async def delete_object(self, key: str) -> ErrorOrStr:
        blob_client: BlobClient = None

        try:
            blob_client = self._container_client.get_blob_client(blob=key)
            await blob_client.delete_blob()
            await blob_client.close()

            return f"Deleted object: {key}"
        except Exception as ex:
            error_mesg = f"Failed to delete object: {key} from {self._config.stype} store: {self._store_id} for container: {self._config.container_name}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)
        finally:
            if blob_client:
                await blob_client.close()

    async def get_objects(self, keys: List[str]) -> AsyncGenerator[ErrorOrBytesIO, Any]:
        if not keys:
            return YaliError("Keys list is empty")

        for okey in keys:
            result = await self.get_object(key=okey)
            yield result

    async def put_objects(self, entries: List[BulkPutEntry]) -> AsyncGenerator[ErrorOrStr, Any]:
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
