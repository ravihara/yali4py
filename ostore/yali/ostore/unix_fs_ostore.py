import asyncio
from io import BytesIO
from typing import Any, AsyncGenerator, List

from yali.core.errors import ErrorOrBytesIO, ErrorOrStr, YaliError
from yali.core.osfiles import FSNode

from .abc_ostore import AbstractStore, BulkPutEntry, UnixFsStoreConfig


class UnixFsStore(AbstractStore):
    def __init__(
        self, *, config: UnixFsStoreConfig, aio_loop: asyncio.AbstractEventLoop
    ):
        self._config = config
        super().__init__(config, aio_loop)

        if self._config.is_readonly:
            if not FSNode.is_dir_readable(self._config.sroot):
                raise YaliError(
                    f"Store's root folder is not readable: {self._config.sroot}"
                )
        elif not FSNode.is_dir_writable(self._config.sroot, check_creatable=True):
            raise YaliError(
                f"Store's root folder is not writable: {self._config.sroot}"
            )

    def object_store_path(self, key: str):
        if key.startswith(f"{self._config.sroot}/"):
            return key

        if key.startswith("/"):
            return f"{self._config.sroot}{key}"

        return f"{self._config.sroot}/{key}"

    async def close(self):
        self._logger.info(f"Closing {self._config.stype} store: {self._store_id}")
        await super().close()

    async def get_object(self, key: str) -> ErrorOrBytesIO:
        try:
            opath = self.object_store_path(key=key)
            odata = await self.add_thread_pool_task(FSNode.read_bytes, opath)

            assert isinstance(odata, bytes)
            return BytesIO(odata)
        except Exception as ex:
            error_mesg = f"Failed to read object: {key} from {self._config.stype} store: {self._store_id}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)

    async def put_object(
        self, key: str, data: BytesIO, overwrite: bool = False
    ) -> ErrorOrStr:
        if self._config.is_readonly:
            return YaliError(
                f"Store: {self._store_id} is read-only, cannot write object: {key}"
            )

        try:
            opath = self.object_store_path(key=key)
            result = await self.add_thread_pool_task(
                FSNode.write_bytes, opath, data.getvalue(), overwrite=overwrite
            )
            assert isinstance(result, int)

            if result == -1:
                return YaliError(
                    f"Object: {key} already exists in store: {self._store_id}"
                )

            return f"Written {result} bytes for object: {key}"
        except Exception as ex:
            error_mesg = f"Failed to write object: {key} to {self._config.stype} store: {self._store_id}"

            self._logger.error(error_mesg, exc_info=ex)
            return YaliError(error_mesg, exc_cause=ex)

    async def delete_object(self, key: str) -> ErrorOrStr:
        if self._config.is_readonly:
            return YaliError(
                f"Store: {self._store_id} is read-only, cannot delete object: {key}"
            )

        try:
            opath = self.object_store_path(key=key)
            await self.add_thread_pool_task(FSNode.delete_file, opath)

            return f"Deleted object: {key}"
        except Exception as ex:
            error_mesg = f"Failed to delete object: {key} from {self._config.stype} store: {self._store_id}"

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

        if self._config.is_readonly:
            return YaliError(
                f"Store: {self._store_id} is read-only, cannot write objects"
            )

        for okey, data, overwrite in entries:
            result = await self.put_object(key=okey, data=data, overwrite=overwrite)
            yield result

    async def delete_objects(self, keys: List[str]) -> AsyncGenerator[ErrorOrStr, Any]:
        if not keys:
            return YaliError("Keys list is empty")

        if self._config.is_readonly:
            return YaliError(
                f"Store: {self._store_id} is read-only, cannot delete objects"
            )

        for okey in keys:
            result = await self.delete_object(key=okey)
            yield result

    async def object_exists(self, key: str) -> bool:
        await asyncio.sleep(0)
        return FSNode.file_exists(f"{self._config.sroot}/{key}")

    async def total_objects(self) -> int:
        await asyncio.sleep(0)
        return FSNode.total_files_in_dir(base_dir=self._config.sroot)
