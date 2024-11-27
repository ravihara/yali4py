import asyncio
from typing import List

from yali.core.utils.osfiles import FilesConv

from .abc_store import AbstractStore, BulkPutEntry, UnixFsStoreConfig


class UnixFsStore(AbstractStore):
    def __init__(self, config: UnixFsStoreConfig):
        super().__init__(config=config)

    def object_store_path(self, key: str):
        if key.startswith(f"{self._config.sroot}/"):
            return key

        if key.startswith("/"):
            return f"{self._config.sroot}{key}"

        return f"{self._config.sroot}/{key}"

    def is_accessible(self):
        if self._config.is_readonly and FilesConv.is_dir_readable(self._config.sroot):
            self._logger.debug(f"Store's root folder is readable: {self._config.sroot}")
            return True

        if FilesConv.is_dir_writable(self._config.sroot, check_creatable=True):
            self._logger.debug(f"Store's root folder is writable: {self._config.sroot}")
            return True

        self._logger.error(f"Store's root folder is not accessible: {self._config.sroot}")
        return False

    async def close(self):
        self._logger.info(f"Closing {self._config.stype} store: {self._store_id}")
        await super().close()

    async def get_object(self, key: str):
        opath = self.object_store_path(key=key)
        aio_future = self.add_thread_pool_task(FilesConv.read_bytes, opath)

        try:
            result = await aio_future
            assert isinstance(result, bytes)
            return result
        except Exception as ex:
            self._logger.error(
                f"Failed to read object: {key} from {self._config.stype} store: {self._store_id}",
                exc_info=ex,
            )
            return None

    async def put_object(self, key: str, data: bytes, overwrite: bool = False):
        opath = self.object_store_path(key=key)
        aio_future = self.add_thread_pool_task(
            FilesConv.write_bytes, opath, data, overwrite=overwrite
        )

        try:
            result = await aio_future
            assert isinstance(result, int)

            if result == -1:
                self._logger.warning(f"Object: {key} already exists in store: {self._store_id}")
        except Exception as ex:
            self._logger.error(
                f"Failed to write object: {key} to {self._config.stype} store: {self._store_id}",
                exc_info=ex,
            )

    async def delete_object(self, key: str):
        opath = self.object_store_path(key=key)
        aio_future = self.add_thread_pool_task(FilesConv.delete_file, opath)

        try:
            await aio_future
        except Exception as ex:
            self._logger.error(
                f"Failed to delete object: {key} from {self._config.stype} store: {self._store_id}",
                exc_info=ex,
            )

    async def get_objects(self, keys: List[str]):
        aio_futures = []

        for okey in keys:
            opath = self.object_store_path(key=okey)
            aio_futures.append(self.add_thread_pool_task(FilesConv.read_bytes, opath))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)
        files_data: List[bytes] = []

        for okey, result in zip(keys, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to read object: {okey} from {self._config.stype} store: {self._store_id}",
                    exc_info=result,
                )
                results[keys.index(okey)] = None
            else:
                files_data.append(result)

        results.clear()
        return files_data

    async def put_objects(self, entries: List[BulkPutEntry]):
        aio_futures = []

        for okey, data, overwrite in entries:
            opath = self.object_store_path(key=okey)
            aio_futures.append(
                self.add_thread_pool_task(FilesConv.write_bytes, opath, data, overwrite=overwrite)
            )

        results = await asyncio.gather(*aio_futures, return_exceptions=True)

        for okey, result in zip(entries, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to write object: {okey} to {self._config.stype} store: {self._store_id}",
                    exc_info=result,
                )
            elif result == -1:
                self._logger.warning(f"Object: {okey} already exists in store: {self._store_id}")

        results.clear()

    async def delete_objects(self, keys: List[str]):
        aio_futures = []

        for okey in keys:
            opath = self.object_store_path(key=okey)
            aio_futures.append(self.add_thread_pool_task(FilesConv.delete_file, opath))

        results = await asyncio.gather(*aio_futures, return_exceptions=True)

        for okey, result in zip(keys, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to delete object: {okey} from {self._config.stype} store: {self._store_id}",
                    exc_info=result,
                )

        results.clear()

    async def object_exists(self, key: str) -> bool:
        await asyncio.sleep(0)
        return FilesConv.file_exists(f"{self._config.sroot}/{key}")

    async def total_objects(self) -> int:
        await asyncio.sleep(0)
        return FilesConv.total_files_in_dir(base_dir=self._config.sroot)
