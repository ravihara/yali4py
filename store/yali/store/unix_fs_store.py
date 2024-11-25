from typing import List
from .abc_store import AbstractStore, UnixFsStoreConfig, BulkPutEntry
from asyncio import AbstractEventLoop
from yali.core.utils.osfiles import FilesConv


class UnixFsStore(AbstractStore):
    def __init__(self, *, config: UnixFsStoreConfig, aio_loop: AbstractEventLoop):
        super().__init__(config=config, aio_loop=aio_loop)

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

        if not FilesConv.is_file_readable(file_path=opath):
            self._logger.warning(f"File '{opath}' is not readable")
            return None

    async def put_object(self, key: str, data: bytes, overwrite: bool = False):
        opath = self.object_store_path(key=key)
        FilesConv.write_file(file_path=opath, data=data, overwrite=overwrite)

    async def delete_object(self, key: str):
        opath = self.object_store_path(key=key)
        FilesConv.delete_file(file_path=opath)

    async def get_objects(self, keys: List[str]):
        raise NotImplementedError()

    async def put_objects(self, entries: List[BulkPutEntry]):
        raise NotImplementedError()

    async def delete_objects(self, keys: List[str]):
        raise NotImplementedError()

    async def object_exists(self, key: str) -> bool:
        return FilesConv.file_exists(f"{self._config.sroot}/{key}")

    async def total_objects(self) -> int:
        return FilesConv.total_files_in_dir(base_dir=self._config.sroot)
