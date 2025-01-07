import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, Coroutine

from janus import AsyncQueue, SyncQueue
from janus import Queue as AioQueue

from ..constants import YALI_BREAK_EVENT
from ..metatypes import DataType

SyncQTask = Callable[[SyncQueue[DataType]], Any]
AsyncQTask = Callable[[AsyncQueue[DataType]], Coroutine[Any, Any, Any]]
SyncConsumer = Callable[[DataType], None]
AsyncConsumer = Callable[[DataType], Coroutine[Any, Any, None]]


def _sync_handler(sync_q: SyncQueue[DataType], sync_q_task: SyncQTask):
    logger = logging.getLogger(sync_q_task.__qualname__)

    try:
        return sync_q_task(sync_q)
    except Exception as ex:
        logger.error(ex, exc_info=True)


class MemQueue:
    _logger = logging.getLogger("yali.core.memq.mem_pubsub")

    def __init__(
        self,
        *,
        max_queue_size: int = 1000,
        sync_consumer: SyncConsumer | None = None,
        async_consumer: AsyncConsumer | None = None,
        sync_executor: ThreadPoolExecutor | None = None,
    ):
        queue_inst = AioQueue(maxsize=max_queue_size)

        self.__sync_q = queue_inst.sync_q
        self.__async_q = queue_inst.async_q

        self.__sync_consumer = sync_consumer
        self.__async_consumer = async_consumer

        self.__sync_executor = sync_executor

    def publish(self, item: DataType):
        self.__sync_q.put(item)

    async def aio_publish(self, item: DataType):
        await self.__async_q.put(item)

    def consume(self):
        if not self.__sync_consumer:
            raise ValueError("Sync consumer is not set")

        while True:
            item = self.__sync_q.get()

            if item is YALI_BREAK_EVENT:
                break

            try:
                self.__sync_consumer(item)
            except Exception as ex:
                self._logger.error(
                    "Unhandled error while consuming message", exc_info=ex
                )

            self.__sync_q.task_done()

    async def aio_consume(self):
        if not self.__async_consumer:
            raise ValueError("Async consumer is not set")

        while True:
            item = await self.__async_q.get()

            if item is YALI_BREAK_EVENT:
                break

            try:
                await self.__async_consumer(item)
            except Exception as ex:
                self._logger.error(
                    "Unhandled error while consuming message", exc_info=ex
                )

            await self.__async_q.task_done()

    def run_task(self, sync_q_task: SyncQTask):
        if not self.__sync_executor:
            raise ValueError("Sync executor is not set")

        aio_loop = asyncio.get_running_loop()
        wrapped_fn = partial(_sync_handler, self.__sync_q, sync_q_task)

        return aio_loop.run_in_executor(self.__sync_executor, wrapped_fn)

    async def run_aio_task(self, async_q_task: AsyncQTask):
        try:
            return await async_q_task(self.__async_q)
        except Exception as ex:
            self._logger.error(ex, exc_info=True)
