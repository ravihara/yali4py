import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from yali.core.aio import AsyncQueue, SyncQueue, ThreadAio
from yali.core.consts import YALI_SENTINEL


@pytest.mark.asyncio(scope="class")
class TestAioThread:
    @pytest.fixture
    def thread_aio(self):
        def sync_consumer(item):
            time.sleep(2)
            print(item)

        async def async_consumer(item):
            await asyncio.sleep(2)
            print(item)

        thrd_pool_executor = ThreadPoolExecutor(max_workers=2)

        yield ThreadAio(
            max_queue_size=100,
            sync_consumer=sync_consumer,
            async_consumer=async_consumer,
            sync_executor=thrd_pool_executor,
        )

        thrd_pool_executor.shutdown()

    async def test_async_pub_sync_sub(self, thread_aio: ThreadAio):
        for i in range(3):
            await thread_aio.aio_publish(i)

        await thread_aio.aio_publish(YALI_SENTINEL)
        thread_aio.consume()

    async def test_sync_pub_async_sub(self, thread_aio: ThreadAio):
        for i in range(3):
            thread_aio.publish(i)

        thread_aio.publish(YALI_SENTINEL)
        await thread_aio.aio_consume()

    async def test_sync_task(self, thread_aio: ThreadAio):
        def sync_q_task(sync_q: SyncQueue[str]):
            while True:
                item = sync_q.get()
                if item is YALI_SENTINEL:
                    break

                print(item)
                sync_q.task_done()

        async def async_q_task(async_q: AsyncQueue[str]):
            for i in range(3):
                await async_q.put(i)

            await async_q.put(YALI_SENTINEL)

        await thread_aio.run_aio_task(async_q_task)
        await thread_aio.run_task(sync_q_task)

    async def test_async_task(self, thread_aio: ThreadAio):
        def sync_q_task(sync_q: SyncQueue[str]):
            for i in range(3):
                sync_q.put(i)

            sync_q.put(YALI_SENTINEL)

        async def async_q_task(async_q: AsyncQueue[str]):
            while True:
                item = await async_q.get()
                if item is YALI_SENTINEL:
                    break

                print(item)
                async_q.task_done()

        await thread_aio.run_task(sync_q_task)
        await thread_aio.run_aio_task(async_q_task)
