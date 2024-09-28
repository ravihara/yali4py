import asyncio
import logging
import threading
from concurrent.futures import as_completed

import pytest
from core.yali.core.threadasync import ThreadPoolAsyncExecutor


@pytest.mark.asyncio(scope="class")
class TestAioThread:
    counter: int = 0
    logger = logging.getLogger(__name__)

    async def delayed_job(self):
        await asyncio.sleep(0.1)
        self.logger.info(threading.current_thread().name)
        self.counter += 1

        return self.counter

    async def test_aio_thread_pool_executor(self):
        futures = []
        results = []

        with ThreadPoolAsyncExecutor(max_workers=10) as executor:
            for _ in range(200):
                futures.append(executor.submit(self.delayed_job()))

        for _, fut in enumerate(as_completed(futures)):
            results.append(fut.result())

        results.sort()
        assert len(results) == 200
        assert results == list(range(1, 201))
