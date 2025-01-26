import asyncio
import itertools
import logging
import os
import queue
import threading
import types
import weakref
from concurrent.futures import BrokenExecutor, ThreadPoolExecutor
from concurrent.futures import Executor as BaseExecutor
from concurrent.futures import Future as BaseFuture
from functools import partial
from typing import Any, Callable, Coroutine, MutableSet

from janus import AsyncQueue, SyncQueue
from janus import Queue as _Queue

from .consts import YALI_NUM_THREAD_WORKERS, YALI_SENTINEL
from .typebase import DataType

SyncQTask = Callable[[SyncQueue[DataType]], Any]
AsyncQTask = Callable[[AsyncQueue[DataType]], Coroutine[Any, Any, Any]]
SyncConsumer = Callable[[DataType], None]
AsyncConsumer = Callable[[DataType], Coroutine[Any, Any, None]]

_logger = logging.getLogger("yali.core.aio")
_threads_queues = weakref.WeakKeyDictionary()
_shutdown = False

# Lock that ensures that new workers are not created while the interpreter is
# shutting down. Must be held while mutating _threads_queues and _shutdown.
_global_shutdown_lock = threading.Lock()


def _python_exit():
    global _shutdown

    with _global_shutdown_lock:
        _shutdown = True

    items = list(_threads_queues.items())

    for t, q in items:
        q.put(None)

    for t, q in items:
        t.join()


# Register for `_python_exit()` to be called just before joining all
# non-daemon threads. This is used instead of `atexit.register()` for
# compatibility with subinterpreters, which no longer support daemon threads.
threading._register_atexit(_python_exit)

# At fork, reinitialize the `_global_shutdown_lock` lock in the child process
if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_global_shutdown_lock.acquire,
        after_in_child=_global_shutdown_lock._at_fork_reinit,
        after_in_parent=_global_shutdown_lock.release,
    )


def _sync_handler(sync_q: SyncQueue[DataType], sync_q_task: SyncQTask):
    logger = logging.getLogger(sync_q_task.__qualname__)

    try:
        return sync_q_task(sync_q)
    except Exception as ex:
        logger.error(ex, exc_info=True)


class _WorkItem(object):
    def __init__(self, future: BaseFuture, coro: Coroutine) -> None:
        self.future = future
        self.coro = coro

    async def run(self):
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            result = await self.coro
        except Exception as ex:
            self.future.set_exception(ex)
            # Break a reference cycle with the exception 'ex'
            self = None
        else:
            self.future.set_result(result)

    __class_getitem__ = classmethod(types.GenericAlias)


def _worker(
    executor_reference: weakref.ReferenceType["ThreadPoolAsyncExecutor"],
    work_queue: queue.SimpleQueue,
    initializer,
    initargs,
):
    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception:
            _logger.critical("Exception in initializer:", exc_info=True)
            ref_instance = executor_reference()

            if ref_instance is not None:
                ref_instance._initializer_failed()

            return

    try:
        while True:
            work_item: _WorkItem = work_queue.get(block=True)

            if work_item is not None:
                asyncio.run(work_item.run())
                # Delete references to object. See issue16284
                del work_item

                # attempt to increment idle count
                ref_instance = executor_reference()

                if ref_instance is not None:
                    ref_instance._idle_semaphore.release()
                    del ref_instance

                continue

            ref_instance = executor_reference()
            # Exit if:
            #   - The interpreter is shutting down OR
            #   - The executor that owns the worker has been collected OR
            #   - The executor that owns the worker has been shutdown.
            if _shutdown or ref_instance is None or ref_instance._shutdown:
                # Flag the executor as shutting down as early as possible if it
                # is not gc-ed yet.
                if ref_instance is not None:
                    ref_instance._shutdown = True

                # Notice other workers
                work_queue.put(None)
                return

            del ref_instance
    except Exception:
        _logger.critical("Exception in worker", exc_info=True)


class BrokenThreadPool(BrokenExecutor):
    """
    Raised when a worker thread in a ThreadPoolExecutor failed initializing.
    """

    pass


class ThreadPoolAsyncExecutor(BaseExecutor):
    # Used to assign unique thread names when thread_name_prefix is not supplied.
    _counter = itertools.count().__next__

    def __init__(
        self, max_workers=None, thread_name_prefix="", initializer=None, initargs=()
    ):
        """Initializes a new ThreadPoolExecutor instance.

        Args:
            max_workers: The maximum number of threads that can be used to
                execute the given calls.
            thread_name_prefix: An optional name prefix to give our threads.
            initializer: A callable used to initialize worker threads.
            initargs: A tuple of arguments to pass to the initializer.
        """
        if max_workers is None:
            # ThreadPoolExecutor is often used to:
            # * CPU bound task which releases GIL
            # * I/O bound task (which releases GIL, of course)
            #
            # We use cpu_count + 4 for both types of tasks.
            # But we limit it to 32 to avoid consuming surprisingly large resource
            # on many core machine.
            max_workers = YALI_NUM_THREAD_WORKERS

        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        if initializer is not None and not callable(initializer):
            raise TypeError("initializer must be a callable")

        self._max_workers = max_workers
        self._work_queue = queue.SimpleQueue()
        self._idle_semaphore = threading.Semaphore(0)
        self._threads: MutableSet[threading.Thread] = set()
        self._broken = False
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

        self._thread_name_prefix = thread_name_prefix or (
            "ThreadPoolAsyncExecutor-%d" % self._counter()
        )

        self._initializer = initializer
        self._initargs = initargs

    def submit(self, coro: Coroutine):
        with self._shutdown_lock, _global_shutdown_lock:
            if self._broken:
                raise BrokenThreadPool(self._broken)

            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")

            if _shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after interpreter shutdown"
                )

            fut = BaseFuture()
            witem = _WorkItem(future=fut, coro=coro)

            self._work_queue.put(witem)
            self._adjust_thread_count()

            return fut

    submit.__doc__ = BaseExecutor.submit.__doc__

    def _adjust_thread_count(self):
        # if idle threads are available, don't spin new threads
        if self._idle_semaphore.acquire(timeout=0):
            return

        # When the executor gets lost, the weakref callback will wake up
        # the worker threads.
        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)

        if num_threads < self._max_workers:
            thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)
            t = threading.Thread(
                name=thread_name,
                target=_worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
            )

            t.start()
            self._threads.add(t)

            _threads_queues[t] = self._work_queue

    def _initializer_failed(self):
        with self._shutdown_lock:
            self._broken = (
                "A thread initializer failed, the thread pool is not usable anymore"
            )

            # Drain work queue and mark pending futures failed
            while True:
                try:
                    work_item: _WorkItem = self._work_queue.get_nowait()
                except queue.Empty:
                    break

                if work_item is not None:
                    work_item.future.set_exception(BrokenThreadPool(self._broken))

    def shutdown(self, wait=True, *, cancel_futures=False):
        with self._shutdown_lock:
            self._shutdown = True

            if cancel_futures:
                # Drain all work items from the queue, and then cancel their
                # associated futures.
                while True:
                    try:
                        work_item: _WorkItem = self._work_queue.get_nowait()
                    except queue.Empty:
                        break

                    if work_item is not None:
                        work_item.future.cancel()

            # Send a wake-up to prevent threads calling
            # _work_queue.get(block=True) from permanently blocking.
            self._work_queue.put(None)

        if wait:
            for t in self._threads:
                t.join()

    shutdown.__doc__ = BaseExecutor.shutdown.__doc__


class ThreadAio:
    def __init__(
        self,
        *,
        max_queue_size: int = 1000,
        sync_consumer: SyncConsumer | None = None,
        async_consumer: AsyncConsumer | None = None,
        sync_executor: ThreadPoolExecutor | None = None,
    ):
        queue_inst = _Queue(maxsize=max_queue_size)

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

            if item is YALI_SENTINEL:
                break

            try:
                self.__sync_consumer(item)
            except Exception as ex:
                _logger.error("Unhandled error while consuming message", exc_info=ex)

            self.__sync_q.task_done()

    async def aio_consume(self):
        if not self.__async_consumer:
            raise ValueError("Async consumer is not set")

        while True:
            item = await self.__async_q.get()

            if item is YALI_SENTINEL:
                break

            try:
                await self.__async_consumer(item)
            except Exception as ex:
                _logger.error("Unhandled error while consuming message", exc_info=ex)

            self.__async_q.task_done()

    def run_task(self, sync_q_task: SyncQTask):
        if not self.__sync_executor:
            _logger.warning(
                "Sync executor is not set explicitly. Using the default thread-pool-executor"
            )

        aio_loop = asyncio.get_running_loop()
        wrapped_fn = partial(_sync_handler, self.__sync_q, sync_q_task)

        return aio_loop.run_in_executor(self.__sync_executor, wrapped_fn)

    async def run_aio_task(self, async_q_task: AsyncQTask):
        try:
            return await async_q_task(self.__async_q)
        except Exception as ex:
            _logger.error(ex, exc_info=True)
