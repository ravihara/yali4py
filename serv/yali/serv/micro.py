import asyncio
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Queue as LogQueue
from typing import Any, Callable, Coroutine, Dict, List, Tuple

from yali.core.errors import YaliError
from yali.core.metatypes import (
    Awaitable,
    AwaitableDoneHandler,
    AwaitCondition,
    PoolExecutorInitFunc,
)
from yali.core.models import AioExceptValue
from yali.telemetry.logging import LogOptions, YaliLog, init_mproc_logging

from .settings import micro_service_settings

_mserv_settings = micro_service_settings()


def subprocess_handler(log_queue: LogQueue, proc_func: Callable, *fnargs, **fnkwargs):
    if log_queue:
        init_mproc_logging(queue=log_queue, is_main=False)

    proc_logger = logging.getLogger(proc_func.__name__)

    try:
        return proc_func(*fnargs, **fnkwargs)
    except Exception as ex:
        proc_logger.error(ex, exc_info=True)


class YaliMicro(ABC):
    def __init__(
        self,
        *,
        service_name: str,
        log_config: Dict[str, Any] | None = None,
        thread_init_func: PoolExecutorInitFunc | None = None,
        thread_init_args: Tuple = (),
        process_init_func: PoolExecutorInitFunc | None = None,
        process_init_args: Tuple = (),
    ):
        self._service_name = service_name

        self.__aio_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.__aio_loop)

        log_options = LogOptions(
            name=service_name, config=log_config, post_hook=self.post_log_setup
        )

        self.__app_log: YaliLog = YaliLog(options=log_options)
        self._logger = self.__app_log.get_logger(name=service_name)

        self.__thread_pool_executor = ThreadPoolExecutor(
            max_workers=_mserv_settings.max_thread_workers,
            thread_name_prefix=f"{self._service_name}_thrd:",
            initializer=thread_init_func,
            initargs=thread_init_args,
        )

        self.__process_pool_executor = ThreadPoolExecutor(
            max_workers=_mserv_settings.max_process_workers,
            thread_name_prefix=f"{self._service_name}_proc:",
            initializer=process_init_func,
            initargs=process_init_args,
        )

    def __shutdown(self):
        if self.__thread_pool_executor:
            self._logger.info(
                f"Shutting down thread-pool executor of {self._service_name} service"
            )
            self.__thread_pool_executor.shutdown()

        if self.__process_pool_executor:
            self._logger.info(
                f"Shutting down process-pool executor of {self._service_name} service"
            )
            self.__process_pool_executor.shutdown()

        tasks = asyncio.all_tasks(loop=self.__aio_loop)

        for task in tasks:
            if not task.done():
                task.cancel()
                print(f"Task {task.get_name()} cancelled")

        self.__app_log.close()
        root_logger = logging.getLogger()

        for hndl in root_logger.handlers:
            hndl.flush()
            hndl.close()

    @staticmethod
    def __release_waiter(waiter: asyncio.Future):
        if not waiter.done():
            waiter.set_result(None)

    @staticmethod
    def __finalize_awaitables(
        awaitables: Dict[str, Awaitable], done_cb: AwaitableDoneHandler
    ):
        results: Dict[str, Any] = dict()

        for k, v in awaitables.items():
            v.remove_done_callback(done_cb)

            if v.done():
                if v.cancelled():
                    exc = asyncio.CancelledError(f"Job {k} cancelled internally")
                    res = AioExceptValue(name=type(exc).__name__, exc_info=exc)

                    results[k] = res
                else:
                    exc = v.exception()

                    if not exc:
                        results[k] = v.result()
                    else:
                        res = AioExceptValue(name=type(exc).__name__, exc_info=exc)
                        results[k] = res
            else:
                v.cancel()
                exc = asyncio.CancelledError(f"Pending job {k} cancelled")
                res = AioExceptValue(name=type(exc).__name__, exc_info=exc)

                results[k] = res

        return results

    @staticmethod
    def coros_to_tasks(coros: List[Coroutine]) -> List[asyncio.Task]:
        idx: int = 1
        tasks: List[asyncio.Task] = []

        for cr in coros:
            tasks.append(asyncio.create_task(coro=cr, name=f"{cr.__name__}_{idx}"))
            idx += 1

        return tasks

    async def gather_awaitables(
        self,
        awaitables: Dict[str, Awaitable],
        *,
        wait_for: AwaitCondition = AwaitCondition.ALL_DONE,
        wait_till_sec: float = -1.0,
    ) -> Dict[str, Any]:
        awt_counter = len(awaitables)
        assert awt_counter > 0, "Awaitables dictionary is empty"

        if awt_counter > 9999:
            raise YaliError("Maxium number of awaitables allowed is 9999")

        run_loop = asyncio.get_running_loop()
        waiter = run_loop.create_future()
        timeout_hndl = None

        if wait_till_sec > 0:
            timeout_hndl = run_loop.call_later(
                wait_till_sec, self.__release_waiter, waiter
            )

        def completion_cb(f: asyncio.Future):
            nonlocal awt_counter
            awt_counter -= 1

            if (
                awt_counter <= 0
                or (wait_for == AwaitCondition.ANY_DONE)
                or (
                    (wait_for == AwaitCondition.ANY_FAIL)
                    and (not f.cancelled() and f.exception() is not None)
                )
            ):
                if timeout_hndl is not None:
                    timeout_hndl.cancel()

                if not waiter.done():
                    waiter.set_result(None)

        for k, v in awaitables.items():
            # NOTE: We must convert a Coroutine into a Task
            if asyncio.coroutines.iscoroutine(v):
                v = asyncio.create_task(v)
                awaitables[k] = v

            v.add_done_callback(completion_cb)

        try:
            await waiter
        except asyncio.CancelledError:
            self._logger.warning("Awaitables result cancelled")
        finally:
            if timeout_hndl is not None:
                timeout_hndl.cancel()

        return self.__finalize_awaitables(awaitables=awaitables, done_cb=completion_cb)

    async def gather_coroutines(
        self,
        coros: List[Coroutine],
        *,
        wait_for: AwaitCondition = AwaitCondition.ALL_DONE,
        wait_till_sec: float = -1.0,
    ) -> Dict[str, Any]:
        tasks = self.coros_to_tasks(coros)
        results: Dict[str, Any] = dict()

        if wait_till_sec > 0:
            done, pending = await asyncio.wait(
                tasks, timeout=wait_till_sec, return_when=wait_for
            )
        else:
            done, pending = await asyncio.wait(tasks, return_when=wait_for)

        for dtask in done:
            tn = dtask.get_name()

            if dtask.cancelled():
                exc = asyncio.CancelledError(f"Job {tn} cancelled internally")
                res = AioExceptValue(name=type(exc).__name__, exc_info=exc)

                results[tn] = res
            else:
                exc = dtask.exception()

                if not exc:
                    results[tn] = dtask.result()
                else:
                    res = AioExceptValue(name=type(exc).__name__, exc_info=exc)
                    results[tn] = res

        for ptask in pending:
            tn = ptask.get_name()

            ptask.cancel()
            exc = asyncio.CancelledError(f"Pending job {tn} cancelled")
            res = AioExceptValue(name=type(exc).__name__, exc_info=exc)

            results[tn] = res

        return results

    def run_task_in_thread(self, func: Callable, *fnargs, **fnkwargs) -> asyncio.Future:
        wrapped_fn = partial(func, *fnargs, **fnkwargs)
        future = self.__aio_loop.run_in_executor(
            self.__thread_pool_executor, wrapped_fn
        )

        return future

    def run_task_in_subprocess(
        self, func: Callable, *fnargs, **fnkwargs
    ) -> asyncio.Future:
        if self.__app_log.mproc_queue:
            wrapped_fn = partial(
                subprocess_handler,
                self.__app_log.mproc_queue,
                func,
                *fnargs,
                **fnkwargs,
            )
            future = self.__aio_loop.run_in_executor(
                self.__process_pool_executor, wrapped_fn
            )
        else:
            wrapped_fn = partial(func, *fnargs, **fnkwargs)
            future = self.__aio_loop.run_in_executor(
                self.__process_pool_executor, wrapped_fn
            )

        return future

    @abstractmethod
    def post_log_setup(self):
        pass

    @abstractmethod
    async def start(self):
        """This should typically be run in a loop. When this function returns, the main loop is stopped."""
        raise NotImplementedError

    @abstractmethod
    async def stop(self):
        """This should gracefully stop the service."""
        raise NotImplementedError

    async def run_loop(self):
        if self.__aio_loop.is_running():
            raise YaliError(f"The main-loop of {self._service_name} is already running")

        try:
            self.__aio_loop.run_until_complete(self.start())
        except (KeyboardInterrupt, asyncio.CancelledError):
            self.__aio_loop.run_until_complete(self.stop())
        except Exception as ex:
            print(f"Error during main-loop execution: {ex}")
            self.__aio_loop.run_until_complete(self.stop())
        finally:
            try:
                self.__shutdown()
            except asyncio.CancelledError:
                pass

            logging.shutdown()
            self.__aio_loop.stop()
            self.__aio_loop.close()
            print(f"All done for service - {self._service_name}. Bye!")
