from logging import LogRecord, getLogger
from logging.config import dictConfig as dict_logging_config
from multiprocessing import Queue as LogQueue
from multiprocessing import current_process
from multiprocessing import get_context as mproc_get_context
from typing import Any, Callable, Dict

from yali.core.constants import YALI_BREAK_EVENT
from yali.core.metatypes import SingletonMeta
from yali.core.typings import FlexiTypesModel, NonEmptyStr
from yali.core.utils.strings import lower_with_hyphens

from ..settings import log_settings
from .configs import default_log_config
from .formatters import effective_log_level

_log_level = effective_log_level()
_log_settings = log_settings()


def init_mproc_logging(queue: LogQueue, is_main: bool = True):
    """
    Initialize multiprocessing logging for a given parent or child process

    Parameters
    ----------
    queue: LogQueue
        The multiprocessing queue to use
    is_main: bool, optional
        Is the current process the main process
    """
    dict_logging_config(
        config={
            "version": 1,
            "disable_existing_loggers": is_main,
            "handlers": {
                "q_handler": {
                    "class": "yali.telemetry.logging.handlers.MprocAsyncLogHandler",
                    "queue": queue,
                }
            },
            "root": {"handlers": ["q_handler"], "level": _log_level},
        }
    )


def _handle_mproc_logs(queue: LogQueue, config: Dict[str, Any]):
    dict_logging_config(config=config)
    curr_process_name = current_process().name

    while True:
        try:
            record: LogRecord = queue.get()

            if record is YALI_BREAK_EVENT:
                root_handler = getLogger()

                for hndl in root_handler.handlers:
                    hndl.flush()
                    hndl.close()

                print("Received break-event, exiting...")
                break

            if record.name == "root":
                logger = getLogger()
            else:
                logger = getLogger(record.name)

            if curr_process_name != record.processName:
                record.processName = f"{curr_process_name}->{record.processName}"

            logger.handle(record=record)
        except KeyboardInterrupt:
            print("In order to break processing logs, send a 'YALI_BREAK_EVENT' to the queue")
        except Exception:
            import sys
            import traceback

            print("Multi-process log processing failed", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


class LogOptions(FlexiTypesModel):
    name: NonEmptyStr = "yali_app"
    config: Dict[str, Any] | None = None
    post_hook: Callable[[], None] | None = None


class YaliLog(metaclass=SingletonMeta):
    def __init__(self, *, options: LogOptions):
        self._log_name = lower_with_hyphens(options.name)
        self._mproc_enabled = _log_settings.enable_mproc_logging

        log_config = options.config or default_log_config(log_name=self._log_name)

        if self._mproc_enabled:
            self._mproc_context = mproc_get_context("spawn")
            self._mproc_manager = self._mproc_context.Manager()
            self._mproc_queue = self._mproc_manager.Queue(maxsize=_log_settings.log_queue_size)

            init_mproc_logging(queue=self._mproc_queue, is_main=True)

            self._log_worker = self._mproc_context.Process(
                name=f"{current_process().name}-{self._log_name}",
                target=_handle_mproc_logs,
                kwargs={"queue": self._mproc_queue, "config": log_config},
            )

            self._log_worker.start()
        else:
            dict_logging_config(config=log_config)

        self._root = getLogger()
        self._app = getLogger(name=self._log_name)
        self._root.propagate = False

        if options.post_hook:
            options.post_hook()

    @property
    def name(self):
        return self._log_name

    @property
    def level(self):
        return _log_level

    @property
    def root_logger(self):
        return self._root

    @property
    def app_logger(self):
        return self._app

    @property
    def mproc_enabled(self):
        return self._mproc_enabled

    @property
    def mproc_queue(self):
        return self._mproc_queue

    def get_logger(self, name: str = ""):
        """Get a logger by name, or the app logger if no name is provided"""
        name = name.strip() if name else self._log_name

        if name:
            return getLogger(name=name)

        return self._app

    def close(self):
        """Close the logger"""
        if self._log_worker and self._log_worker.is_alive():
            self._mproc_queue.put(YALI_BREAK_EVENT)
            self._log_worker.join()
            self._log_worker.close()
