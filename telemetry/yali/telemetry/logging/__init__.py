import os
from logging import LogRecord, getLogger
from logging.config import dictConfig as dict_logging_config
from multiprocessing import Queue as LogQueue
from multiprocessing import current_process
from typing import Any, Callable, Dict

from yali.core.constants import YALI_BREAK_EVENT, YALI_LOG_QUEUE_SIZE
from yali.core.metatypes import YaliSingleton
from yali.core.typings import (
    Field,
    FlexiTypesModel,
    MultiProcContext,
    NonEmptyStr,
    model_validator,
)
from yali.core.utils.common import filename_by_sysinfo
from yali.core.utils.osfiles import FilesConv
from yali.core.utils.strings import lower_with_hyphens

from .filters import get_filter_class_for_level
from .formatters import YaliJsonLogFormatter, effective_log_level, log_settings

_log_level = effective_log_level()


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


class YaliLogOptions(FlexiTypesModel):
    name: NonEmptyStr = "yali_app"
    config: Dict[str, Any] | None = None
    mproc_enabled: bool = False
    mproc_queue_size: int = Field(YALI_LOG_QUEUE_SIZE, ge=1000)
    mproc_context: MultiProcContext | None = None
    post_hook: Callable[[], None] | None = None

    @model_validator(mode="after")
    def ensure_mproc_context(self):
        if self.mproc_enabled and self.mproc_context is None:
            raise ValueError("If 'mproc_enabled' is set, 'mproc_context' must be set as well")

        return self


class YaliLog(metaclass=YaliSingleton):
    def __init__(self, *, options: YaliLogOptions):
        self._log_name = lower_with_hyphens(options.name)
        self._mproc_enabled = options.mproc_enabled

        log_config = options.config or self._default_config()

        if self._mproc_enabled:
            assert options.mproc_context

            self._mproc_context = options.mproc_context
            self._mproc_manager = self._mproc_context.Manager()
            self._mproc_queue = self._mproc_manager.Queue(maxsize=options.mproc_queue_size)

            init_mproc_logging(queue=self._mproc_queue, is_main=True)

            self._log_worker = self._mproc_context.Process(
                name=f"{current_process().name}-{self._log_name}",
                target=_handle_mproc_logs,
                kwargs={"queue": self._mproc_queue, "config": log_config},
            )

            self._log_worker.start()
        else:
            dict_logging_config(config=log_config)

        if options.post_hook:
            options.post_hook()

        self._root = getLogger()
        self._app = getLogger(name=self._log_name)
        self._root.propagate = False

    def _default_config(self):
        log_filter_class = get_filter_class_for_level(_log_level)

        log_config = {
            "version": 1,
            "disable_existing_loggers": True,
            "filters": {"yali_default": {"()": log_filter_class}},
            "formatters": {"yali_default": {"()": YaliJsonLogFormatter}},
            "handlers": {
                "console": {
                    "formatter": "yali_default",
                    "filters": ["yali_default"],
                    "class": "logging.StreamHandler",
                    "level": _log_level,
                },
            },
            "root": {"handlers": ["console"], "level": _log_level},
        }

        if log_settings.log_to_file:
            log_config["handlers"]["rotating_file"] = {
                "formatter": "yali_default",
                "filters": ["yali_default"],
                "class": "logging.handlers.RotatingFileHandler",
                "level": _log_level,
                "filename": self._get_logfile_path(),
                "encoding": "utf-8",
                "maxBytes": log_settings.max_log_file_bytes,
                "backupCount": log_settings.max_log_rotations,
                "mode": "a",
            }
            log_config["root"]["handlers"].append("rotating_file")

        return log_config

    def _get_logfile_path(self):
        logs_root = log_settings.logs_root_dir
        log_filename = filename_by_sysinfo(basename=self._log_name, extension=".log")

        if FilesConv.is_dir_writable(dir_path=logs_root, check_creatable=True):
            os.makedirs(logs_root, exist_ok=True)
            logfile_path = os.path.join(logs_root, log_filename)

            return logfile_path

        return os.path.join(os.getcwd(), log_filename)

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
