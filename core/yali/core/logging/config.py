import os
from multiprocessing import Queue as MprocQueue
from typing import Any, Dict

from ..common import filename_by_sysinfo
from ..consts import (
    DEFAULT_LOG_FORMAT,
    ROTATING_FILE_HANDLER_CLS,
    STREAM_LOG_HANDLER_CLS,
)
from ..osfiles import FSNode
from ..settings import log_settings
from .filters import get_filter_class_for_level
from .formatters import DefaultLogFormatter, effective_log_level

__log_settings = log_settings()
__log_level = effective_log_level()


def get_logfile_path(log_name: str):
    logs_root = __log_settings.logs_root_dir
    log_filename = filename_by_sysinfo(basename=log_name, extension=".log")

    if FSNode.is_dir_writable(dir_path=logs_root, check_creatable=True):
        os.makedirs(logs_root, exist_ok=True)
        logfile_path = os.path.join(logs_root, log_filename)

        return logfile_path

    return os.path.join(os.getcwd(), log_filename)


def default_log_config(log_name: str, log_format: str = DEFAULT_LOG_FORMAT):
    log_handlers: Dict[str, Dict[str, Any]] = {
        "console": {
            "formatter": "standard",
            "filters": ["standard"],
            "class": STREAM_LOG_HANDLER_CLS,
            "level": __log_level,
        }
    }
    root_handlers = ["console"]

    if __log_settings.log_to_file:
        log_handlers["rotated_file"] = {
            "class": ROTATING_FILE_HANDLER_CLS,
            "formatter": "standard",
            "filters": ["standard"],
            "level": __log_level,
            "filename": get_logfile_path(log_name=log_name),
            "encoding": "utf-8",
            "maxBytes": __log_settings.max_log_file_bytes,
            "backupCount": __log_settings.max_log_rotations,
            "mode": "a",
        }

        root_handlers.append("rotated_file")

    log_config = {
        "version": 1,
        "disable_existing_loggers": True,
        "filters": {"standard": {"()": get_filter_class_for_level(__log_level)}},
        "formatters": {"standard": {"()": DefaultLogFormatter, "format": log_format}},
        "handlers": log_handlers,
        "root": {"handlers": root_handlers, "level": __log_level},
    }

    return log_config


__yali_log_config = default_log_config(log_name="yali_app")


def mproc_qlog_config(
    log_queue: MprocQueue,
    *,
    log_config: Dict[str, Any] = __yali_log_config,
    log_format: str = DEFAULT_LOG_FORMAT,
    is_main_process: bool = False,
):
    config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": is_main_process,
        "filters": {"standard": {"()": get_filter_class_for_level(__log_level)}},
        "formatters": {"standard": {"()": DefaultLogFormatter, "format": log_format}},
        "handlers": {
            "queue_listener": {
                "class": "yali.core.logging.handlers.MprocLogQueueHandler",
                "log_queue": log_queue,
                "log_config": log_config,
                "is_main_process": is_main_process,
            }
        },
        "root": {"handlers": ["queue_listener"], "level": __log_level},
    }

    return config
