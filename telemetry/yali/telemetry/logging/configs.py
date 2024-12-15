import os

from yali.core.utils.common import filename_by_sysinfo
from yali.core.utils.osfiles import FilesConv

from .filters import get_filter_class_for_level
from .formatters import (
    AccessLogFormatter,
    DefaultLogFormatter,
    effective_log_level,
    log_settings,
)

__STREAM_LOG_HANDLER_CLS = "logging.StreamHandler"
__ROTATING_FILE_HANDLER_CLS = "logging.handlers.RotatingFileHandler"

_log_level = effective_log_level()


def _get_logfile_path(log_name: str):
    logs_root = log_settings.logs_root_dir
    log_filename = filename_by_sysinfo(basename=log_name, extension=".log")

    if FilesConv.is_dir_writable(dir_path=logs_root, check_creatable=True):
        os.makedirs(logs_root, exist_ok=True)
        logfile_path = os.path.join(logs_root, log_filename)

        return logfile_path

    return os.path.join(os.getcwd(), log_filename)


def default_log_config(log_name: str):
    log_filter_class = get_filter_class_for_level(_log_level)

    log_config = {
        "version": 1,
        "disable_existing_loggers": True,
        "filters": {"default": {"()": log_filter_class}},
        "formatters": {"default": {"()": DefaultLogFormatter}},
        "handlers": {
            "console": {
                "formatter": "default",
                "filters": ["default"],
                "class": __STREAM_LOG_HANDLER_CLS,
                "level": _log_level,
            },
        },
        "root": {"handlers": ["console"], "level": _log_level},
    }

    if log_settings.log_to_file:
        log_config["handlers"]["default_file"] = {
            "formatter": "default",
            "filters": ["default"],
            "class": __ROTATING_FILE_HANDLER_CLS,
            "level": _log_level,
            "filename": _get_logfile_path(log_name=log_name),
            "encoding": "utf-8",
            "maxBytes": log_settings.max_log_file_bytes,
            "backupCount": log_settings.max_log_rotations,
            "mode": "a",
        }
        log_config["root"]["handlers"].append("default_file")

    return log_config


def uvicorn_log_config(log_name: str):
    log_filter_class = get_filter_class_for_level(_log_level)

    log_config = {
        "version": 1,
        "disable_existing_loggers": True,
        "filters": {"default": {"()": log_filter_class}},
        "formatters": {
            "default": {"()": DefaultLogFormatter},
            "access": {
                "()": AccessLogFormatter,
                "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            },
        },
        "handlers": {
            "default_console": {
                "formatter": "default",
                "filters": ["default"],
                "class": __STREAM_LOG_HANDLER_CLS,
                "stream": "ext://sys.stderr",
                "level": _log_level,
            },
            "access_console": {
                "formatter": "access",
                "filters": ["default"],
                "class": __STREAM_LOG_HANDLER_CLS,
                "stream": "ext://sys.stdout",
                "level": _log_level,
            },
        },
        "root": {"handlers": ["default_console"], "level": _log_level},
        "loggers": {
            "uvicorn": {"handlers": ["default_console"], "level": _log_level, "propagate": False},
            "uvicorn.error": {"handlers": ["default_console"], "level": _log_level},
            "uvicorn.access": {
                "handlers": ["access_console"],
                "level": _log_level,
                "propagate": False,
            },
        },
    }

    if log_settings.log_to_file:
        log_config["handlers"]["default_file"] = {
            "formatter": "default",
            "filters": ["default"],
            "class": __ROTATING_FILE_HANDLER_CLS,
            "level": _log_level,
            "filename": _get_logfile_path(log_name=log_name),
            "encoding": "utf-8",
            "maxBytes": log_settings.max_log_file_bytes,
            "backupCount": log_settings.max_log_rotations,
            "mode": "a",
        }

        log_config["handlers"]["access_file"] = {
            "formatter": "default",
            "filters": ["default"],
            "class": __ROTATING_FILE_HANDLER_CLS,
            "level": _log_level,
            "filename": _get_logfile_path(log_name=f"{log_name}-access"),
            "encoding": "utf-8",
            "maxBytes": log_settings.max_log_file_bytes,
            "backupCount": log_settings.max_log_rotations,
            "mode": "a",
        }

        log_config["root"]["handlers"].append("default_file")

        log_config["loggers"]["uvicorn"]["handlers"].append("default_file")
        log_config["loggers"]["uvicorn.error"]["handlers"].append("default_file")
        log_config["loggers"]["uvicorn.access"]["handlers"].append("access_file")

    return log_config
