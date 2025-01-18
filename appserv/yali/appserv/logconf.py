import logging
from http import HTTPStatus

from yali.core.logging.configs import (
    ROTATING_FILE_HANDLER_CLS,
    STREAM_LOG_HANDLER_CLS,
    effective_log_level,
    get_logfile_path,
)
from yali.core.logging.filters import get_filter_class_for_level
from yali.core.logging.formatters import DefaultLogFormatter, log_settings

_log_level = effective_log_level()


class AccessLogFormatter(DefaultLogFormatter):
    def get_status_code(self, status_code: int) -> str:
        try:
            status_phrase = HTTPStatus(status_code).phrase
        except ValueError:
            status_phrase = ""

        return f"{status_code} {status_phrase}"

    def extra_from_record(self, record: logging.LogRecord):
        extra_dict = super().extra_from_record(record)

        (
            client_addr,
            method,
            full_path,
            http_version,
            status_code,
        ) = record.args

        status_code = self.get_status_code(int(status_code))
        request_line = f"{method} {full_path} HTTP/{http_version}"
        extra_dict.update(
            {
                "client_addr": client_addr,
                "request_line": request_line,
                "status_code": status_code,
            }
        )

        return extra_dict


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
                "class": STREAM_LOG_HANDLER_CLS,
                "stream": "ext://sys.stderr",
                "level": _log_level,
            },
            "access_console": {
                "formatter": "access",
                "filters": ["default"],
                "class": STREAM_LOG_HANDLER_CLS,
                "stream": "ext://sys.stdout",
                "level": _log_level,
            },
        },
        "root": {"handlers": ["default_console"], "level": _log_level},
        "loggers": {
            "uvicorn": {
                "handlers": ["default_console"],
                "level": _log_level,
                "propagate": False,
            },
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
            "class": ROTATING_FILE_HANDLER_CLS,
            "level": _log_level,
            "filename": get_logfile_path(log_name=log_name),
            "encoding": "utf-8",
            "maxBytes": log_settings.max_log_file_bytes,
            "backupCount": log_settings.max_log_rotations,
            "mode": "a",
        }

        log_config["handlers"]["access_file"] = {
            "formatter": "default",
            "filters": ["default"],
            "class": ROTATING_FILE_HANDLER_CLS,
            "level": _log_level,
            "filename": get_logfile_path(log_name=f"{log_name}-access"),
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
