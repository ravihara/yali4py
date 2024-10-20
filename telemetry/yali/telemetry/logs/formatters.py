import datetime as dt
from logging import Formatter as LogFormatter

from yali.core.constants import YALI_LOG_DATETIME_FORMAT


class _BaseLogFormatter(LogFormatter):
    def formatException(self, exc_info):
        result = super().formatException(exc_info)
        result.replace("\n", " ")

        return repr(result)

    def format(self, record):
        s = super().format(record)

        if record.exc_text:
            s = s.replace("\n", " ")

        return s


class UTCLogFormatter(_BaseLogFormatter):
    def formatTime(self, record, datefmt=None):
        log_dt = dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc)

        if datefmt:
            dt_str = log_dt.strftime(datefmt)

            if datefmt.endswith("%f"):
                dt_str = dt_str[:-3]
        else:
            dt_str = log_dt.strftime(YALI_LOG_DATETIME_FORMAT)
            dt_str += f".{int(record.msecs):03d}"
            dt_str += log_dt.strftime("%z")

        return dt_str


class LocalLogFormatter(_BaseLogFormatter):
    def formatTime(self, record, datefmt=None):
        log_dt = dt.datetime.fromtimestamp(record.created)

        if datefmt:
            dt_str = log_dt.strftime(datefmt)

            if datefmt.endswith("%f"):
                dt_str = dt_str[:-3]
        else:
            dt_str = log_dt.strftime(YALI_LOG_DATETIME_FORMAT)
            dt_str += f".{int(record.msecs):03d}"
            dt_str += log_dt.strftime("%z")

        return dt_str
