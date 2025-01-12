from logging import Filter as LogFilter
from typing import Dict

from ..settings import LogLevelName, LogLevelNumber


class CriticalLogFilter(LogFilter):
    def filter(self, record):
        return record.levelno >= LogLevelNumber.CRITICAL


class ErrorLogFilter(LogFilter):
    def filter(self, record):
        return record.levelno >= LogLevelNumber.ERROR


class WarningLogFilter(LogFilter):
    def filter(self, record):
        return record.levelno >= LogLevelNumber.WARNING


class InfoLogFilter(LogFilter):
    def filter(self, record):
        return record.levelno >= LogLevelNumber.INFO


class DebugLogFilter(LogFilter):
    def filter(self, record):
        return record.levelno >= LogLevelNumber.DEBUG


class TraceLogFilter(LogFilter):
    def filter(self, record):
        return record.levelno >= LogLevelNumber.TRACE


FilterForLogLevel: Dict[LogLevelName, type[LogFilter]] = {
    LogLevelName.CRITICAL: CriticalLogFilter,
    LogLevelName.ERROR: ErrorLogFilter,
    LogLevelName.WARNING: WarningLogFilter,
    LogLevelName.INFO: InfoLogFilter,
    LogLevelName.DEBUG: DebugLogFilter,
    LogLevelName.TRACE: TraceLogFilter,
}


def get_filter_class_for_level(log_level: str):
    """
    Returns the filter class for a given log level

    Parameters
    ----------
    log_level: str
        The log level

    Returns
    -------
    type[LogFilter]
        The filter class
    """
    _level = log_level

    if _level == "WARN":
        _level = LogLevelName.WARNING
    elif _level == "FATAL":
        _level = LogLevelName.CRITICAL

    if _level in FilterForLogLevel:
        return FilterForLogLevel[_level]

    return InfoLogFilter
