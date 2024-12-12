import json
import logging
from decimal import Decimal
from typing import Dict

from opentelemetry import trace
from opentelemetry.trace.span import INVALID_SPAN
from yali.core.constants import YALI_LOG_DATETIME_FORMAT
from yali.core.settings import CommonSettings, LogLevelName
from yali.core.utils.datetimes import DateTimeConv

_YALI_RECORD_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "otelTraceID",
    "otelSpanID",
}

log_settings = CommonSettings()


def _json_serializable(obj):
    try:
        return obj.__dict__
    except AttributeError:
        return str(obj)


def effective_log_level():
    """Return the effective log level based on debug mode and log level setting"""
    log_level: LogLevelName = "DEBUG" if log_settings.debug_enabled else log_settings.log_level

    return log_level


class YaliJsonLogFormatter(logging.Formatter):
    _keep_attr_types = (bool, int, float, Decimal, complex, str, DateTimeConv.mod.datetime)

    def format(self, record):
        message = record.getMessage()
        extra = self.extra_from_record(record)
        json_record = self.json_record(message, extra, record)
        mutated_record = self.marshal_time_attrs(json_record)

        # Backwards compatibility: Functions that overwrite marshal_time_attrs
        #  but don't return a new value will return None because they modified
        # the argument passed in.
        if mutated_record is None:
            mutated_record = json_record

        return self.to_json(mutated_record)

    def to_json(self, record: Dict):
        """Converts record dict to a JSON string.

        It makes best effort to serialize a record (represents an object as a string)
        instead of raising TypeError if json library supports default argument.
        Note, ujson doesn't support it.
        ValueError and OverflowError are also caught to avoid crashing an app,
        e.g., due to circular reference.

        Override this method to change the way dict is converted to JSON.
        """
        try:
            return json.dumps(record, default=_json_serializable)
        except (TypeError, ValueError, OverflowError):
            try:
                return json.dumps(record)
            except (TypeError, ValueError, OverflowError):
                return "{}"

    def extra_from_record(self, record: logging.LogRecord):
        """Returns `extra` dict you passed to logger.

        The `extra` keyword argument is used to populate the `__dict__` of
        the `LogRecord`.
        """
        return {
            attr_name: record.__dict__[attr_name]
            for attr_name in record.__dict__
            if attr_name not in _YALI_RECORD_ATTRS
        }

    def json_record(self, message: str, extra: Dict, record: logging.LogRecord):
        """
        Prepares a JSON payload which will be logged.
        Override this method to change JSON log format.

        Parameters
        ----------
        message: str
            Log message, e.g., `logger.info(msg='Sign up')`.
        extra: Dict
            Dictionary that was passed as `extra` param
            `logger.info('Sign up', extra={'referral_code': '52d6ce'})`.
        record: LogRecord
            `LogRecord` we got from `JSONFormatter.format()`.

        Returns
        -------
        Dict
            Dictionary which will be passed to JSON lib
        """
        extra["message"] = message
        extra["levelname"] = record.levelname
        extra["name"] = record.name
        extra["processName"] = record.processName

        if effective_log_level() == "DEBUG":
            extra["module"] = record.module
            extra["pathname"] = record.pathname
            extra["filename"] = record.filename
            extra["funcName"] = record.funcName
            extra["lineno"] = record.lineno
            extra["process"] = record.process
            extra["thread"] = record.thread
            extra["threadName"] = record.threadName

        if hasattr(record, "stack_info"):
            extra["stack_info"] = record.stack_info
        else:
            extra["stack_info"] = None

        if "asctime" not in extra:
            extra["asctime"] = record.asctime

        if "utctime" not in extra:
            extra["utctime"] = DateTimeConv.get_current_utc_time()

        if record.exc_info:
            extra["exc_info"] = self.formatException(record.exc_info)

        curr_span = trace.get_current_span()

        if curr_span != INVALID_SPAN:
            span_context = curr_span.get_span_context()

            if "otelTraceID" not in extra:
                extra["otelTraceID"] = trace.format_trace_id(span_context.trace_id)

            if "otelSpanID" not in extra:
                extra["otelSpanID"] = trace.format_span_id(span_context.span_id)

        return {
            k: v if (v is None) or isinstance(v, self._keep_attr_types) else str(v)
            for k, v in extra.items()
        }

    def marshal_time_attrs(self, json_record: Dict):
        """Override it to convert fields of `json_record` to needed types."""
        for attr_name in json_record:
            attr = json_record[attr_name]

            if isinstance(attr, DateTimeConv.mod.datetime):
                attr_str = attr.strftime(YALI_LOG_DATETIME_FORMAT)[:-3]
                json_record[attr_name] = attr_str + attr.strftime("%z")

        return json_record
