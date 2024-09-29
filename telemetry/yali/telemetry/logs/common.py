YALI_LOG_QUEUE_SIZE = 1_000_000
YALI_LOG_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
YALI_LOG_MESSAGE_FORMAT = "%(asctime)s %(levelname)s %(message)s"
YALI_LOG_LEVELS = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]

from enum import StrEnum, IntEnum


class LogLevelName(StrEnum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


class LogLevelNumber(IntEnum):
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    DEBUG = 10
