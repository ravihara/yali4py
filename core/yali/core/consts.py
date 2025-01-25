from os import cpu_count
from typing import List

_OS_CPU_COUNT = cpu_count() or 1

YALI_NUM_PROCESS_WORKERS = _OS_CPU_COUNT
YALI_NUM_THREAD_WORKERS = min(32, _OS_CPU_COUNT + 4)
YALI_SENTINEL = None
YALI_TAG_FIELD = "_yali_mdl_tag"

PASS_MESSAGE = "success"
FAIL_MESSAGE = "failure"

DEFAULT_DELIMITERS = " -_"
ALLCHARS_REGEX = r"[{}]+"
DEFAULT_COMPRESS_LEVEL = 6

SERVICE_INST_ID_KEY = "service.instance.id"
STREAM_LOG_HANDLER_CLS = "logging.StreamHandler"
ROTATING_FILE_HANDLER_CLS = "logging.handlers.RotatingFileHandler"
DEFAULT_LOG_FORMAT = "%(levelname)s - %(asctime)s - %(name)s - %(module)s - %(message)s"

ALLOWED_DATETIME_FORMATS: List[str] = [
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S.%f%Z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
]
