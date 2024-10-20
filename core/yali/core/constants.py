from os import cpu_count

_OS_CPU_COUNT = cpu_count() or 1

YALI_NUM_PROC_WORKERS = _OS_CPU_COUNT
YALI_NUM_THREAD_WORKERS = min(32, _OS_CPU_COUNT + 4)
YALI_BREAK_EVENT = None

YALI_LOG_QUEUE_SIZE = 1_000_000
YALI_LOG_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
YALI_LOG_MESSAGE_FORMAT = "%(asctime)s %(levelname)s %(message)s"
