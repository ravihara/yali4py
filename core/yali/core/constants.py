from os import cpu_count

_OS_CPU_COUNT = cpu_count() or 1

YALI_NUM_PROCESS_WORKERS = _OS_CPU_COUNT
YALI_NUM_THREAD_WORKERS = min(32, _OS_CPU_COUNT + 4)
YALI_BREAK_EVENT = None

PASS_MESSAGE = "success"
FAIL_MESSAGE = "failure"
