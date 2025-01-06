from yali.core.constants import YALI_NUM_PROCESS_WORKERS, YALI_NUM_THREAD_WORKERS
from yali.core.metatypes import PositiveInt
from yali.core.models import BaseModel
from yali.core.utils.common import env_config

_env_config = env_config()


class MicroServiceSettings(BaseModel):
    max_thread_workers: PositiveInt = _env_config(
        "MAX_THREAD_WORKERS", default=YALI_NUM_THREAD_WORKERS, cast=PositiveInt
    )
    max_process_workers: PositiveInt = _env_config(
        "MAX_PROCESS_WORKERS", default=YALI_NUM_PROCESS_WORKERS, cast=PositiveInt
    )


__micro_service_settings: MicroServiceSettings | None = None


def micro_service_settings():
    global __micro_service_settings

    if not __micro_service_settings:
        __micro_service_settings = MicroServiceSettings()

    return __micro_service_settings
