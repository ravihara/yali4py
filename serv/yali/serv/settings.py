from typing import List

from yali.core.constants import YALI_NUM_PROCESS_WORKERS, YALI_NUM_THREAD_WORKERS
from yali.core.metatypes import NonEmptyStr, PositiveInt
from yali.core.models import BaseModel, field_specs
from yali.core.settings import env_config

_env_config = env_config()


class PathConfig(BaseModel):
    name: NonEmptyStr
    path: NonEmptyStr


class EndpointConfig(BaseModel):
    host: NonEmptyStr = field_specs(default="0.0.0.0")
    port: PositiveInt
    base_path: NonEmptyStr
    end_paths: List[PathConfig]


class PortalConfig(BaseModel):
    api: EndpointConfig
    web: EndpointConfig | None = field_specs(default=None)


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
