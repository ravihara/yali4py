from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from yali.core.constants import YALI_NUM_PROCESS_WORKERS, YALI_NUM_THREAD_WORKERS


class MicroServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file_encoding="utf-8", case_sensitive=True)

    max_thread_workers: int = Field(
        YALI_NUM_THREAD_WORKERS,
        validation_alias=AliasChoices("YALI_MAX_THREAD_WORKERS", "MAX_THREAD_WORKERS"),
        ge=1,
    )
    max_process_workers: int = Field(
        YALI_NUM_PROCESS_WORKERS,
        validation_alias=AliasChoices("YALI_MAX_PROCESS_WORKERS", "MAX_PROCESS_WORKERS"),
        ge=1,
    )


__micro_service_settings: MicroServiceSettings | None = None


def micro_service_settings():
    global __micro_service_settings

    if not __micro_service_settings:
        __micro_service_settings = MicroServiceSettings()

    return __micro_service_settings
