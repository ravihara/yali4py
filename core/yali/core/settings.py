import os
from enum import IntEnum, StrEnum

from pydantic import AliasChoices, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


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


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file_encoding="utf-8", case_sensitive=True)

    logs_root_dir: str = Field(
        "/tmp/logs", validation_alias=AliasChoices("YALI_LOGS_ROOT_DIR", "LOGS_ROOT_DIR")
    )
    log_level: LogLevelName = Field(
        "INFO", validation_alias=AliasChoices("YALI_LOG_LEVEL", "LOG_LEVEL")
    )
    log_to_file: bool = Field(
        False, validation_alias=AliasChoices("YALI_LOG_TO_FILE", "LOG_TO_FILE")
    )
    max_log_file_bytes: int = Field(
        10485760, validation_alias=AliasChoices("YALI_MAX_LOG_FILE_BYTES", "MAX_LOG_FILE_BYTES")
    )
    max_log_rotations: int = Field(
        10, validation_alias=AliasChoices("YALI_MAX_LOG_ROTATIONS", "MAX_LOG_ROTATIONS")
    )

    @computed_field
    @property
    def debug_enabled(self):
        with_debug_env = bool(os.getenv("DEBUG", False))

        if (self.log_level == LogLevelName.DEBUG) or with_debug_env:
            return True

        return False
