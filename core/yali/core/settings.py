import os
from collections import ChainMap
from enum import IntEnum, StrEnum
from typing import Annotated, ClassVar, List

from decouple import Config as EnvConfig
from decouple import RepositoryEnv, RepositoryIni

from yali.core.hooks import constr_num_hook
from yali.core.models import BaseModel

QueueSizeInt = Annotated[int, constr_num_hook(ge=1000)]
LogFileBytesInt = Annotated[int, constr_num_hook(ge=1048576)]
LogRotationsInt = Annotated[int, constr_num_hook(gt=1, le=100)]


class LogLevelName(StrEnum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"


class LogLevelNumber(IntEnum):
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    DEBUG = 10
    TRACE = 5


__env_config: EnvConfig | None = None


def env_config():
    global __env_config

    if __env_config:
        return __env_config

    env_files = os.getenv("ENV_FILES", ".env").split(",")
    conf_repos: List[RepositoryEnv | RepositoryIni] = []

    for env_file in env_files:
        env_file = env_file.strip()

        if env_file.endswith(".env"):
            conf_repos.append(RepositoryEnv(env_file))
        elif env_file.endswith(".ini"):
            conf_repos.append(RepositoryIni(env_file))

    if not conf_repos:
        raise ValueError(
            "No environment files found. Please set ENV_FILES environment variable with comma separated list of environment files (.env or .ini)"
        )

    __env_config = EnvConfig(ChainMap(*conf_repos))

    return __env_config


class LogSettings(BaseModel):
    _env_config: ClassVar[EnvConfig] = env_config()

    debug_enabled: bool = _env_config("DEBUG", default=False, cast=bool)
    logs_root_dir: str = _env_config("LOGS_ROOT_DIR", default="/tmp/logs")
    log_level: LogLevelName = _env_config(
        "LOG_LEVEL", default="INFO", cast=LogLevelName
    )
    enable_mproc_logging: bool = _env_config(
        "ENABLE_MPROC_LOGGING", default=True, cast=bool
    )
    log_queue_size: QueueSizeInt = _env_config(
        "LOG_QUEUE_SIZE", default=1_000_000, cast=QueueSizeInt
    )
    log_to_file: bool = _env_config("LOG_TO_FILE", default=False, cast=bool)
    max_log_file_bytes: LogFileBytesInt = _env_config(
        "MAX_LOG_FILE_BYTES", default=10_485_760, cast=LogFileBytesInt
    )
    max_log_rotations: LogRotationsInt = _env_config(
        "MAX_LOG_ROTATIONS", default=10, cast=LogRotationsInt
    )

    def __post_init__(self):
        if self.log_level == LogLevelName.DEBUG:
            self.debug_enabled = True
        elif self.debug_enabled:
            self.log_level = LogLevelName.DEBUG


__log_settings: LogSettings | None = None


def log_settings():
    global __log_settings

    if not __log_settings:
        __log_settings = LogSettings()

    return __log_settings
