from enum import IntEnum, StrEnum
from typing import Annotated, Literal

from yali.core.hooks import constr_num_hook, constr_str_hook
from yali.core.models import BaseModel
from yali.core.utils.common import env_config, id_by_sysinfo

_SERVICE_INST_ID_KEY = "service.instance.id"
_env_config = env_config()


OTelResourceAttrsStr = Annotated[
    str,
    constr_str_hook(
        pattern=r"^service.name=[a-zA-Z0-9_.-]+,service.version=[a-zA-Z0-9_.-]+,deployment.environment=[a-zA-Z0-9_.-]+(,[a-zA-Z0-9_.]+=[a-zA-Z0-9_.,%&@\'\"\[\]-]+)*$"
    ),
]
QueueSizeInt = Annotated[int, constr_num_hook(ge=1000)]
LogFileBytesInt = Annotated[int, constr_num_hook(ge=1048576)]
LogRotationsInt = Annotated[int, constr_num_hook(gt=1, le=100)]
IntervalMillisFloat = Annotated[float, constr_num_hook(ge=1000)]


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


class LogSettings(BaseModel):
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


class TelemetrySettings(BaseModel):
    otel_exporter: Literal["otlp"] = "otlp"
    otel_exporter_headers: str = _env_config("OTEL_EXPORTER_OTLP_HEADERS")
    otel_exporter_endpoint: str = _env_config("OTEL_EXPORTER_OTLP_ENDPOINT")
    otel_resource_attributes: OTelResourceAttrsStr = _env_config(
        "OTEL_RESOURCE_ATTRIBUTES", cast=OTelResourceAttrsStr
    )
    otel_export_interval_millis: IntervalMillisFloat = _env_config(
        "OTEL_EXPORT_INTERVAL_MILLIS", default=5000, cast=IntervalMillisFloat
    )
    otel_exporter_certificate: str | None = _env_config(
        "OTEL_EXPORTER_OTLP_CERTIFICATE", default=None
    )
    otel_exporter_certchain: str | None = _env_config(
        "OTEL_EXPORTER_CERTCHAIN", default=None
    )
    otel_exporter_privkey: str | None = _env_config(
        "OTEL_EXPORTER_PRIVKEY", default=None
    )
    resource_attributes: dict = {}

    def __post_init__(self):
        if (
            self.otel_exporter_certchain is not None
            and self.otel_exporter_privkey is None
        ):
            raise ValueError(
                "If 'otel_exporter_certchain' is set, 'otel_exporter_privkey' must be set as well"
            )

        if (
            self.otel_exporter_certchain is None
            and self.otel_exporter_privkey is not None
        ):
            raise ValueError(
                "If 'otel_exporter_privkey' is set, 'otel_exporter_certchain' must be set as well"
            )

        resource_pairs = self.otel_resource_attributes.split(",")

        for resource_pair in resource_pairs:
            key, value = resource_pair.split("=")
            self.resource_attributes[key] = value

        if _SERVICE_INST_ID_KEY not in self.resource_attributes:
            self.resource_attributes[_SERVICE_INST_ID_KEY] = id_by_sysinfo()


__log_settings: LogSettings | None = None
__telemetry_settings: TelemetrySettings | None = None


def log_settings():
    global __log_settings

    if not __log_settings:
        __log_settings = LogSettings()

    return __log_settings


def telemetry_settings():
    global __telemetry_settings

    if not __telemetry_settings:
        __telemetry_settings = TelemetrySettings()

    return __telemetry_settings
