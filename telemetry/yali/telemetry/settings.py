import os
from enum import IntEnum, StrEnum
from typing import Annotated, Literal

from pydantic import (
    AliasChoices,
    Field,
    StringConstraints,
    computed_field,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from yali.core.utils.common import id_by_sysinfo

_SERVICE_INST_ID_KEY = "service.instance.id"


OTelResourceAttrsStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        to_lower=True,
        pattern=r"^service.name=[a-zA-Z0-9_.-]+,service.version=[a-zA-Z0-9_.-]+,deployment.environment=[a-zA-Z0-9_.-]+(,[a-zA-Z0-9_.]+=[a-zA-Z0-9_.,%&@\'\"\[\]-]+)*$",
    ),
]


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


class LogSettings(BaseSettings):
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


class TelemetrySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file_encoding="utf-8", case_sensitive=True)

    otel_exporter: Literal["otlp"] = "otlp"
    otel_exporter_headers: str = Field(
        ...,
        validation_alias=AliasChoices("YALI_OTEL_EXPORTER_HEADERS", "OTEL_EXPORTER_OTLP_HEADERS"),
    )
    otel_exporter_endpoint: str = Field(
        ...,
        validation_alias=AliasChoices("YALI_OTEL_EXPORTER_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"),
    )
    otel_resource_attributes: OTelResourceAttrsStr = Field(
        ...,
        validation_alias=AliasChoices("YALI_OTEL_RESOURCE_ATTRIBUTES", "OTEL_RESOURCE_ATTRIBUTES"),
    )
    otel_export_interval_millis: float = Field(
        5000,
        ge=1000,
        validation_alias=AliasChoices("YALI_OTEL_EXPORT_INTERVAL_MILLIS"),
    )
    otel_exporter_certificate: str | None = Field(
        None,
        validation_alias=AliasChoices(
            "YALI_OTEL_EXPORTER_CERTIFICATE", "OTEL_EXPORTER_OTLP_CERTIFICATE"
        ),
    )
    otel_exporter_certchain: str | None = Field(
        None,
        validation_alias=AliasChoices("YALI_OTEL_EXPORTER_CERTCHAIN"),
    )
    otel_exporter_privkey: str | None = Field(
        None,
        validation_alias=AliasChoices("YALI_OTEL_EXPORTER_PRIVKEY"),
    )

    @computed_field
    @property
    def resource_attributes(self):
        res_attributes = {}
        resource_pairs = self.otel_resource_attributes.split(",")

        for resource_pair in resource_pairs:
            key, value = resource_pair.split("=")
            res_attributes[key] = value

        if _SERVICE_INST_ID_KEY not in res_attributes:
            res_attributes[_SERVICE_INST_ID_KEY] = id_by_sysinfo()

        return res_attributes

    @model_validator(mode="after")
    def check_certchain_and_privkey(self):
        if self.otel_exporter_certchain is not None and self.otel_exporter_privkey is None:
            raise ValueError(
                "If 'otel_exporter_certchain' is set, 'otel_exporter_privkey' must be set as well"
            )

        if self.otel_exporter_certchain is None and self.otel_exporter_privkey is not None:
            raise ValueError(
                "If 'otel_exporter_privkey' is set, 'otel_exporter_certchain' must be set as well"
            )

        return self
