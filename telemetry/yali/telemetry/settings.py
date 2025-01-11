from typing import Annotated, Literal

from yali.core.hooks import constr_num_hook, constr_str_hook
from yali.core.models import BaseModel
from yali.core.settings import env_config
from yali.core.utils.common import id_by_sysinfo

_SERVICE_INST_ID_KEY = "service.instance.id"
_env_config = env_config()


OTelResourceAttrsStr = Annotated[
    str,
    constr_str_hook(
        pattern=r"^service.name=[a-zA-Z0-9_.-]+,service.version=[a-zA-Z0-9_.-]+,deployment.environment=[a-zA-Z0-9_.-]+(,[a-zA-Z0-9_.]+=[a-zA-Z0-9_.,%&@\'\"\[\]-]+)*$"
    ),
]
IntervalMillisFloat = Annotated[float, constr_num_hook(ge=1000)]


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


__telemetry_settings: TelemetrySettings | None = None


def telemetry_settings():
    global __telemetry_settings

    if not __telemetry_settings:
        __telemetry_settings = TelemetrySettings()

    return __telemetry_settings
