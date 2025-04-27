from typing import Annotated, ClassVar, Dict, Literal

import grpc
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import Meter, MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Tracer, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .appconf import EnvConfig, env_config
from .common import id_by_sysinfo
from .consts import SERVICE_INST_ID_KEY
from .models import BaseModel
from .typebase import Constraint, SingletonMeta

OTelResourceAttrsStr = Annotated[
    str,
    Constraint.as_string(
        pattern=r"^service.name=[a-zA-Z0-9_.-]+,service.version=[a-zA-Z0-9_.-]+,deployment.environment=[a-zA-Z0-9_.-]+(,[a-zA-Z0-9_.]+=[a-zA-Z0-9_.,%&@\'\"\[\]-]+)*$"
    ),
]
IntervalMillisFloat = Annotated[float, Constraint.as_number(ge=1000)]


class TelemetrySettings(BaseModel):
    _env_config: ClassVar[EnvConfig] = env_config()

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

        if SERVICE_INST_ID_KEY not in self.resource_attributes:
            self.resource_attributes[SERVICE_INST_ID_KEY] = id_by_sysinfo()


class YaliTelemetry(metaclass=SingletonMeta):
    __settings = TelemetrySettings()

    def __init__(self):
        self._insecure: bool = True
        ssl_credentials: grpc.ChannelCredentials | None = None

        if self.__settings.otel_exporter_certificate is not None:
            self._insecure = False
            ssl_credentials = grpc.ssl_channel_credentials(
                root_certificates=open(
                    self.__settings.otel_exporter_certificate, "rb"
                ).read(),
                private_key=(
                    open(self.__settings.otel_exporter_privkey, "rb").read()
                    if self.__settings.otel_exporter_privkey
                    else None
                ),
                certificate_chain=(
                    open(self.__settings.otel_exporter_certchain, "rb").read()
                    if self.__settings.otel_exporter_certchain
                    else None
                ),
            )

        self._resource = Resource.create(attributes=self.__settings.resource_attributes)

        self._span_exporter = OTLPSpanExporter(
            endpoint=self.__settings.otel_exporter_endpoint,
            headers=self.__settings.otel_exporter_headers,
            insecure=self._insecure,
            credentials=ssl_credentials,
        )
        self._span_processor = BatchSpanProcessor(span_exporter=self._span_exporter)
        self._tracer_provider = TracerProvider(resource=self._resource)
        self._tracer_provider.add_span_processor(span_processor=self._span_processor)

        self._metric_exporter = OTLPMetricExporter(
            endpoint=self.__settings.otel_exporter_endpoint,
            headers=self.__settings.otel_exporter_headers,
            insecure=self._insecure,
            credentials=ssl_credentials,
        )
        self._metric_reader = PeriodicExportingMetricReader(
            exporter=self._metric_exporter,
            export_interval_millis=self.__settings.otel_export_interval_millis,
        )
        self._meter_provider = MeterProvider(
            metric_readers=[self._metric_reader], resource=self._resource
        )

        otel_trace.set_tracer_provider(tracer_provider=self._tracer_provider)
        otel_metrics.set_meter_provider(meter_provider=self._meter_provider)

        self._tracers: Dict[str, Tracer] = {}
        self._meters: Dict[str, Meter] = {}

    def get_tracer(self, *, mod_name: str, lib_version: str | None = None):
        """
        Returns an OpenTelemetry tracer for a given module.

        Parameters
        ----------
        mod_name: str
            The name of the module.
        lib_version: str | None, optional
            The version of the library.

        Returns
        -------
        Tracer
            An OpenTelemetry tracer.
        """
        tracer_name = f"{mod_name}|{lib_version}" if lib_version else mod_name

        if tracer_name not in self._tracers:
            self._tracers[tracer_name] = otel_trace.get_tracer(
                instrumenting_module_name=mod_name,
                instrumenting_library_version=lib_version,
            )

        return self._tracers[tracer_name]

    def get_meter(self, *, name: str, version: str = ""):
        """
        Returns an OpenTelemetry meter for a given name and version.

        Parameters
        ----------
        name: str
            The name of the meter.
        version: str, optional
            The version of the meter.

        Returns
        -------
        Meter
            An OpenTelemetry meter.
        """
        meter_name = f"{name}|{version}" if version else name

        if meter_name not in self._meters:
            self._meters[meter_name] = otel_metrics.get_meter(
                name=name, version=version
            )

        return self._meters[meter_name]
