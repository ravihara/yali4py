from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider, Tracer
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

from yali.core.metatypes import YaliSingleton
from yali.core.settings import OTelOTLPSettings
from opentelemetry import metrics as otel_metrics
from opentelemetry.sdk.metrics import MeterProvider, Meter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

from typing import Dict
import grpc


class YaliTelemetry(metaclass=YaliSingleton):
    __settings = OTelOTLPSettings()

    def __init__(self):
        self._insecure: bool = True
        ssl_credentials: grpc.ChannelCredentials | None = None

        if self.__settings.otel_exporter_certificate is not None:
            self._insecure = False
            ssl_credentials = grpc.ssl_channel_credentials(
                root_certificates=open(self.__settings.otel_exporter_certificate, "rb").read(),
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
        tracer_name = f"{mod_name}|{lib_version}" if lib_version else mod_name

        if tracer_name not in self._tracers:
            self._tracers[tracer_name] = otel_trace.get_tracer(
                instrumenting_module_name=mod_name, instrumenting_library_version=lib_version
            )

        return self._tracers[tracer_name]

    def get_meter(self, *, name: str, version: str = ""):
        meter_name = f"{name}|{version}" if version else name

        if meter_name not in self._meters:
            self._meters[meter_name] = otel_metrics.get_meter(name=name, version=version)

        return self._meters[meter_name]
