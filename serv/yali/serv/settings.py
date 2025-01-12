from typing import Dict, List

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
    port: PositiveInt = field_specs(default=8080)
    base_path: NonEmptyStr
    end_paths: List[PathConfig]


class PortalConfig(BaseModel):
    api: EndpointConfig
    web: EndpointConfig | None = field_specs(default=None)


class PortalEndpoint(EndpointConfig):
    int_base: str
    ext_base: str
    int_origin: str
    ext_origin: str


class AppPortal(BaseModel):
    api: PortalEndpoint
    web: PortalEndpoint | None = field_specs(default=None)


class AppEnv(BaseModel):
    app_version: str = _env_config("APP_VERSION", default="v1")
    portals: Dict[str, AppPortal]
    cors_origins: List[str] = []
    portal_for_web: Dict[str, str] = {}


class MicroServiceSettings(BaseModel):
    web_proxy_base: str | None = _env_config("WEB_PROXY_BASE", default=None)
    api_proxy_base: str | None = _env_config("API_PROXY_BASE", default=None)
    max_thread_workers: PositiveInt = _env_config(
        "MAX_THREAD_WORKERS", default=YALI_NUM_THREAD_WORKERS, cast=PositiveInt
    )
    max_process_workers: PositiveInt = _env_config(
        "MAX_PROCESS_WORKERS", default=YALI_NUM_PROCESS_WORKERS, cast=PositiveInt
    )


__micro_service_settings: MicroServiceSettings | None = None
__app_env: AppEnv | None = None


def micro_service_settings():
    global __micro_service_settings

    if not __micro_service_settings:
        __micro_service_settings = MicroServiceSettings()

    return __micro_service_settings


def app_env(
    portal_configs: Dict[str, PortalConfig],
    force: bool = False,
):
    global __app_env
    srv_settings = micro_service_settings()

    if __app_env:
        if not force:
            return __app_env

        del __app_env
        __app_env = None

    portals: Dict[str, AppPortal] = {}
    cors_origins: List[str] = []
    portal_for_web: Dict[str, str] = {}

    for portal, config in portal_configs.items():
        api_host = config.api.host.replace("0.0.0.0", "localhost")
        int_origin = f"http://{api_host}:{config.api.port}"
        ext_origin = srv_settings.api_proxy_base or int_origin

        cors_origins.append(int_origin)

        if int_origin != ext_origin:
            cors_origins.append(ext_origin)

        api_endpoint = PortalEndpoint(
            host=config.api.host,
            port=config.api.port,
            base_path=config.api.base_path,
            end_paths=config.api.end_paths,
            int_origin=int_origin,
            ext_origin=ext_origin,
            int_base=f"{int_origin}{config.api.base_path}",
            ext_base=f"{ext_origin}{config.api.base_path}",
        )

        app_portal = AppPortal(api=api_endpoint)

        if config.web:
            web_host = config.web.host.replace("0.0.0.0", "localhost")
            int_origin = f"http://{web_host}:{config.web.port}"
            ext_origin = srv_settings.web_proxy_base or int_origin

            cors_origins.append(int_origin)

            if int_origin != ext_origin:
                cors_origins.append(ext_origin)

            web_endpoint = PortalEndpoint(
                host=config.web.host,
                port=config.web.port,
                base_path=config.web.base_path,
                end_paths=config.web.end_paths,
                int_origin=int_origin,
                ext_origin=ext_origin,
                int_base=f"{int_origin}{config.web.base_path}",
                ext_base=f"{ext_origin}{config.web.base_path}",
            )

            portal_for_web[web_endpoint.ext_base] = portal
            app_portal.web = web_endpoint

        portals[portal] = app_portal

    __app_env = AppEnv(
        portals=portals, cors_origins=cors_origins, portal_for_web=portal_for_web
    )

    return __app_env
