import os
import ssl
from typing import ClassVar

from ..models import BaseModel
from ..typebase import SecretStr
from ..utils.osfiles import FSNode
from . import EnvConfig, env_config


class ServerSSLConfig(BaseModel):
    _env_config: ClassVar[EnvConfig] = env_config()

    is_enabled: bool = _env_config("SERVER_SSL_ENABLED", default=False, cast=bool)
    ca_file: str | None = _env_config("SERVER_SSL_CA_FILE", default=None)
    cert_file: str | None = _env_config("SERVER_SSL_CERT_FILE", default=None)
    key_file: str | None = _env_config("SERVER_SSL_KEY_FILE", default=None)
    password: SecretStr | None = _env_config(
        "SERVER_SSL_PASSWORD", default=None, cast=SecretStr
    )
    is_self_signed: bool = False

    def __post_init__(self):
        if self.is_enabled:
            if not self.cert_file or not FSNode.is_file_readable(self.cert_file):
                raise ValueError(
                    "SERVER_SSL_CERT_FILE environment variable is not set or, the file is not readable"
                )

            if not self.key_file or not FSNode.is_file_readable(self.key_file):
                raise ValueError(
                    "SERVER_SSL_KEY_FILE environment variable is not set or, the file is not readable"
                )

            if not self.ca_file:
                self.is_self_signed = True
            elif not FSNode.is_file_readable(self.ca_file):
                raise ValueError(
                    "SERVER_SSL_CA_FILE environment variable is not set or, the file is not readable"
                )


class ClientSSLConfig(BaseModel):
    _env_config: ClassVar[EnvConfig] = env_config()

    is_enabled: bool = _env_config("CLIENT_SSL_ENABLED", default=False, cast=bool)
    ca_file: str | None = _env_config("CLIENT_SSL_CA_FILE", default=None)
    cert_file: str | None = _env_config("CLIENT_SSL_CERT_FILE", default=None)
    key_file: str | None = _env_config("CLIENT_SSL_KEY_FILE", default=None)
    password: SecretStr | None = _env_config(
        "CLIENT_SSL_PASSWORD", default=None, cast=SecretStr
    )
    is_self_signed: bool = False

    def __post_init__(self):
        if self.is_enabled:
            if not self.cert_file or not FSNode.is_file_readable(self.cert_file):
                raise ValueError(
                    "CLIENT_SSL_CERT_FILE environment variable is not set or, the file is not readable"
                )

            if not self.key_file or not FSNode.is_file_readable(self.key_file):
                raise ValueError(
                    "CLIENT_SSL_KEY_FILE environment variable is not set or, the file is not readable"
                )

            if not self.ca_file:
                self.is_self_signed = True
            elif not FSNode.is_file_readable(self.ca_file):
                raise ValueError(
                    "CLIENT_SSL_CA_FILE environment variable is not set or, the file is not readable"
                )


__def_client_ssl_config = ClientSSLConfig()
__def_server_ssl_config = ServerSSLConfig()


def create_context(
    certfile: str | os.PathLike[str],
    keyfile: str | os.PathLike[str] | None,
    password: str | None,
    ssl_version: int,
    cert_reqs: int,
    ca_certs: str | os.PathLike[str] | None,
    ciphers: str | None,
) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl_version)
    get_password = (lambda: password) if password else None

    ctx.load_cert_chain(certfile, keyfile, get_password)
    ctx.verify_mode = ssl.VerifyMode(cert_reqs)

    if ca_certs:
        ctx.load_verify_locations(ca_certs)

    if ciphers:
        ctx.set_ciphers(ciphers)

    return ctx


def server_context(settings: ServerSSLConfig = __def_server_ssl_config):
    """
    Get the SSL context for the server using environment variables

    Returns
    -------
    ssl.SSLContext
        SSL context
    """

    verify_mode = ssl.CERT_OPTIONAL if settings.is_self_signed else ssl.CERT_REQUIRED
    get_password = (
        (lambda: settings.password.get_secret_value()) if settings.password else None
    )

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)  # NOSONAR
    ctx.load_default_certs(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(settings.cert_file, settings.key_file, get_password)

    ctx.verify_mode = ssl.VerifyMode(verify_mode)

    if not settings.is_self_signed:
        ctx.load_verify_locations(settings.ca_file)

    return ctx


def client_context(settings: ClientSSLConfig = __def_client_ssl_config):
    """
    Get the SSL context for the client using environment variables

    Returns
    -------
    ssl.SSLContext
        SSL context
    """

    verify_mode = ssl.CERT_OPTIONAL if settings.is_self_signed else ssl.CERT_REQUIRED
    get_password = (
        (lambda: settings.password.get_secret_value()) if settings.password else None
    )

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)  # NOSONAR
    ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(settings.cert_file, settings.key_file, get_password)

    ctx.verify_mode = ssl.VerifyMode(verify_mode)

    if not settings.is_self_signed:
        ctx.load_verify_locations(settings.ca_file)

    return ctx
