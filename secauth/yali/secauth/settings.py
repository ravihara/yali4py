from typing import ClassVar

from yali.core.metatypes import SecretStr
from yali.core.models import BaseModel
from yali.core.settings import EnvConfig, env_config
from yali.core.utils.osfiles import FilesConv


class ServerSSLSettings(BaseModel):
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
            if not self.cert_file or not FilesConv.is_file_readable(self.cert_file):
                raise ValueError(
                    "SERVER_SSL_CERT_FILE environment variable is not set or, the file is not readable"
                )

            if not self.key_file or not FilesConv.is_file_readable(self.key_file):
                raise ValueError(
                    "SERVER_SSL_KEY_FILE environment variable is not set or, the file is not readable"
                )

            if not self.ca_file:
                self.is_self_signed = True
            elif not FilesConv.is_file_readable(self.ca_file):
                raise ValueError(
                    "SERVER_SSL_CA_FILE environment variable is not set or, the file is not readable"
                )


class ClientSSLSettings(BaseModel):
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
            if not self.cert_file or not FilesConv.is_file_readable(self.cert_file):
                raise ValueError(
                    "CLIENT_SSL_CERT_FILE environment variable is not set or, the file is not readable"
                )

            if not self.key_file or not FilesConv.is_file_readable(self.key_file):
                raise ValueError(
                    "CLIENT_SSL_KEY_FILE environment variable is not set or, the file is not readable"
                )

            if not self.ca_file:
                self.is_self_signed = True
            elif not FilesConv.is_file_readable(self.ca_file):
                raise ValueError(
                    "CLIENT_SSL_CA_FILE environment variable is not set or, the file is not readable"
                )
