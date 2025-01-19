from typing import ClassVar

from yali.core.models import BaseModel
from yali.core.osfiles import FSNode
from yali.core.settings import EnvConfig, env_config
from yali.core.typebase import NonEmptyStr, SecretStr

_env_config = env_config()


class DbSSLSettings(BaseModel):
    _env_config: ClassVar[EnvConfig] = env_config()

    ssl_enabled: bool = _env_config("DB_SSL_ENABLED", default=False, cast=bool)
    ssl_ca_file: str | None = _env_config("DB_SSL_CA_FILE", default=None)
    ssl_cert_file: str | None = _env_config("DB_SSL_CERT_FILE", default=None)
    ssl_key_file: str | None = _env_config("DB_SSL_KEY_FILE", default=None)
    ssl_password: SecretStr | None = _env_config(
        "DB_SSL_PASSWORD", default=None, cast=SecretStr
    )
    ssl_self_signed: bool = False

    def __post_init__(self):
        if self.ssl_enabled:
            if not self.ssl_cert_file or not FSNode.is_file_readable(
                self.ssl_cert_file
            ):
                raise ValueError(
                    "DB_SSL_CERT_FILE environment variable is not set or, the file is not readable"
                )

            if not self.ssl_key_file or not FSNode.is_file_readable(self.ssl_key_file):
                raise ValueError(
                    "DB_SSL_KEY_FILE environment variable is not set or, the file is not readable"
                )

            if not self.ssl_ca_file:
                self.ssl_self_signed = True
            elif not FSNode.is_file_readable(self.ssl_ca_file):
                raise ValueError(
                    "DB_SSL_CA_FILE environment variable is not set or, the file is not readable"
                )


class PgSQLSettings(DbSSLSettings):
    log_name: NonEmptyStr = "yali.pgsql.client"
    username: str = _env_config("PGSQL_USERNAME")
    password: str = _env_config("PGSQL_PASSWORD")
    host: str = _env_config("PGSQL_HOST", default="localhost")
    port: int = _env_config("PGSQL_PORT", default=5432, cast=int)
    database: str = _env_config("PGSQL_DATABASE")
    geospatial: bool = _env_config("PGSQL_GEOSPATIAL", default=False, cast=bool)


class MySQLSettings(DbSSLSettings):
    log_name: NonEmptyStr = "yali.mysql.client"
    username: str = _env_config("MYSQL_USERNAME")
    password: str = _env_config("MYSQL_PASSWORD")
    host: str = _env_config("MYSQL_HOST", default="localhost")
    port: int = _env_config("MYSQL_PORT", default=3306, cast=int)
    database: str = _env_config("MYSQL_DATABASE")


class SqliteSettings(BaseModel):
    log_name: NonEmptyStr = "yali.sqlite.client"
    db_file: str = _env_config("SQLITE_DB_FILE", default="sqlite.db")


SqlDbSettings = PgSQLSettings | MySQLSettings | SqliteSettings
