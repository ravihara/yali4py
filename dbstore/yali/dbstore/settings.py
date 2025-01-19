from typing import ClassVar, Dict, Literal

from yali.core.models import BaseModel
from yali.core.settings import EnvConfig, env_config

_env_config = env_config()

DbClientName = Literal[
    "yali.pgsql.client",
    "yali.mysql.client",
    "yali.oracle.client",
    "yali.mssql.client",
    "yali.sqlite.client",
]


class DbBaseModel(BaseModel):
    _env_config: ClassVar[EnvConfig] = env_config()
    query: Dict[str, any] = {}


class PgSQLSettings(DbBaseModel):
    client_name: DbClientName = "yali.pgsql.client"
    username: str = _env_config("PGSQL_USERNAME")
    password: str = _env_config("PGSQL_PASSWORD")
    host: str = _env_config("PGSQL_HOST", default="localhost")
    port: int = _env_config("PGSQL_PORT", default=5432, cast=int)
    database: str = _env_config("PGSQL_DATABASE")
    geospatial: bool = _env_config("PGSQL_GEOSPATIAL", default=False, cast=bool)


class MySQLSettings(DbBaseModel):
    client_name: DbClientName = "yali.mysql.client"
    username: str = _env_config("MYSQL_USERNAME")
    password: str = _env_config("MYSQL_PASSWORD")
    host: str = _env_config("MYSQL_HOST", default="localhost")
    port: int = _env_config("MYSQL_PORT", default=3306, cast=int)
    database: str = _env_config("MYSQL_DATABASE")


class OracleSettings(DbBaseModel):
    client_name: DbClientName = "yali.oracle.client"
    username: str = _env_config("ORACLE_USERNAME")
    password: str = _env_config("ORACLE_PASSWORD")
    host: str = _env_config("ORACLE_HOST", default="localhost")
    port: int = _env_config("ORACLE_PORT", default=1521, cast=int)
    database: str = _env_config("ORACLE_DATABASE")
    query: Dict[str, any] = {}


class MSSQLSettings(DbBaseModel):
    client_name: DbClientName = "yali.mssql.client"
    username: str = _env_config("MSSQL_USERNAME")
    password: str = _env_config("MSSQL_PASSWORD")
    host: str = _env_config("MSSQL_HOST", default="localhost")
    port: int = _env_config("MSSQL_PORT", default=1433, cast=int)
    database: str = _env_config("MSSQL_DATABASE")
    query: Dict[str, any] = {
        "driver": "ODBC Driver 17 for SQL Server",
    }


class SqliteSettings(DbBaseModel):
    client_name: DbClientName = "yali.sqlite.client"
    db_file: str = _env_config("SQLITE_DB_FILE", default="sqlite.db")


SqlDbSettings = (
    PgSQLSettings | MySQLSettings | OracleSettings | MSSQLSettings | SqliteSettings
)
