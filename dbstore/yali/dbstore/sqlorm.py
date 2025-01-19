import datetime as dt
import logging
from typing import Annotated

from sqlalchemy import URL as DbURL
from sqlalchemy import create_engine, func, types
from sqlalchemy.orm import DeclarativeBase, Session, mapped_column
from sqlalchemy.util import EMPTY_DICT as DB_EMPTY_DICT

from yali.core.codecs import JSONNode
from yali.core.settings import log_settings

from .settings import MySQLSettings, PgSQLSettings, SqlDbSettings, SqliteSettings


def mapped_str_type(*, length: int | None = None, is_utf8: bool = True):
    if is_utf8:
        if length and length > 0:
            str_type = types.String(length=length, collation="utf8")
        else:
            str_type = types.String(collation="utf8")
    elif length and length > 0:
        str_type = types.String(length=length)
    else:
        str_type = types.String()

    str_type = str_type.with_variant(types.NVARCHAR, "mssql")

    return str_type


DbPrimaryKey = Annotated[int, mapped_column(primary_key=True)]

DbBool = Annotated[bool, mapped_column(types.Boolean, nullable=False)]
DbOptBool = Annotated[bool, mapped_column(types.Boolean, nullable=True)]

DbInt = Annotated[int, mapped_column(types.Integer, nullable=False)]
DbOptInt = Annotated[int, mapped_column(types.Integer, nullable=True)]

DbSmallInt = Annotated[int, mapped_column(types.SmallInteger, nullable=False)]
DbOptSmallInt = Annotated[int, mapped_column(types.SmallInteger, nullable=True)]

DbBigInt = Annotated[int, mapped_column(types.BigInteger, nullable=False)]
DbOptBigInt = Annotated[int, mapped_column(types.BigInteger, nullable=True)]

DbFloat = Annotated[float, mapped_column(types.Float, nullable=False)]
DbOptFloat = Annotated[float, mapped_column(types.Float, nullable=True)]

DbDouble = Annotated[float, mapped_column(types.Double, nullable=False)]
DbOptDouble = Annotated[float, mapped_column(types.Double, nullable=True)]

DbDateTime = Annotated[
    dt.datetime,
    mapped_column(
        types.TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    ),
]
DbOptDateTime = Annotated[
    dt.datetime,
    mapped_column(
        types.TIMESTAMP(timezone=True), server_default=func.now(), nullable=True
    ),
]

DbDate = Annotated[
    dt.date, mapped_column(types.DATE, server_default=func.now(), nullable=False)
]
DbOptDate = Annotated[
    dt.date, mapped_column(types.DATE, server_default=func.now(), nullable=True)
]

DbTime = Annotated[
    dt.time, mapped_column(types.TIME, server_default=func.now(), nullable=False)
]
DbOptTime = Annotated[
    dt.time, mapped_column(types.TIME, server_default=func.now(), nullable=True)
]

DbRawStr = Annotated[str, mapped_column(mapped_str_type(is_utf8=False), nullable=False)]
DbOptRawStr = Annotated[
    str, mapped_column(mapped_str_type(is_utf8=False), nullable=True)
]

DbStr = Annotated[str, mapped_column(mapped_str_type(is_utf8=True), nullable=False)]
DbOptStr = Annotated[str, mapped_column(mapped_str_type(is_utf8=True), nullable=True)]

DbStr32 = Annotated[
    str, mapped_column(mapped_str_type(length=32, is_utf8=True), nullable=False)
]
DbOptStr32 = Annotated[
    str, mapped_column(mapped_str_type(length=32, is_utf8=True), nullable=True)
]

DbStr64 = Annotated[
    str, mapped_column(mapped_str_type(length=64, is_utf8=True), nullable=False)
]
DbOptStr64 = Annotated[
    str, mapped_column(mapped_str_type(length=64, is_utf8=True), nullable=True)
]

DbStr128 = Annotated[
    str, mapped_column(mapped_str_type(length=128, is_utf8=True), nullable=False)
]
DbOptStr128 = Annotated[
    str, mapped_column(mapped_str_type(length=128, is_utf8=True), nullable=True)
]

DbStr256 = Annotated[
    str, mapped_column(mapped_str_type(length=256, is_utf8=True), nullable=False)
]
DbOptStr256 = Annotated[
    str, mapped_column(mapped_str_type(length=256, is_utf8=True), nullable=True)
]


class DbModel(DeclarativeBase):
    pass


class DbClient:
    __log_settings = log_settings()

    def __make_db_engine(self):
        plugins = []

        if isinstance(self.__settings, SqliteSettings):
            db_url = DbURL.create(
                drivername="sqlite+pysqlite",
                database=self.__settings.db_file,
            )
        elif isinstance(self.__settings, PgSQLSettings):
            db_url = DbURL.create(
                drivername="postgresql+psycopg",
                username=self.__settings.username,
                password=self.__settings.password,
                host=self.__settings.host,
                port=self.__settings.port,
                database=self.__settings.database,
            )

            if self.__settings.geospatial:
                plugins.append("geoalchemy2")
        else:
            if self.__settings.ssl_enabled:
                query_dict = {
                    "ssl_cert": self.__settings.ssl_cert_file,
                    "ssl_key": self.__settings.ssl_key_file,
                    "ssl_check_hostname": "true",
                }

                if self.__settings.ssl_self_signed:
                    query_dict["ssl_ca"] = self.__settings.ssl_ca_file
                    query_dict["ssl_check_hostname"] = "false"
            else:
                query_dict = DB_EMPTY_DICT

            db_url = DbURL.create(
                drivername="mysql+mysqldb",
                username=self.__settings.username,
                password=self.__settings.password,
                host=self.__settings.host,
                port=self.__settings.port,
                database=self.__settings.database,
                query=query_dict,
            )

        if not plugins:
            return create_engine(
                url=db_url,
                echo=self.__log_settings.debug_enabled,
                json_serializer=JSONNode.dump_str,
                json_deserializer=JSONNode.load_data,
            )

        return create_engine(
            url=db_url,
            echo=self.__log_settings.debug_enabled,
            json_serializer=JSONNode.dump_str,
            json_deserializer=JSONNode.load_data,
            plugins=plugins,
        )

    def __set_db_logging(self):
        if isinstance(self.__settings, PgSQLSettings):
            logging.getLogger("sqlalchemy.dialects.postgresql").setLevel(
                self.__log_settings.log_level
            )

    def __init__(self, settings: SqlDbSettings):
        self.__settings = settings
        self.__set_db_logging()
        self.__engine = self.__make_db_engine()

    @property
    def dialect(self):
        if isinstance(self.__settings, PgSQLSettings):
            return "postgresql"
        elif isinstance(self.__settings, MySQLSettings):
            return "mysql"
        else:
            return "sqlite"

    @property
    def engine(self):
        return self.__engine

    @property
    def connection_ctx(self):
        """Default connection context"""
        return self.__engine.connect()

    @property
    def txn_connection_ctx(self):
        """Transactional connection context"""
        return self.__engine.begin()

    def session_ctx(self) -> Session:
        return Session(bind=self.__engine)
