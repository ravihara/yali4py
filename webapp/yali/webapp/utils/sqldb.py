import datetime as dt
from typing import Annotated

from sqlalchemy import func, types
from sqlalchemy.orm import DeclarativeBase, mapped_column


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
