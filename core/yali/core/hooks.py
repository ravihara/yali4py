from typing import Any, Type

import msgspec

from .metatypes import SecretStr


def model_tag_hook(name: str):
    return f"YaliMdl.{name}"


def json_default_enc_hook(obj: Any) -> Any:
    if isinstance(obj, BaseException):
        return str(obj.args[0]) if obj.args else obj.__str__()
    elif isinstance(obj, complex):
        return (obj.real, obj.imag)
    elif isinstance(obj, SecretStr):
        return obj.get_secret_value()

    raise NotImplementedError(f"Objects of type {type(obj)} are not supported")


def json_default_dec_hook(type: Type, obj: Any) -> Any:
    if issubclass(type, BaseException):
        return type(obj)
    elif type is complex:
        return complex(obj[0], obj[1])
    elif type is SecretStr:
        return SecretStr(obj)

    raise NotImplementedError(f"Objects of type {type} are not supported")


def constr_num_hook(
    *,
    title: str | None = None,
    description: str | None = None,
    ge: int | float | None = None,
    le: int | float | None = None,
    gt: int | float | None = None,
    lt: int | float | None = None,
    multiple_of: int | float | None = None,
):
    if ge is None and gt is None and le is None and lt is None and multiple_of is None:
        raise ValueError("At least one of ge, gt, le, lt, or multiple_of must be set")

    return msgspec.Meta(
        title=title,
        description=description,
        ge=ge,
        gt=gt,
        le=le,
        lt=lt,
        multiple_of=multiple_of,
    )


def constr_str_hook(
    *,
    title: str | None = None,
    description: str | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
):
    if min_length is None and max_length is None and pattern is None:
        raise ValueError(
            "At least one of min_length, max_length, or pattern must be set"
        )

    return msgspec.Meta(
        title=title,
        description=description,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
    )


def constr_seq_hook(
    *,
    title: str | None = None,
    description: str | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
):
    if min_length is None and max_length is None:
        raise ValueError("At least one of min_length or max_length must be set")

    return msgspec.Meta(
        title=title,
        description=description,
        min_length=min_length,
        max_length=max_length,
    )
