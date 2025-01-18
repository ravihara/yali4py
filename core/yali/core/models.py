import datetime as dt
from typing import Any, Callable, Dict, Iterable, List, Tuple, Type

import msgspec

from .consts import YALI_TAG_FIELD
from .typebase import NonEmptyStr, UnsignedInt

ModelType = Type[msgspec.Struct]
ModelField = str | Tuple[str, type] | Tuple[str, type, Any]

field_specs = msgspec.field


def model_tag_hook(name: str):
    return f"YaliMdl.{name}"


class DataModelOptions(msgspec.Struct):
    is_frozen: bool = field_specs(default=False)
    is_tagged: bool = field_specs(default=True)
    is_ordered: bool = field_specs(default=False)
    parents: Tuple[ModelType, ...] | None = field_specs(default=None)
    post_init_hook: Callable[[], None] | None = field_specs(default=None)


class BaseModel(
    msgspec.Struct,
    frozen=False,
    order=False,
    eq=True,
    kw_only=True,
    omit_defaults=False,
    forbid_unknown_fields=False,
    tag=model_tag_hook,
    tag_field=YALI_TAG_FIELD,
    array_like=False,
):
    pass


class AioExceptValue(BaseModel):
    name: str
    exc_info: BaseException
    extra: str | None = "Ting"


class DateTimeRange(BaseModel):
    start_dt: dt.datetime
    end_dt: dt.datetime

    def __post_init__(self):
        if self.start_dt > self.end_dt:
            raise ValueError(
                f"Value of end_dt '{self.end_dt}' should be greater than or equal to start_dt '{self.start_dt}'"
            )


class Failure(BaseModel):
    error: NonEmptyStr
    cause: BaseException | None = None
    extrac: Dict[str, Any] | None = None


class Success(BaseModel):
    data: dict
    code: UnsignedInt | None = field_specs(default=None)


Result = Success | Failure


class MultiResult(BaseModel):
    passed: List[dict] = field_specs(default=[])
    failed: List[Failure] = field_specs(default=[])
    summary: str | None = field_specs(default=None)


def data_model(
    name: str,
    *,
    fields: Iterable[ModelField],
    options: DataModelOptions = DataModelOptions(),
):
    model_ns = None
    tag_field = YALI_TAG_FIELD if options.is_tagged else None
    tag_value = model_tag_hook if options.is_tagged else None

    if options.post_init_hook:
        model_ns = {"__post_init__": options.post_init_hook}

    model_type = msgspec.defstruct(
        name=name,
        fields=fields,
        namespace=model_ns,
        bases=options.parents,
        frozen=options.is_frozen,
        order=options.is_ordered,
        eq=True,
        kw_only=True,
        omit_defaults=False,
        forbid_unknown_fields=False,
        tag=tag_value,
        tag_field=tag_field,
        array_like=False,
    )

    return model_type
