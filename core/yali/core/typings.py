import asyncio
import datetime as dt
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from enum import StrEnum
from io import BytesIO
from typing import (
    Annotated,
    Any,
    Callable,
    Coroutine,
    Iterable,
    List,
    Tuple,
    Type,
)

import msgspec

PASS_MESSAGE = "success"
FAIL_MESSAGE = "failure"
YALI_TAG_FIELD = "_yali_mdl_tag"

ResultCode = int | str
JSONValue = dict | list
ModelType = Type[msgspec.Struct]
ModelField = str | Tuple[str, type] | Tuple[str, type, Any]
Awaitable = Coroutine | asyncio.Future | asyncio.Task
PoolExecutor = ProcessPoolExecutor | ThreadPoolExecutor
AwaitableDoneHandler = Callable[[asyncio.Future], None]
PoolExecutorInitFunc = Callable[[Any], object]
SequenceType = Type[bytes | bytearray | list | tuple | set | frozenset]

field_specs = msgspec.field


def _model_tag_hook(name: str):
    return f"YaliMdl.{name}"


## Common annotated type definitions
NonNegativeInt = Annotated[
    int, msgspec.Meta(title="NonNegativeInt", description="Non-negative integer", ge=0)
]
NonNegativeFloat = Annotated[
    float,
    msgspec.Meta(title="NonNegativeFloat", description="Non-negative float", ge=0.0),
]
TZDateTime = Annotated[
    dt.datetime,
    msgspec.Meta(
        title="TZDateTime", description="Datetime with timezone info", tz=True
    ),
]
NonTZDateTime = Annotated[
    dt.datetime,
    msgspec.Meta(
        title="NoTZDateTime", description="Datetime without timezone info", tz=False
    ),
]
TZTime = Annotated[
    dt.time,
    msgspec.Meta(title="TZTime", description="Time with timezone info", tz=True),
]
NonTZTime = Annotated[
    dt.time,
    msgspec.Meta(title="NoTZTime", description="Time without timezone info", tz=False),
]
EmptyStr = Annotated[
    str,
    msgspec.Meta(
        title="EmptyStr", description="Empty string", min_length=0, max_length=0
    ),
]
NonEmptyStr = Annotated[
    str, msgspec.Meta(title="NonEmptyStr", description="Non-empty string", min_length=1)
]
SnakeCaseStr = Annotated[
    str,
    msgspec.Meta(
        title="SnakeCaseStr", description="Snake-case string", pattern=r"^[a-z0-9_]+$"
    ),
]


class DataModelOptions(msgspec.Struct):
    is_frozen: bool = field_specs(default=False)
    is_tagged: bool = field_specs(default=True)
    is_ordered: bool = field_specs(default=False)
    forbid_unknown: bool = field_specs(default=False)
    omit_defaults: bool = field_specs(default=False)
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
    tag=_model_tag_hook,
    tag_field="_yali_mdl_tag",
    repr_omit_defaults=False,
    array_like=False,
):
    pass


class YaliError(Exception):
    def __init__(self, error: NonEmptyStr, exc_cause: BaseException | None = None):
        super().__init__(error)
        self.exc_cause = exc_cause

    def __str__(self):
        if self.exc_cause:
            exc_str = super().__str__()
            return f"{exc_str} (caused by {repr(self.exc_cause)})"

        return super().__str__()


ErrorOrBytesIO = YaliError | BytesIO
ErrorOrStr = YaliError | str


class AwaitCondition(StrEnum):
    ALL_DONE = asyncio.ALL_COMPLETED
    ANY_DONE = asyncio.FIRST_COMPLETED
    ANY_FAIL = asyncio.FIRST_EXCEPTION


class AioExceptValue(BaseModel):
    name: str
    exc_info: BaseException


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
    extra: dict = field_specs(default={})


class Success(BaseModel):
    data: dict


Result = Success | Failure


class MultiResult(BaseModel):
    passed: List[dict] = field_specs(default=[])
    failed: List[Failure] = field_specs(default=[])
    summary: str | None = field_specs(default=None)


def constrain_number(
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


def constrain_string(
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


def constrain_sequence(
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


def data_model(
    name: str,
    *,
    fields: Iterable[ModelField],
    options: DataModelOptions = DataModelOptions(),
):
    model_ns = None
    tag_field = YALI_TAG_FIELD if options.is_tagged else None
    tag_value = _model_tag_hook if options.is_tagged else None

    if options.post_init_hook:
        model_ns = {"__post_init__": options.post_init_hook}

    model_type = msgspec.defstruct(
        name=name,
        fields=fields,
        namespace=model_ns,
        bases=options.parents,
        kw_only=True,
        array_like=False,
        eq=True,
        forbid_unknown_fields=options.forbid_unknown,
        omit_defaults=options.omit_defaults,
        repr_omit_defaults=options.omit_defaults,
        frozen=options.is_frozen,
        order=options.is_ordered,
        tag_field=tag_field,
        tag=tag_value,
    )

    return model_type
