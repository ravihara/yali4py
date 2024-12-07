import asyncio
import datetime as dt
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from enum import StrEnum
from io import BytesIO
from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
from typing import Annotated, Any, Coroutine, Dict, List, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

PASS_MESSAGE = "success"
FAIL_MESSAGE = "failure"

JsonType = Dict | List
MultiProcContext = ForkContext | ForkServerContext | SpawnContext
Awaitable = Coroutine | asyncio.Future | asyncio.Task
PoolExecutor = ProcessPoolExecutor | ThreadPoolExecutor
ResultCode = int | str

EmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, max_length=0)]
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


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


class StrictTypesModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=False,
        allow_inf_nan=False,
        populate_by_name=True,
        loc_by_alias=True,
    )


class SkipTypesModel(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        arbitrary_types_allowed=True,
        allow_inf_nan=False,
        populate_by_name=True,
        loc_by_alias=True,
    )


class FlexiTypesModel(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
        allow_inf_nan=False,
        populate_by_name=True,
        loc_by_alias=True,
    )

    @property
    def extra_fields(self) -> set[str]:
        return set(self.model_extra)

    @property
    def extra_properties(self) -> dict[str, Any] | None:
        return self.model_extra


class AwaitCondition(StrEnum):
    ALL_DONE = asyncio.ALL_COMPLETED
    ANY_DONE = asyncio.FIRST_COMPLETED
    ANY_FAIL = asyncio.FIRST_EXCEPTION


class AioExceptValue(FlexiTypesModel):
    name: str
    exc: BaseException


class DateTimeRange(StrictTypesModel):
    start_dt: dt.datetime
    end_dt: dt.datetime

    @model_validator(mode="after")
    def ensure_end_gt_start(self):
        val_start: dt.datetime = self.start_dt
        val_end: dt.datetime = self.end_dt

        if val_end < val_start:
            raise ValueError(
                f"{val_end} < {val_start} - Value of 'end_dt' should be greater than or equal to 'start_dt'"
            )

        return self


class FailBase(FlexiTypesModel):
    error: NonEmptyStr
    extra: Dict = {}


class Failure(FailBase):
    tid: Literal["fl"] = "fl"


class Success(FlexiTypesModel):
    tid: Literal["sc"] = "sc"
    data: Dict


Result = Annotated[Union[Success, Failure], Field(discriminator="tid")]


class MultiResult(FlexiTypesModel):
    passed: List[Dict] = []
    failed: List[FailBase] = []
    summary: str | None = None
