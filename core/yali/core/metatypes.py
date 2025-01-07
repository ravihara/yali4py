import asyncio
import datetime as dt
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from enum import StrEnum
from threading import Lock
from typing import Annotated, Any, Callable, Coroutine, Type, TypeVar

import msgspec

YaliT = TypeVar("YaliT")
DataType = Type[YaliT]
ResultCode = int | str
JSONValue = dict | list
Awaitable = Coroutine | asyncio.Future | asyncio.Task
PoolExecutor = ProcessPoolExecutor | ThreadPoolExecutor
AwaitableDoneHandler = Callable[[asyncio.Future], None]
PoolExecutorInitFunc = Callable[[Any], object]

## Common annotated type definitions
PositiveInt = Annotated[
    int, msgspec.Meta(title="PositiveInt", description="Non-negative integer", gt=0)
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
AmqpUrl = Annotated[
    str,
    msgspec.Meta(
        title="AmqpUrl",
        description="Rabbitmq URL string",
        pattern=r"^(?:amqp|amqps)://(\w+):(\w+)@(\w+):(\d+)/(\w+)$",
    ),
]
WebsocketUrl = Annotated[
    str,
    msgspec.Meta(
        title="WebsocketUrl",
        description="Websocket URL string",
        pattern=r"^(?:ws|wss)://(\w+):(\d+)/(\w+)$",
    ),
]


class AwaitCondition(StrEnum):
    ALL_DONE = asyncio.ALL_COMPLETED
    ANY_DONE = asyncio.FIRST_COMPLETED
    ANY_FAIL = asyncio.FIRST_EXCEPTION


## Secret string type
class SecretStr(str):
    def __init__(self, value: str):
        self._secret_str = value

    def get_secret_value(self):
        return self._secret_str

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.get_secret_value() == other.get_secret_value()
        )

    def __hash__(self) -> int:
        return hash(self.get_secret_value())

    def __str__(self) -> str:
        return str(self._display())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._display()!r})"

    def _display(self):
        return "********" if self._secret_str else ""


## Singleton meta-types
class SingletonMeta(type):
    """
    SingletonMeta meta-type used to create singleton classes
    """

    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """
        Method use to create callable class-objects, by the enclosing meta-class (type)
        """
        if cls not in cls.__instances:
            with cls.__lock:
                if cls not in cls.__instances:
                    instance = super().__call__(*args, **kwargs)

                    @classmethod
                    def get_instance(cls):
                        return instance

                    setattr(cls, "get_instance", get_instance)
                    cls.__instances[cls] = instance

        return cls.__instances[cls]


class RiSingletonMeta(type):
    """
    RiSingletonMeta meta-type used to create singleton classes with
    the ability to reinitialize the instance
    """

    __instances = {}
    __inst_lock: Lock = Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """
        Method use to create callable class-objects, by the enclosing meta-class (type)
        """
        is_fresh = False

        if cls not in cls.__instances:
            with cls.__inst_lock:
                if cls not in cls.__instances:
                    is_fresh = True
                    instance = super().__call__(*args, **kwargs)

                    @classmethod
                    def get_instance(cls):
                        return instance

                    setattr(cls, "get_instance", get_instance)
                    cls.__instances[cls] = instance

        instance = cls.__instances[cls]

        if not is_fresh:
            instance.__init__(*args, **kwargs)

        return instance
