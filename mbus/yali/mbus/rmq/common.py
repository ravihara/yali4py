import re
from logging import getLogger
from typing import Annotated, Any, Callable, Coroutine, Dict, List, Tuple

from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage

from yali.core.common import sizeof_object
from yali.core.models import BaseModel, field_specs
from yali.core.typebase import (
    AmqpUrl,
    ConstrNode,
    NonEmptyStr,
    PositiveInt,
    SnakeCaseStr,
)

_binding_key_regex = re.compile(r"^(?:\w+|\*)(?:\.(?:\w+|\*))*(?:\.\#)?$")


## Callbacks / handlers for RMQ messages
RMQBatchItem = Tuple[int, Dict]
RMQData = Dict | List[RMQBatchItem]
RMQDataPreprocessor = Callable[[RMQData], Coroutine[Any, Any, RMQData]]
RMQDataProcessor = Callable[[RMQData], Coroutine[Any, Any, None]]


class PublisherConfig(BaseModel):
    service: NonEmptyStr
    amqp_url: AmqpUrl
    exchange_name: SnakeCaseStr
    exchange_type: ExchangeType = ExchangeType.TOPIC
    prefetch_count: PositiveInt = field_specs(default=1)
    max_message_size: Annotated[int, ConstrNode.constr_num(ge=5120)] = field_specs(
        default=5242880
    )


class PubSubConfig(PublisherConfig):
    binding_keys: List[str] = []
    data_processor: RMQDataProcessor
    data_preprocessor: RMQDataPreprocessor | None = None

    ## Only used for batch based pubsub
    batch_interval: Annotated[float, ConstrNode.constr_num(ge=1.0)] = field_specs(
        default=10.0
    )
    max_batch_entries: PositiveInt = field_specs(default=10)
    max_batch_size: Annotated[int, ConstrNode.constr_num(ge=10240)] = field_specs(
        default=52428800
    )

    def __post_init__(self):
        if self.max_batch_size <= self.max_message_size:
            raise ValueError("'max_batch_size' must be greater than 'max_message_size'")

        for key in self.binding_keys:
            if not _binding_key_regex.match(key):
                raise ValueError(f"Invalid binding key: {key}")


class RMQBatchBuffer:
    _logger = getLogger(__name__)

    def __init__(self, *, max_entries: int, max_size: int):
        self._max_entries = max_entries
        self._max_size = max_size

        self._entries: List[RMQBatchItem] = []
        self._last_message: AbstractIncomingMessage | None = None

        self._size = 0

    def reset(self):
        self._entries.clear()
        self._last_message = None
        self._size = 0

    def append(self, *, data: Dict, message: AbstractIncomingMessage):
        data_size = sizeof_object(data)

        if len(self._entries) >= self._max_entries:
            self._logger.error(
                f"Dropping message due to entry limit of {self._max_entries}"
            )
            return -1

        if (self._size + data_size) > self._max_size:
            self._logger.error(
                f"Dropping message due to size limit of {self._max_size}"
            )
            return -1

        self._entries.append((message.delivery_tag, data))
        self._last_message = message
        self._size += data_size

        return data_size

    def count(self):
        return len(self._entries)

    def is_full(self):
        return (len(self._entries) >= self._max_entries) or (
            self._size >= self._max_size
        )

    def is_empty(self):
        return len(self._entries) == 0

    def enqueued(self):
        return self._entries, self._last_message
