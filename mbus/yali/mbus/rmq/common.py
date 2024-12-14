import re
from logging import getLogger
from typing import Any, Callable, Coroutine, Dict, List, Tuple

from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage
from pydantic import AmqpDsn, Field, field_validator, model_validator
from yali.core.typings import FlexiTypesModel, SnakeCaseStr
from yali.core.utils.common import sizeof_object

_binding_key_regex = re.compile(r"^(?:\w+|\*)(?:\.(?:\w+|\*))*(?:\.\#)?$")

## Callbacks / handlers for RMQ messages
RMQBatchItem = Tuple[int, Dict]
RMQData = Dict | List[RMQBatchItem]
RMQDataPreprocessor = Callable[[RMQData], Coroutine[Any, Any, RMQData]]
RMQDataProcessor = Callable[[RMQData], Coroutine[Any, Any, None]]


class PublisherConfig(FlexiTypesModel):
    service: str = Field(min_length=3)
    amqp_url: AmqpDsn = Field(min_length=5)
    exchange_name: SnakeCaseStr
    exchange_type: ExchangeType = ExchangeType.TOPIC
    max_message_size: int = Field(5242880, ge=5120)


class PubSubConfig(PublisherConfig):
    prefetch_count: int = Field(default=1, ge=1)
    binding_keys: List[str] = []
    data_processor: RMQDataProcessor
    data_preprocessor: RMQDataPreprocessor | None = None

    ## Only used for batch based pubsub
    batch_interval: float = Field(10.0, ge=1.0)
    max_batch_entries: int = Field(10, gt=1)
    max_batch_size: int = Field(52428800, ge=10240)

    @model_validator(mode="after")
    def ensure_max_batch_size(self):
        if self.max_batch_size <= self.max_message_size:
            raise ValueError("'max_batch_size' must be greater than 'max_message_size'")

        return self

    @field_validator("binding_keys", mode="before")
    @classmethod
    def validate_binding_keys(cls, v: List[str]):
        if not isinstance(v, list):
            raise ValueError("Binding keys must be a list of string")

        for key in v:
            if not _binding_key_regex.match(key):
                raise ValueError(f"Invalid binding key: {key}")

        return v


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
            self._logger.error(f"Dropping message due to entry limit of {self._max_entries}")
            return -1

        if (self._size + data_size) > self._max_size:
            self._logger.error(f"Dropping message due to size limit of {self._max_size}")
            return -1

        self._entries.append((message.delivery_tag, data))
        self._last_message = message
        self._size += data_size

        return data_size

    def count(self):
        return len(self._entries)

    def is_full(self):
        return (len(self._entries) >= self._max_entries) or (self._size >= self._max_size)

    def is_empty(self):
        return len(self._entries) == 0

    def enqueued(self):
        return self._entries, self._last_message
