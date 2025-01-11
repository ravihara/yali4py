import asyncio
from logging import getLogger
from typing import Any, Dict

from aio_pika import Message, connect_robust
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustExchange,
    HeadersType,
)

from yali.core.codecs import data_to_json
from yali.core.errors import YaliError
from yali.core.utils.datetimes import DateTimeConv

from .common import PublisherConfig, _binding_key_regex


class RMQPublisher:
    def __init__(self, config: PublisherConfig):
        self._config = config
        self._logger = getLogger(config.service)

        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._exchange: AbstractRobustExchange | None = None

    async def _refresh_connection(self):
        is_fresh = False

        if self._connection:
            if self._connection.reconnecting:
                backoff = DateTimeConv.exponential_backoff(2, 5, 30)

                while True:
                    try:
                        delay = next(backoff)
                        self._logger.info(
                            f"Waiting for {delay} seconds before reconnecting"
                        )
                        await asyncio.sleep(delay)

                        if not self._connection.reconnecting:
                            backoff.close()
                            break
                    except StopIteration:
                        self._logger.warning("Maximum reconnect attempts reached")
                        break

                if not self._connection.is_closed:
                    self._logger.info(
                        f"RMQ pubsub service: {self._config.service} reconnected"
                    )
                    return is_fresh
            elif not self._connection.is_closed:
                self._logger.debug(
                    f"RMQ pubsub service: {self._config.service} is already connected"
                )
                return is_fresh

        is_fresh = True
        self._connection = await connect_robust(url=str(self._config.amqp_url))
        self._channel = self._connection.channel(publisher_confirms=True)

        await self._channel.initialize()

        qos = await self._channel.set_qos(prefetch_count=self._config.prefetch_count)
        self._exchange = await self._channel.declare_exchange(
            name=self._config.exchange_name,
            type=self._config.exchange_type,
            durable=True,
            auto_delete=False,
            internal=False,
            passive=False,
            robust=True,
        )

        self._logger.info(
            f"QoS name is {qos.name}, it's sync status is {qos.synchronous}"
        )

        return is_fresh

    async def close(self, exc_info: BaseException | None = None):
        if self._channel and (not self._channel.is_closed):
            await self._channel.close(exc=exc_info)

        if self._connection and (not self._connection.is_closed):
            (
                await self._connection.close(exc=exc_info)
                if exc_info
                else await self._connection.close()
            )

        self._exchange = None
        self._channel = None
        self._connection = None

    async def publish(
        self, *, routing_key: str, headers: HeadersType, json_data: Dict[str, Any]
    ):
        if not _binding_key_regex.match(routing_key):
            return YaliError(f"Invalid binding key: {routing_key}")

        await self._refresh_connection()

        try:
            if not self._exchange:
                return YaliError(
                    f"Exchange {self._config.exchange_name} not initialized yet"
                )

            mesg_body = data_to_json(data=json_data)
            message = Message(
                body=mesg_body, content_type="application/json", headers=headers
            )
            result = await self._exchange.publish(message, routing_key=routing_key)

            if result:
                self._logger.info(
                    f"Message published with delivery-tag: {result.delivery_tag}"
                )
        except Exception as ex:
            return YaliError("Failed to publish message", exc_cause=ex)

        return None
