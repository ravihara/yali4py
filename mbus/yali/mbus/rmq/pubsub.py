import asyncio
import json
from typing import Dict

from aio_pika import ExchangeType
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueueIterator,
    AbstractRobustQueue,
)
from aio_pika.exceptions import ChannelClosed
from yali.core.threadasync import ThreadPoolAsyncExecutor
from yali.core.utils.strings import StringConv

from .common import PubSubConfig
from .publisher import YaliPublisher


class YaliPubSub(YaliPublisher):
    def __init__(self, config: PubSubConfig):
        super().__init__(config)

        self._config = config
        self._is_running = False
        self._qbase = StringConv.to_snakecase(config.service)
        self._queue: AbstractRobustQueue | None = None
        self._q_iter: AbstractQueueIterator | None = None
        self._q_executor = ThreadPoolAsyncExecutor(thread_name_prefix=f"yalirmq_{self._qbase}")

        if self._config.exchange_type == ExchangeType.FANOUT:
            self.__q_name = f"{self._qbase}_bcast_q"
        else:
            self.__q_name = f"{self._qbase}_q"

    async def __compute_q_arguments(self):
        if self._config.exchange_type != ExchangeType.TOPIC:
            return None

        self.__dlx_name = f"{self._config.exchange_name}_dlx"
        self.__dlq_name = f"{self._qbase}_dlq"

        self.__dl_exchange = await self._channel.declare_exchange(
            name=self.__dlx_name,
            type=ExchangeType.DIRECT,
            durable=False,
            auto_delete=False,
            internal=False,
            passive=False,
            robust=True,
        )

        self.__dl_queue = await self._channel.declare_queue(
            name=self.__dlq_name,
            durable=True,
            auto_delete=False,
            passive=False,
            robust=True,
            exclusive=False,
            arguments={
                "x-dead-letter-exchange": "amq.direct",
                "x-dead-letter-routing-key": self.__q_name,
            },
        )

        await self.__dl_queue.bind(exchange=self.__dl_exchange)

        return {
            "x-dead-letter-exchange": self.__dlx_name,
            "x-dead-letter-routing-key": self.__dlq_name,
        }

    async def _refresh_connection(self):
        is_fresh = await super()._refresh_connection()
        assert self._exchange

        if not is_fresh and (self._queue and not self._queue.channel.is_closed):
            return

        queue_args = await self.__compute_q_arguments()

        self._queue = await self._channel.declare_queue(
            name=self.__q_name,
            durable=True,
            auto_delete=False,
            passive=False,
            robust=True,
            exclusive=False,
            arguments=queue_args,
        )

        for bind_key in self._config.binding_keys:
            await self._queue.bind(exchange=self._exchange, routing_key=bind_key, robust=True)

        return is_fresh

    async def close(self, exc_info: BaseException | None = None):
        self._is_running = False

        if self._q_iter:
            await self._q_iter.close()
            self._q_iter = None

        self._queue = None

        await super().close(exc_info=exc_info)

    async def process_message(self, message: AbstractIncomingMessage):
        try:
            mesg_text = message.body.decode("utf-8")
            mesg_json: Dict = json.loads(mesg_text)

            if self._config.data_preprocessor:
                mesg_json = await self._q_executor.submit(self._config.data_preprocessor(mesg_json))

            await self._q_executor.submit(self._config.data_processor(mesg_json))
            self._logger.info(f"Processed message with delivery-tag: {message.delivery_tag}")
            await message.ack(multiple=False)
        except Exception as ex:
            self._logger.error("Unhandled error while processing message", exc_info=ex)
            await message.nack(multiple=False, requeue=False)

    async def consume(self):
        if self._is_running:
            self._logger.warning(f"Pubsub {self._config.service} is already running")
            return

        await self._refresh_connection()
        assert self._queue

        self._is_running = True

        while self._is_running:
            try:
                self._q_iter = self._queue.iterator()

                async with self._q_iter as iter:
                    async for message in iter:
                        await self.process_message(message)
            except ChannelClosed as ex:
                self._q_iter = None
                self._logger.error(f"Channel closed: {ex.reason}")
                await asyncio.sleep(3)
                await self._refresh_connection()
            except asyncio.TimeoutError:
                self._q_iter = None
                self._logger.error("Timeout while waiting for message")
                await asyncio.sleep(3)
                await self._refresh_connection()
            except KeyboardInterrupt:
                self._logger.warning("KeyboardInterrupt received. Stopping consumer")
                await self.close()
            except Exception as ex:
                self._logger.error("Unhandled error while consuming message", exc_info=ex)
                await self.close(exc_info=ex)
