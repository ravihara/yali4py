import asyncio
import json
import time
from typing import Dict

from aio_pika.abc import AbstractIncomingMessage

from .common import PubSubConfig, RMQBatchBuffer
from .pubsub import YaliPubSub


class YaliBatchPubSub(YaliPubSub):
    def __init__(self, config: PubSubConfig):
        super().__init__(config)

        self.__buffer = RMQBatchBuffer(
            max_entries=self._config.max_batch_entries, max_size=self._config.max_batch_size
        )

        self.__batch_interval = self._config.batch_interval
        self.__periodic_wait = self._config.batch_interval
        self.__last_processed_time = time.time()
        self.__run_periodic = False

        self.__aio_lock = asyncio.Lock()
        self.__aio_periodic_task: asyncio.Task | None = None

    async def __process_batch(self):
        buffer, last_message = self.__buffer.enqueued()
        assert len(buffer) > 0
        assert last_message is not None

        try:
            await self._q_executor.submit(self._config.data_processor(buffer))
            await last_message.ack(multiple=True)

            self._logger.info(f"Processed batch with {len(buffer)} messages")
            self.__buffer.reset()
        except Exception as ex:
            self._logger.error("Unhandled error while processing batch", exc_info=ex)
            await last_message.nack(multiple=True, requeue=False)
            self.__buffer.reset()

        self.__last_processed_time = time.time()
        self.__periodic_wait = self.__batch_interval

    async def run_periodic_processing(self):
        while self.__run_periodic:
            try:
                self._logger.debug(
                    f"Waiting for {self.__periodic_wait} seconds before running periodic batch task"
                )
                await asyncio.sleep(self.__periodic_wait)

                time_elapsed = int(time.time() - self.__last_processed_time)
                self._logger.debug(f"Time elapsed since last processed message: {time_elapsed}s")

                if time_elapsed >= self.__batch_interval:
                    if self.__buffer.is_empty():
                        self._logger.debug("No messages to process in the batch")
                        continue

                    self._logger.info(
                        f"Running periodic batch task as {time_elapsed}s have elapsed since last processed message"
                    )

                    async with self.__aio_lock:
                        await self.__process_batch()
                else:
                    self.__periodic_wait = self.__batch_interval - time_elapsed
            except Exception as ex:
                self._logger.error("Unhandled error while running periodic batch task", exc_info=ex)

    async def process_message(self, message: AbstractIncomingMessage):
        try:
            async with self.__aio_lock:
                if self.__buffer.is_full():
                    await self.__process_batch()
                    return

                mesg_text = message.body.decode("utf-8")
                mesg_json: Dict = json.loads(mesg_text)

                if self._config.data_preprocessor:
                    mesg_json = await self._q_executor.submit(
                        self._config.data_preprocessor(mesg_json)
                    )

                size = self.__buffer.append(data=mesg_json, message=message)
                self._logger.info(
                    f"Appended {size} bytes to batch with message delivery-tag: {message.delivery_tag}"
                )
        except Exception as ex:
            self._logger.error("Unhandled error while processing message", exc_info=ex)

    async def close(self, exc_info: BaseException | None = None):
        self.__run_periodic = False

        if self.__aio_periodic_task and (not self.__aio_periodic_task.done()):
            try:
                await asyncio.wait_for(self.__aio_periodic_task, timeout=self.__batch_interval)
            except asyncio.TimeoutError:
                self._logger.warning(
                    f"Periodic task {self.__aio_periodic_task} was cancelled due to wait timeout"
                )
            except Exception as ex:
                self._logger.error(
                    "Unhandled error while waiting for periodic task to complete", exc_info=ex
                )

        self.__aio_periodic_task = None

        await super().close(exc_info=exc_info)

    async def consume(self):
        if self._is_running:
            self._logger.warning(f"Pubsub {self._config.service} is already running")
            return

        try:
            self.__run_periodic = True
            self.__aio_periodic_task = asyncio.create_task(self.run_periodic_processing())
            await super().consume()
        except asyncio.CancelledError:
            self._logger.warning("Cancellation received. Stopping consumer")
            await self.close()
        except Exception as ex:
            self._logger.error("Unhandled error while consuming message", exc_info=ex)
            await self.close(exc_info=ex)
