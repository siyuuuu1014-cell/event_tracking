import asyncio
from contextlib import suppress

from confluent_kafka import KafkaException, Producer


class DeliveryUnknown(Exception):
    pass


class BrokerUnavailable(Exception):
    pass


class KafkaPublisher:
    def __init__(self, bootstrap: str) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": 5000,
                "request.timeout.ms": 4000,
                "linger.ms": 5,
                "queue.buffering.max.messages": 10000,
            }
        )
        self._poller: asyncio.Task | None = None

    async def _poll(self) -> None:
        while True:
            self.producer.poll(0)
            await asyncio.sleep(0.02)

    async def publish(self, topic: str, key: str, body: bytes, signature: str) -> None:
        loop = asyncio.get_running_loop()
        if self._poller is None:
            self._poller = loop.create_task(self._poll())
        delivered = loop.create_future()

        def callback(error, _message) -> None:
            if not delivered.done():
                delivered.set_result(error)

        try:
            self.producer.produce(
                topic, key=key, value=body, headers={"analytics-signature": signature},
                on_delivery=callback,
            )
        except (KafkaException, BufferError) as error:
            raise BrokerUnavailable from error
        try:
            error = await asyncio.wait_for(delivered, timeout=4)
        except TimeoutError as error:
            raise DeliveryUnknown from error
        if error is not None:
            raise DeliveryUnknown

    async def close(self) -> None:
        if self._poller is not None:
            self._poller.cancel()
            with suppress(asyncio.CancelledError):
                await self._poller
