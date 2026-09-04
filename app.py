import asyncio
import logging
import random
from typing import Any

from azure.servicebus.aio import ServiceBusClient

from brit.fastevent import exceptions as E
from brit.fastevent.consumer import Consumer
from brit.fastevent.handler import Handler

log = logging.getLogger(__name__)


class EventApp:
    def __init__(self, sb_client: ServiceBusClient) -> None:
        self.handlers: list[Handler] = []
        self.sb_client: ServiceBusClient = sb_client
        self._receivers: list[asyncio.Task] = []
        self.exiting: bool = False
        self._sb_backoff: int = 1

    def include_handler(self, router: Handler) -> None:
        self.handlers.append(router)

    async def _receiver_supervisor(
        self, consumer: Consumer, client: ServiceBusClient
    ) -> None:
        while not self.exiting:
            try:
                await self._run_receiver(consumer, client)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(
                    "Receiver crashed for topic=%s subscription=%s. Error=%r",
                    consumer.topic,
                    consumer.subscription,
                    e,
                )
                # Don't thundering herd
                await asyncio.sleep(self._sb_backoff + random.random())

    async def run(self, blocking: bool = True) -> None:
        log.info("Starting fastevent...")

        main_loop = asyncio.get_running_loop()

        # Setup producers sync.
        for router in self.handlers:
            # Setup handler with correct loop and client.
            router._initialize_sb_client(self.sb_client, main_loop)

        receivers = []
        for router in self.handlers:
            # Setup consumers (task/thread) for each consumer
            for _, consumer in router.consumers.items():
                task = asyncio.create_task(
                    self._receiver_supervisor(consumer, self.sb_client)
                )
                receivers.append(task)

        self._receivers = receivers
        log.info("Started receivers and running.")

        if blocking:
            try:
                await asyncio.gather(*receivers)
            except asyncio.CancelledError:
                pass

    def stop(self) -> None:
        self.exiting = True
        log.info("Stopping receiving.")
        for receiver in self._receivers:
            log.debug("Canceling task %r", receiver)
            receiver.cancel()

    async def _run_receiver(self, consumer: Consumer, client: ServiceBusClient) -> None:
        log.debug("Starting receiver")

        match (consumer.topic, consumer.subscription, consumer.queue):
            case (topic, subscription, _) if topic and subscription:
                _client = client.get_subscription_receiver(
                    topic_name=topic,
                    subscription_name=subscription,
                )
            case (_, _, queue) if isinstance(queue, str):
                _client = client.get_queue_receiver(queue_name=queue)
            case _:  # pragma: nocover
                raise E.ConfigurationError("Invalid consumer configuration")

        try:
            async with _client as receiver:
                log.debug("Starting receiving of messages...")

                while not self.exiting:
                    messages = await receiver.receive_messages(  # type: ignore[attr-defined]
                        max_message_count=10,
                        max_wait_time=1,
                    )
                    for message in messages:
                        try:
                            await consumer.handle_message(message)
                        except E.RetryableException:
                            await receiver.abandon_message(message)
                        except Exception as e:
                            log.warning("DLQ'ed message: %r due to %r", message, e)
                            await receiver.dead_letter_message(message)
                        else:
                            await receiver.complete_message(message)

        # This is the app trying to exit.
        except asyncio.CancelledError:
            log.debug("Receiver cancelled.")
            raise

        # This will be caught by the supervisor and trigger a restart
        except Exception as e:
            log.error("Service Bus failure in receiver: %r", e)
            raise

    def generate_asyncapi_spec(
        self,
        title: str = "FastEvent Messaging API",
        version: str = "1.0.0",
        description: str = "AsyncAPI 3.0.0 spec.",
        server_host: str = "127.0.0.1",
        server_name: str = "development",
        server_protocol: str = "amqp",
        server_protocol_version: str = "1.0",
        server_description: str = "Local Azure Service Bus instance",
    ) -> dict[str, Any]:
        # Build server payload.
        servers = {
            server_name: {
                "host": server_host,
                "protocol": server_protocol,
                "protocolVersion": server_protocol_version,
                "description": server_description,
            }
        }

        channels: dict[str, Any] = {}
        operations: dict[str, Any] = {}
        schemas: dict[str, Any] = {}
        messages: dict[str, Any] = {}

        for handler in self.handlers:
            spec = handler._generate_asyncapi_spec(server=server_name)
            channels = {**channels, **spec.channels}
            operations = {**operations, **spec.operations}
            schemas = {**schemas, **spec.components["schemas"]}
            messages = {**messages, **spec.components["messages"]}

        asyncapi_spec = {
            "asyncapi": "3.0.0",
            "info": {
                "title": title,
                "version": version,
                "description": description,
            },
            "defaultContentType": "application/json",
            "channels": channels,
            "operations": operations,
            "components": {"schemas": schemas, "messages": messages},
            "servers": servers,
        }

        return asyncapi_spec
