import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from azure.servicebus import ServiceBusMessage
from azure.servicebus import exceptions as SBE
from azure.servicebus.aio import ServiceBusClient, ServiceBusSender
from opentelemetry import trace
from opentelemetry.trace import Link, SpanKind
from pydantic import BaseModel

from brit.fastevent import exceptions as E
from brit.fastevent.types import HandlerType, resolve_output_model

log = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
application_properties = Mapping[str | bytes, int | float | bytes | bool | str | UUID]


class Producer(ABC):
    receiver_name: str = "Producer name not specified"
    receiver_type: str = "Producer type not specified"

    def __init__(
        self,
        handler: HandlerType,
        operation_id: str,
    ) -> None:
        self.handler = handler
        self.output_model = resolve_output_model(self.handler)
        self._servicebus_client: ServiceBusClient | None = None
        self._sender: ServiceBusSender | None = None
        self.operation_id = operation_id

    def initialize_sb_client(
        self, sb_client: ServiceBusClient
    ) -> ServiceBusClient | None:
        if not self._servicebus_client:
            self._servicebus_client = sb_client
        else:
            raise E.ProducerInitilizationError("ServiceBusClient double initialized.")
        return self._servicebus_client

    def _get_sb_client(self) -> ServiceBusClient:
        log.debug("SB client is: %r", self._servicebus_client)
        if not self._servicebus_client:
            raise E.ProducerInitilizationError("Uninitialized client.")

        return self._servicebus_client

    async def produce_message(
        self,
        payload: BaseModel | Any,
        *,
        application_properties: application_properties | None = None,
        correlation_id: str | None = None,
        session_id: str | None = None,
        scheduled_enqueue_time_utc: datetime | None = None,
    ) -> None:
        """
        Produce a message to the service bus.

        Args:
            payload: The message payload, which must be a Pydantic BaseModel instance.

        KWargs:
            application_properties: Metadata to include in the message. Defaults to
                None.

        Raises:
            ValueError: If the payload is not an instance of pydantic.BaseModel.
            ServiceBusError: If sending the message to the service bus fails.
            NotInititializedError: If the ServiceBusClient is not initialized.
        """
        if application_properties is None:
            application_properties = {}

        if self.output_model is None or not isinstance(payload, self.output_model):
            raise E.SerialisationError(
                "Payload must be an instance of: %r", self.output_model
            )

        message_id = str(uuid4())

        span_context = trace.get_current_span().get_span_context()
        links = [Link(span_context)]

        with tracer.start_as_current_span(
            f"Producer.produce_message {self.receiver_name}",
            links=links,
            kind=SpanKind.PRODUCER,
        ) as span:
            # Add some common debugging attributes to the span.
            span.set_attribute("producer.sb.message_id", message_id)

            if correlation_id:
                span.set_attribute("producer.sb.correlation_id", correlation_id)
            if session_id:
                span.set_attribute("producer.sb.session_id", session_id)

            for k, v in application_properties.items():
                key = k.decode() if isinstance(k, bytes) else k
                v = v.decode() if isinstance(v, bytes) else v
                span.set_attribute(
                    f"producer.sb.application_properties.{key}",
                    str(v),
                )

            message_body = payload.model_dump_json()
            log.debug("Producing payload rendered as: %r", message_body)

            message = ServiceBusMessage(
                body=message_body,
                application_properties=application_properties,  # type: ignore[arg-type]
                correlation_id=correlation_id,
                session_id=session_id,
                scheduled_enqueue_time_utc=scheduled_enqueue_time_utc,
                message_id=message_id,
            )

            try:
                sender = self.get_sender()
                await sender.send_messages(message)

                log.debug(
                    "Message sent to %s %r: %s",
                    self.receiver_type,
                    self.receiver_name,
                    message_body,
                )

            # This is only the service bus error.
            except SBE.ServiceBusError as e:
                log.error(
                    "Unable to send message to %s: %r, cause: %s",
                    self.receiver_type,
                    self.receiver_name,
                    str(e),
                )
                raise E.ServiceBusFailure(
                    f"Unable to send message to {self.receiver_type}."
                ) from e

    @abstractmethod
    def get_sender(self) -> ServiceBusSender:  # pragma: no cover
        pass


class TopicProducer(Producer):
    receiver_type = "topic"

    def __init__(self, topic: str, handler: HandlerType, operation_id: str) -> None:
        super().__init__(handler, operation_id)
        self.receiver_name = topic

    def get_sender(self) -> ServiceBusSender:
        if not self._sender:
            self._sender = self._get_sb_client().get_topic_sender(self.receiver_name)

        return self._sender


class QueueProducer(Producer):
    receiver_type = "queue"

    def __init__(self, queue: str, handler: HandlerType, operation_id: str) -> None:
        super().__init__(handler, operation_id)
        self.receiver_name = queue

    def get_sender(self) -> ServiceBusSender:
        if not self._sender:
            self._sender = self._get_sb_client().get_queue_sender(self.receiver_name)

        return self._sender
