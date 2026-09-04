import asyncio
import inspect
import logging
from typing import Any, Generic, ParamSpec, Sequence, TypeVar

from azure.servicebus import ServiceBusMessage
from opentelemetry import trace
from opentelemetry.instrumentation.threading import ThreadingInstrumentor
from opentelemetry.propagate import extract
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import Span
from pydantic import BaseModel, ValidationError

from brit.fastevent import exceptions as E
from brit.fastevent.fixtures import Request
from brit.fastevent.types import HandlerType, resolve_input_models
from brit.fastevent.utils import DiagnosticIdGetter

ThreadingInstrumentor().instrument()

log = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

P = ParamSpec("P")
R = TypeVar("R")
H = TypeVar("H", bound=BaseModel | None)


class Consumer(Generic[H]):
    def __init__(
        self,
        topic: str | None,
        subscription: str | None,
        queue: str | None,
        handler: HandlerType,
        operation_id: str,
        retryable_exceptions: Sequence[type[Exception]] | None = None,
        header_validator: type[H] | None = None,
        require_correlation_id: bool | None = False,
        require_message_id: bool | None = False,
        require_session_id: bool | None = False,
    ) -> None:
        self.topic = topic
        self.subscription = subscription
        self.queue = queue
        self.handler = handler
        self.input_model = resolve_input_models(handler)
        self.handler_types = {
            v.annotation: k for k, v in inspect.signature(handler).parameters.items()
        }
        self.operation_id = operation_id
        self.retryable_exceptions = retryable_exceptions if retryable_exceptions else []
        self.header_validator = header_validator

        if not (topic and subscription) and not queue or (topic and queue):
            raise E.ConfigurationError(
                "Either topic and subscription or queue must be provided."
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Consumer):
            raise E.NotImplementedError("Unable to compare non consumers.")
        return (self.topic, self.subscription) == (other.topic, other.subscription)

    def __hash__(self) -> int:
        return hash((self.topic, self.subscription))

    def _add_trace_attributes(self, raw_message: ServiceBusMessage, span: Span) -> None:
        # Add some common debugging attributes to the span.
        if raw_message.correlation_id:
            span.set_attribute("consumer.sb.correlation_id", raw_message.correlation_id)
        if raw_message.message_id:
            span.set_attribute("consumer.sb.message_id", raw_message.message_id)
        if raw_message.application_properties:
            for k, v in raw_message.application_properties.items():
                key = k.decode() if isinstance(k, bytes) else k
                v = v.decode() if isinstance(v, bytes) else v
                span.set_attribute(
                    f"consumer.sb.application_properties.{key}",
                    str(v),
                )

    async def handle_message(self, raw_message: ServiceBusMessage) -> Any:
        """Deserialize the message, call the handler, serialize the output."""
        log.info("Received message: %r", raw_message)
        body = "".join([x.decode() for x in raw_message.body])

        context = extract(  # type: ignore[misc]
            raw_message.application_properties,
            getter=DiagnosticIdGetter(),
        )

        span_context = trace.get_current_span(context).get_span_context()

        links = [Link(span_context)]

        _path = f"{self.topic}/{self.subscription}" if self.topic else self.queue
        with tracer.start_as_current_span(
            f"Consumer.handle_message {_path}",
            links=links,
            kind=SpanKind.CONSUMER,
        ) as span:
            self._add_trace_attributes(raw_message, span)

            header: BaseModel | None = None
            if self.header_validator:
                try:
                    raw_header = (
                        raw_message.application_properties
                        if raw_message.application_properties
                        else {}
                    )
                    _header = {
                        k.decode() if isinstance(k, bytes) else k: (
                            v.decode() if isinstance(v, bytes) else v
                        )
                        for k, v in raw_header.items()
                    }
                    header = self.header_validator.model_validate(  # type: ignore[attr-defined]
                        _header
                    )
                except ValidationError as e:
                    raise E.ValidationError("Unable to validate header.") from e

            try:
                input_obj = (
                    self.input_model.model_validate_json(body)
                    if self.input_model
                    else body
                )
            except ValidationError as e:
                log.debug(
                    "Caught a validation error from pydantic: %r", e, exc_info=True
                )
                raise e

            log.debug("Parsed input object.", extra={"input_obj": input_obj})

            # Setup request fixture from raw message.
            # TODO: This feels a bit messy, come up with a better way to
            # handle fixtures and checking whether they should be inserted.
            # This essentially means we're doing it in two places which doesn't
            # feel right.
            fixtures = {}
            if Request in self.handler_types.keys():
                request = Request(
                    raw_message=raw_message,
                    application_properties=raw_message.application_properties,
                    correlation_id=raw_message.correlation_id,
                    session_id=raw_message.session_id,
                    message_id=raw_message.message_id,
                    enqueued_time_utc=getattr(raw_message, "enqueued_time_utc", None),
                    header=header,
                )
                # Map the value back to the correct fixture.
                fixtures[self.handler_types[Request]] = request

            try:
                result = await self._maybe_async(self.handler, input_obj, **fixtures)
            except Exception as e:
                if type(e) in self.retryable_exceptions:
                    log.debug(
                        "Caught %r when running callable. Retrying.",
                        e,
                        exc_info=True,
                    )
                    raise E.RetryableException("Caught a retryable exception") from e

                log.debug("Raising uncaught exception", exc_info=True)
                raise

            log.debug("Execution succesful, returning result for other decorators.")
            return result

    async def _maybe_async(self, func: HandlerType, *args: Any, **kwargs: Any) -> Any:
        log.debug("Maybe handler checking inner function colour...")
        if hasattr(func, "__wrapped__"):
            if inspect.iscoroutinefunction(func.__wrapped__):  # type: ignore
                log.debug("Calling using async")
                return await func(*args, **kwargs)

        # Run the synchronous function in a thread to
        # avoid blocking the event loop
        log.debug("Sync handler, pushing this to thread.")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
