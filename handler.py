# ruff: noqa: C901
import asyncio
import functools
import inspect
import logging
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Sequence, Type, TypeVar, overload

from azure.servicebus.aio import ServiceBusClient
from pydantic import BaseModel, ValidationError

from brit.fastevent import exceptions as E
from brit.fastevent.consumer import Consumer
from brit.fastevent.fixtures import Response
from brit.fastevent.producer import Producer, QueueProducer, TopicProducer

log = logging.getLogger(__name__)

R = TypeVar("R")


@dataclass(frozen=True)
class _Consumer:
    topic: str | None
    subscription: str | None
    queue: str | None


@dataclass(frozen=True)
class _Producer:
    topic: str | None
    queue: str | None


@dataclass(frozen=True)
class _AsyncAPIHandlerSpec:
    channels: dict[str, Any]
    operations: dict[str, Any]
    components: dict[str, Any]


def _build_producer_fixtures(
    sig: inspect.Signature,
    kwargs: dict[str, Any],
) -> dict[type[Response], Response]:
    fixtures: dict[type[Response], Response] = {Response: Response()}

    for name, param in sig.parameters.items():
        if name not in kwargs:
            dependency_type = param.annotation
            if dependency_type in fixtures:
                kwargs[name] = fixtures[dependency_type]

    return fixtures


class Handler:
    def __init__(self) -> None:
        self.consumers: dict[_Consumer, Consumer] = dict()
        self.producers: Mapping[_Producer, list[Producer]] = defaultdict(list)
        self._main_loop: asyncio.AbstractEventLoop | None = None

    def _initialize_sb_client(
        self, sb_client: ServiceBusClient, main_loop: asyncio.AbstractEventLoop
    ) -> None:
        self._main_loop = main_loop

        for topic in self.producers.values():
            for producer in topic:
                producer.initialize_sb_client(sb_client)

    @overload
    def consumer(
        self,
        *,
        queue: str,
        topic: None = None,
        subscription: None = None,
        retryable_exceptions: Sequence[type[Exception]] | None = None,
        header_validator: type[BaseModel] | None = None,
        operation_id: str | None = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:  # pragma: no cover
        """Queue api."""

    @overload
    def consumer(
        self,
        *,
        topic: str,
        subscription: str,
        queue: str | None = None,
        retryable_exceptions: Sequence[type[Exception]] | None = None,
        header_validator: type[BaseModel] | None = None,
        operation_id: str | None = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:  # pragma: no cover
        """Topic and subscription api."""

    def consumer(  # noqa: C901
        self,
        *,
        topic: str | None = None,
        subscription: str | None = None,
        queue: str | None = None,
        retryable_exceptions: Sequence[type[Exception]] | None = None,
        header_validator: type[BaseModel] | None = None,
        operation_id: str | None = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:
        channel = f"{topic}/{subscription}" if topic else queue
        if not operation_id:
            operation_id = f"consume_{channel}"

        def decorator(func: Callable[..., R]) -> Callable[..., R]:
            if not callable(func):
                raise E.DecorationError("Consumer must be applied to a callable.")

            sig = inspect.signature(func)

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> R:
                fixtures = {type(v): v for _, v in kwargs.items()}
                log.debug("Fixtures: %r", str(fixtures))

                # Rewrite fixtures injected from upstream to kwargs
                # expected in the function site.
                for name, param in sig.parameters.items():
                    dependency_type = param.annotation
                    if dependency_type in fixtures:
                        kwargs[name] = fixtures[dependency_type]

                # Clear out injected fixtures from kwargs to
                # ensure we don't raise a TypeError here.
                attributes = {k for k, _ in sig.parameters.items()}
                final_kwargs = {}
                for k in kwargs:
                    if k not in attributes:
                        log.debug(
                            "keyword arg: %r not found in attributes: %r", k, attributes
                        )
                        continue
                    else:
                        final_kwargs[k] = kwargs[k]

                log.debug("Our final kwargs are %r", final_kwargs)

                return func(*args, **final_kwargs)

            key = _Consumer(topic=topic, subscription=subscription, queue=queue)

            if self.consumers.get(key):
                raise E.DuplicateSubscriptionError(
                    f"Duplicate handler for topic '{topic}' "
                    f"and subscription '{subscription}' or queue '{queue}'."
                )

            # Use the wrapped function NOT the func.
            route = Consumer(
                topic,
                subscription,
                queue,
                wrapper,
                operation_id,
                retryable_exceptions,
                header_validator=header_validator,
            )
            if route.input_model is None:
                raise E.DecorationError("consumer must have an input model.")

            self.consumers[key] = route

            return wrapper

        return decorator

    @overload
    def producer(
        self,
        *,
        topic: str,
        queue: None = None,
        header_validator: type[BaseModel] | None = None,
        require_correlation_id: bool = False,
        require_message_id: bool = False,
        require_session_id: bool = False,
        operation_id: str | None = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:  # pragma: no cover
        ...

    @overload
    def producer(
        self,
        *,
        topic: None = None,
        queue: str,
        header_validator: type[BaseModel] | None = None,
        require_correlation_id: bool = False,
        require_message_id: bool = False,
        require_session_id: bool = False,
        operation_id: str | None = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:  # pragma: no cover
        ...

    def producer(
        self,
        *,
        topic: str | None = None,
        queue: str | None = None,
        header_validator: type[BaseModel] | None = None,
        require_correlation_id: bool = False,
        require_message_id: bool = False,
        require_session_id: bool = False,
        operation_id: str | None = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:
        if all([topic, queue]) or not any([topic, queue]):
            raise E.DecorationError("producer must accept one of either topic or queue")

        if not operation_id:
            operation_id = f"produce_{topic if topic else queue}"

        def decorator(func: Callable[..., R]) -> Callable[..., R]:
            if not callable(func):
                raise E.DecorationError("Producer must be applied to a callable.")

            sig = inspect.signature(func)

            async def _produce(
                response: Any, fixtures: dict[type[Response], Response]
            ) -> None:
                if require_correlation_id and not fixtures[Response].correlation_id:
                    raise E.ValidationError("Correlation ID required but not given.")
                if require_message_id and not fixtures[Response].message_id:
                    raise E.ValidationError("Message ID required but not given.")
                if require_session_id and not fixtures[Response].session_id:
                    raise E.ValidationError("Session ID required but not given.")

                if header_validator:
                    try:
                        header_validator.model_validate(
                            fixtures[Response].application_properties
                        )
                    except ValidationError as e:
                        raise E.ValidationError(
                            "Application properties does not validate."
                        ) from e

                log.debug("Producing message.")
                await route.produce_message(
                    response,
                    correlation_id=fixtures[Response].correlation_id,
                    session_id=fixtures[Response].session_id,
                    application_properties=fixtures[Response].application_properties,
                    scheduled_enqueue_time_utc=fixtures[
                        Response
                    ].scheduled_enqueue_time_utc,
                )

            wrapper: Callable[[], R] | Callable[[], Awaitable[R]]

            if inspect.iscoroutinefunction(func):

                @functools.wraps(func)
                async def async_wrapper(*args: Any, **kwargs: Any) -> R:
                    log.debug("Async wrapper hit")
                    fixtures = _build_producer_fixtures(sig, kwargs)
                    response = await func(*args, **kwargs)
                    await _produce(response, fixtures)
                    return response

                wrapper = async_wrapper

            else:

                @functools.wraps(func)
                def sync_wrapper(*args: Any, **kwargs: Any) -> R:
                    log.debug("Sync wrapper hit")
                    fixtures = _build_producer_fixtures(sig, kwargs)
                    response = func(*args, **kwargs)

                    if not self._main_loop:
                        # TODO: Make this something more useful.
                        raise E.ProducerInitilizationError("No event loop found?")

                    # Check to see if we're in a running event loop already
                    try:
                        running_loop = asyncio.get_running_loop()
                        # We're in a running loop in the current thread
                        if running_loop == self._main_loop:
                            # Same loop, schedule in the background
                            asyncio.create_task(_produce(response, fixtures))
                        else:
                            # Different loop, use run_coroutine_threadsafe
                            asyncio.run_coroutine_threadsafe(
                                _produce(response, fixtures),
                                self._main_loop,
                            )
                    except RuntimeError:
                        # No running loop in current thread
                        # Check if the main loop is running in another thread
                        if self._main_loop.is_running():
                            # Loop is running in another thread, schedule and wait
                            log.debug("Main loop already exists, scheduling coroutine")
                            future = asyncio.run_coroutine_threadsafe(
                                _produce(response, fixtures),
                                self._main_loop,
                            )
                            # Wait for completion
                            future.result(timeout=10)
                        else:
                            # Loop is not running, run it to completion
                            log.debug("No running loop, running produce synchronously")
                            self._main_loop.run_until_complete(
                                _produce(response, fixtures)
                            )

                    return response

                wrapper = sync_wrapper

            key = _Producer(topic=topic, queue=queue)

            if topic is not None:
                route: Producer = TopicProducer(topic, func, operation_id)
                self.producers[key].append(route)
            elif queue is not None:
                route = QueueProducer(queue, func, operation_id)
                self.producers[key].append(route)
            else:  # pragma: no cover
                raise E.DecorationError(
                    "producer must accept one of either topic or queue"
                )

            return wrapper  # type: ignore

        return decorator

    def _generate_asyncapi_spec(self, server: str = "local") -> _AsyncAPIHandlerSpec:
        """Generate an AsyncAPI 3.0.0 spec from all producers and consumers."""

        def schema_from_model(model: Type[BaseModel]) -> dict[str, Any]:
            schema = model.model_json_schema(
                ref_template="#/components/schemas/{model}"
            )
            return schema

        channels: dict[str, Any] = {}
        operations: dict[str, Any] = {}
        components: dict[str, Any] = {"schemas": {}, "messages": {}}
        registered_models: dict[str, Type[BaseModel]] = {}

        def ensure_channel(topic: str) -> dict[str, Any]:
            if topic not in channels:
                channels[topic] = {
                    "address": topic,
                    "messages": {},
                    "servers": [{"$ref": f"#/servers/{server}"}],
                }
            return channels[topic]["messages"]

        # Consumers → receive
        for c_desc, consumer in self.consumers.items():
            input_model = consumer.input_model
            msg_name = input_model.__name__ if input_model else "UnknownMessage"

            if input_model and issubclass(input_model, BaseModel):
                registered_models[msg_name] = input_model
                payload_ref = {"$ref": f"#/components/schemas/{msg_name}"}
            # This isn't really supported for now.
            else:  # pragma: no cover
                payload_ref = None

            message_ref = f"#/components/messages/{msg_name}"

            # Add to components.messages if not already present
            if msg_name not in components["messages"]:
                msg_obj: dict[str, Any] = {
                    "name": msg_name,
                    "title": msg_name,
                    "contentType": "application/json",
                }
                if consumer.header_validator:
                    msg_obj["headers"] = schema_from_model(consumer.header_validator)
                if payload_ref:
                    msg_obj["payload"] = payload_ref
                components["messages"][msg_name] = msg_obj

            # Add to channel.messages
            _channel = c_desc.topic if c_desc.topic else c_desc.queue
            ensure_channel(_channel)[msg_name] = {"$ref": message_ref}  # type: ignore

            operations[consumer.operation_id] = {
                "action": "receive",
                "channel": {"$ref": f"#/channels/{c_desc.topic}"},
                "summary": consumer.handler.__doc__,
                "bindings": {
                    "x-azure-service-bus": {"subscription": c_desc.subscription}
                },
            }

        # Producers → send
        for p_desc, producers in self.producers.items():
            _path = p_desc.topic if p_desc.topic else p_desc.queue
            if not _path:  # pragma: no cover
                raise E.SchemaGenerationError("No topic or queue set?")

            for producer in producers:
                output_model = producer.output_model
                msg_name = output_model.__name__ if output_model else "UnknownMessage"

                if output_model and issubclass(output_model, BaseModel):
                    registered_models[msg_name] = output_model
                    payload_ref = {"$ref": f"#/components/schemas/{msg_name}"}
                # This isn't really supported for now.
                else:  # pragma: no cover
                    payload_ref = None

                message_ref = f"#/components/messages/{msg_name}"

                if msg_name not in components["messages"]:
                    msg_obj = {
                        "name": msg_name,
                        "title": msg_name,
                        "contentType": "application/json",
                    }
                    if payload_ref:
                        msg_obj["payload"] = payload_ref
                    components["messages"][msg_name] = msg_obj

                # Add to channel.messages
                ensure_channel(_path)[msg_name] = {"$ref": message_ref}

                operations[producer.operation_id] = {
                    "action": "send",
                    "channel": {"$ref": f"#/channels/{_path}"},
                    "summary": producer.handler.__doc__,
                }

        # Register schemas
        for name, model in registered_models.items():
            schema = schema_from_model(model)

            # Register the main schema
            components["schemas"][name] = {
                k: v for k, v in schema.items() if k != "$defs"
            }

            # Promote nested models from $defs
            defs = schema.get("$defs", {})
            for def_name, def_schema in defs.items():
                if def_name not in components["schemas"]:
                    components["schemas"][def_name] = def_schema

        return _AsyncAPIHandlerSpec(
            channels=channels,
            operations=operations,
            components=components,
        )
