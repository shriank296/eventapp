import json
import warnings
from collections.abc import MutableMapping
from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from azure.servicebus import ServiceBusMessage
from pydantic import BaseModel

type ApplicationProperties = MutableMapping[
    str | bytes, int | float | bytes | bool | str | UUID
]

H = TypeVar("H", bound=BaseModel | None)


class RequestResponseBase:
    _application_properties: ApplicationProperties
    correlation_id: str | None
    session_id: str | None
    message_id: str | None

    def __init__(
        self,
        application_properties: ApplicationProperties | None = None,
        correlation_id: str | None = None,
        session_id: str | None = None,
        message_id: str | None = None,
    ) -> None:
        self._application_properties = (
            application_properties if application_properties else {}
        )
        self.correlation_id = correlation_id
        self.session_id = session_id
        self.message_id = message_id

    @property
    def application_properties(self) -> ApplicationProperties:
        warnings.warn(
            (
                "application_properties is deprecated and will be removed in a future"
                "release."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return self._application_properties


class Request(RequestResponseBase, Generic[H]):
    raw_message: ServiceBusMessage

    enqueued_time_utc: datetime | None
    _header: H | None

    def __init__(
        self,
        raw_message: ServiceBusMessage,
        application_properties: ApplicationProperties | None = None,
        correlation_id: str | None = None,
        session_id: str | None = None,
        message_id: str | None = None,
        enqueued_time_utc: datetime | None = None,
        header: H | None = None,
    ) -> None:
        super().__init__(application_properties, correlation_id, session_id, message_id)
        self.raw_message = raw_message
        self.enqueued_time_utc = enqueued_time_utc
        self._header = header

    @property
    def header(self) -> H | None:
        return self._header


class Response(RequestResponseBase):
    scheduled_enqueue_time_utc: datetime | None
    _header: BaseModel | None

    def __init__(
        self,
        application_properties: ApplicationProperties | None = None,
        correlation_id: str | None = None,
        session_id: str | None = None,
        message_id: str | None = None,
        scheduled_enqueue_time_utc: datetime | None = None,
        header: BaseModel | None = None,
    ) -> None:
        super().__init__(application_properties, correlation_id, session_id, message_id)
        self.scheduled_enqueue_time_utc = scheduled_enqueue_time_utc
        self._header = header

    @property
    def application_properties(self) -> ApplicationProperties:
        warnings.warn(
            (
                "application_properties is deprecated and will be removed in a future "
                "release."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return self._application_properties

    @application_properties.setter
    def application_properties(self, value: ApplicationProperties | None) -> None:
        if value is None:
            return

        if self._header is not None:
            raise ValueError("Cannot set both header and application_properties.")

        self._application_properties = value

    @property
    def header(self) -> BaseModel | None:
        return self._header

    @header.setter
    def header(self, value: BaseModel | None) -> None:
        if value is None:
            return

        if not isinstance(value, BaseModel):
            raise ValueError("Header must be a Pydantic BaseModel instance or None.")

        self._header = value
        if self._application_properties:
            raise ValueError("Cannot set both header and application_properties.")
        else:
            self._application_properties = json.loads(value.model_dump_json())
