_NotImplementedError = NotImplementedError


class FastEventBaseException(Exception):
    """Base exception used for all FastEvent exceptions."""


class ProducerInitilizationError(FastEventBaseException):
    """Initalizing the producer with a service bus client has failed."""


class StopException(FastEventBaseException):
    """Was unable to stop the app."""


class NotImplementedError(FastEventBaseException, _NotImplementedError):
    """This functionality is not implemented."""


class DecorationError(FastEventBaseException):
    """There was an error with the decoration of a function."""


class DuplicateSubscriptionError(FastEventBaseException):
    """There was a duplicate subscription for a topic."""


class SerialisationError(FastEventBaseException):
    """There was an error serialising or deserialising a message."""


class ServiceBusFailure(FastEventBaseException):
    """There was a failure with Azure ServiceBus."""


class RetryableException(FastEventBaseException):
    """Exception that has been decided is retryable."""


class ValidationError(FastEventBaseException):
    """A validation error occured while sending a message."""


class ConfigurationError(FastEventBaseException):
    """There was an error with the configuration of the application."""


class SchemaGenerationError(FastEventBaseException):
    """An error occured while generating an AsyncAPI spec."""
