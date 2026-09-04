import inspect
import logging
from collections.abc import Callable
from typing import Any, get_type_hints

from pydantic import BaseModel

HandlerType = Callable[..., Any]
log = logging.getLogger(__name__)


def resolve_input_models(callable: HandlerType) -> type[BaseModel] | None:
    sig = inspect.signature(callable)
    for param in sig.parameters.values():
        if issubclass(param.annotation, BaseModel):
            return param.annotation
    return None


def resolve_output_model(callable: HandlerType) -> type[BaseModel] | None:
    return_type = get_type_hints(callable).get("return")
    if return_type and issubclass(return_type, BaseModel):
        log.debug("Return type resolved as %r")
        return return_type

    log.warning("Unable to resolve a pydantic return type")
    return None
