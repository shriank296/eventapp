from typing import Any, Mapping

from opentelemetry.propagators.textmap import Getter


class DiagnosticIdGetter(Getter[Mapping[str, Any]]):
    """Gets diagnostic id from application properties.

    Falls back on using Diagonstic-Id for app insights.
    """

    def get(self, carrier: Mapping[str, Any], key: str) -> list[str] | None:
        if not carrier:
            return None

        value = carrier.get(key)
        if value is None and key == "traceparent":
            value = carrier.get("Diagnostic-Id")
        return [value] if value else None

    def keys(self, carrier: Mapping[str, Any] | None) -> list[str]:
        if not carrier:
            return []
        return list(carrier.keys())
