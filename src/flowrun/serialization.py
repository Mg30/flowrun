"""Pluggable result serializers for persistent state backends."""

from __future__ import annotations

import json
import pickle
from typing import Any, Protocol


class ResultSerializer(Protocol):
    """Structural interface for serializing task results to/from bytes."""

    def serialize(self, obj: Any) -> bytes:
        """Serialize *obj* to bytes."""
        ...

    def deserialize(self, data: bytes) -> Any:
        """Deserialize *data* back to a Python object."""
        ...


class JsonSerializer:
    """Serialize results as JSON (UTF-8).

    Works out of the box for dicts, lists, strings, numbers, bools, and None.
    Raises ``TypeError`` for non-JSON-serializable types.
    """

    def serialize(self, obj: Any) -> bytes:
        """Encode *obj* as a compact JSON byte string."""
        return json.dumps(obj, separators=(",", ":")).encode()

    def deserialize(self, data: bytes) -> Any:
        """Decode *data* from JSON bytes back to a Python object."""
        return json.loads(data)


class PickleSerializer:
    """Serialize results using Python's ``pickle`` protocol.

    Handles arbitrary Python objects but the data is opaque and
    version-sensitive.
    """

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        """Initialise with the given pickle *protocol* version."""
        self._protocol = protocol

    def serialize(self, obj: Any) -> bytes:
        """Pickle *obj* using the configured protocol."""
        return pickle.dumps(obj, protocol=self._protocol)

    def deserialize(self, data: bytes) -> Any:
        """Unpickle *data* and return the original Python object."""
        return pickle.loads(data)  # noqa: S301
