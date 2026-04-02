import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any


class RunCancelledError(RuntimeError):
    """Raised when a task checks a cancelled RunContext."""


@dataclass(frozen=True, slots=True)
class RunContext[DepsT]:
    """Container that exposes user-defined dependencies to task functions.

    The user is responsible for instantiating this context with a strongly-typed
    dependency bundle; tasks can then declare ``RunContext[MyDeps]`` to gain IDE
    and type-checker support while still accessing attributes directly.
    """

    deps: DepsT
    metadata: Mapping[str, Any] = field(default_factory=dict)
    _deadline_monotonic_s: float | None = field(default=None, repr=False, compare=False)
    _cancel_event: threading.Event | None = field(default=None, repr=False, compare=False)

    def with_metadata(self, metadata: Mapping[str, Any] | None = None, /, **entries: Any) -> "RunContext[DepsT]":
        """Return a copy with merged run metadata for reporting and tracing.

        This is useful for ETL-style identifiers such as ``batch_id``,
        ``window_start``, ``source``, or ``partition``.
        """
        merged = dict(self.metadata)
        if metadata is not None:
            merged.update(metadata)
        if entries:
            merged.update(entries)
        if merged == dict(self.metadata):
            return self
        return replace(self, metadata=merged)

    def with_deadline_s(self, timeout_s: float | None) -> "RunContext[DepsT]":
        """Return a copy with a deadline derived from now + *timeout_s*.

        When a deadline already exists, the earliest deadline wins.
        """
        if timeout_s is None:
            return self
        deadline_monotonic_s = time.monotonic() + max(timeout_s, 0.0)
        return self._with_deadline_monotonic(deadline_monotonic_s)

    def with_cancel_event(self, cancel_event: threading.Event | None) -> "RunContext[DepsT]":
        """Return a copy that checks *cancel_event* for cooperative cancellation."""
        if cancel_event is self._cancel_event:
            return self
        return replace(self, _cancel_event=cancel_event)

    def has_deadline(self) -> bool:
        """Return True when this context carries an active deadline."""
        return self._deadline_monotonic_s is not None

    def time_remaining_s(self) -> float | None:
        """Return seconds remaining until the deadline, or None when unset."""
        if self._deadline_monotonic_s is None:
            return None
        return max(0.0, self._deadline_monotonic_s - time.monotonic())

    def deadline_exceeded(self) -> bool:
        """Return True when the context deadline has elapsed."""
        remaining_s = self.time_remaining_s()
        return remaining_s is not None and remaining_s <= 0.0

    def cancelled(self) -> bool:
        """Return True when the deadline elapsed or the cancel event was set."""
        return self.deadline_exceeded() or (self._cancel_event.is_set() if self._cancel_event is not None else False)

    def raise_if_cancelled(self) -> None:
        """Raise when the context deadline elapsed or cancellation was requested."""
        if self.deadline_exceeded():
            raise TimeoutError("RunContext deadline exceeded.")
        if self._cancel_event is not None and self._cancel_event.is_set():
            raise RunCancelledError("RunContext was cancelled.")

    def __getattr__(self, item: str) -> Any:
        """Delegate attribute access to the wrapped dependency bundle.

        Allows attribute lookup on the RunContext to fall back to the underlying
        deps object (e.g., ctx.session_factory -> deps.session_factory).
        """
        # Delegate attribute access to the wrapped dependency bundle. This keeps
        # task code ergonomic: ctx.session_factory -> deps.session_factory.
        try:
            return getattr(self.deps, item)
        except AttributeError as exc:
            if isinstance(self.deps, Mapping) and item in self.deps:
                return self.deps[item]
            raise AttributeError(item) from exc

    def _with_deadline_monotonic(self, deadline_monotonic_s: float | None) -> "RunContext[DepsT]":
        """Return a copy using the earlier of the current and provided deadlines."""
        merged_deadline = deadline_monotonic_s
        if self._deadline_monotonic_s is not None and merged_deadline is not None:
            merged_deadline = min(self._deadline_monotonic_s, merged_deadline)
        elif self._deadline_monotonic_s is not None:
            merged_deadline = self._deadline_monotonic_s

        if merged_deadline == self._deadline_monotonic_s:
            return self

        return replace(self, _deadline_monotonic_s=merged_deadline)
