"""Lightweight hook / callback system for flowrun.

Users implement one or more methods of ``RunHook`` (or use the convenience
``fn_hook`` factory for simple one-off callbacks) and pass them to the engine
or scheduler.  Hooks are invoked synchronously from the scheduler's event loop
so they should be fast — offload heavy work (Slack HTTP calls, metric pushes)
to a background task or thread inside your hook.

Example
-------
>>> from flowrun.hooks import RunHook
>>>
>>> class SlackHook(RunHook):
...     def on_task_failure(self, event):
...         requests.post(WEBHOOK, json={"text": f"Task {event.task_name} failed!"})
...
>>> engine = build_default_engine(hooks=[SlackHook()])
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Event payloads — thin, frozen dataclasses so hooks get structured data
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DagStartEvent:
    """Emitted when a DAG run begins."""

    run_id: str
    dag_name: str


@dataclass(frozen=True, slots=True)
class DagEndEvent:
    """Emitted when a DAG run finishes (all tasks terminal)."""

    run_id: str
    dag_name: str


@dataclass(frozen=True, slots=True)
class TaskStartEvent:
    """Emitted when a task transitions to RUNNING."""

    run_id: str
    dag_name: str
    task_name: str
    attempt: int


@dataclass(frozen=True, slots=True)
class TaskSuccessEvent:
    """Emitted when a task succeeds."""

    run_id: str
    dag_name: str
    task_name: str
    attempt: int
    duration_s: float
    result: Any


@dataclass(frozen=True, slots=True)
class TaskFailureEvent:
    """Emitted when a task fails (before retry decision)."""

    run_id: str
    dag_name: str
    task_name: str
    attempt: int
    duration_s: float
    error: str | None


@dataclass(frozen=True, slots=True)
class TaskRetryEvent:
    """Emitted when a failed task is scheduled for retry."""

    run_id: str
    dag_name: str
    task_name: str
    next_attempt: int
    max_attempts: int


@dataclass(frozen=True, slots=True)
class TaskSkipEvent:
    """Emitted when a task is skipped due to upstream failure."""

    run_id: str
    dag_name: str
    task_name: str
    reason: str


# ---------------------------------------------------------------------------
# Base hook class — override only the methods you care about
# ---------------------------------------------------------------------------


class RunHook:
    """Base class for lifecycle hooks.

    Override any subset of methods.  Unimplemented methods are silent no-ops.
    """

    def on_dag_start(self, event: DagStartEvent) -> None:
        """Called when a DAG run starts."""

    def on_dag_end(self, event: DagEndEvent) -> None:
        """Called when a DAG run finishes."""

    def on_task_start(self, event: TaskStartEvent) -> None:
        """Called when a task begins execution."""

    def on_task_success(self, event: TaskSuccessEvent) -> None:
        """Called when a task succeeds."""

    def on_task_failure(self, event: TaskFailureEvent) -> None:
        """Called when a task fails."""

    def on_task_retry(self, event: TaskRetryEvent) -> None:
        """Called when a task is scheduled for retry."""

    def on_task_skip(self, event: TaskSkipEvent) -> None:
        """Called when a task is skipped."""


# ---------------------------------------------------------------------------
# Convenience: function-based hooks
# ---------------------------------------------------------------------------


def fn_hook(
    *,
    on_dag_start: Any | None = None,
    on_dag_end: Any | None = None,
    on_task_start: Any | None = None,
    on_task_success: Any | None = None,
    on_task_failure: Any | None = None,
    on_task_retry: Any | None = None,
    on_task_skip: Any | None = None,
) -> RunHook:
    """Create a ``RunHook`` from plain functions — no subclassing required.

    Pass keyword-only callbacks for the events you care about; the rest remain
    no-ops.

    Example
    -------
    >>> hook = fn_hook(on_task_failure=lambda e: print(f"ALERT: {e.task_name}"))
    """
    hook = RunHook()
    if on_dag_start is not None:
        hook.on_dag_start = on_dag_start  # type: ignore[method-assign]
    if on_dag_end is not None:
        hook.on_dag_end = on_dag_end  # type: ignore[method-assign]
    if on_task_start is not None:
        hook.on_task_start = on_task_start  # type: ignore[method-assign]
    if on_task_success is not None:
        hook.on_task_success = on_task_success  # type: ignore[method-assign]
    if on_task_failure is not None:
        hook.on_task_failure = on_task_failure  # type: ignore[method-assign]
    if on_task_retry is not None:
        hook.on_task_retry = on_task_retry  # type: ignore[method-assign]
    if on_task_skip is not None:
        hook.on_task_skip = on_task_skip  # type: ignore[method-assign]
    return hook


# ---------------------------------------------------------------------------
# Internal dispatcher — used by the scheduler
# ---------------------------------------------------------------------------


class HookDispatcher:
    """Fan-out caller that invokes a list of hooks safely.

    Exceptions in individual hooks are caught and logged so a broken hook
    never crashes the scheduler.
    """

    def __init__(self, hooks: list[RunHook] | None = None, *, logger: Any | None = None) -> None:
        """Initialise the dispatcher with an optional list of hooks and logger."""
        self._hooks: list[RunHook] = list(hooks) if hooks else []
        self._log = logger or logging.getLogger("flowrun.hooks")

    @property
    def hooks(self) -> list[RunHook]:
        """Return the list of registered hooks."""
        return self._hooks

    def emit(self, method_name: str, event: object) -> None:
        """Call *method_name* on every registered hook, swallowing exceptions."""
        for hook in self._hooks:
            fn = getattr(hook, method_name, None)
            if fn is None:
                continue
            try:
                fn(event)
            except Exception:
                self._log.exception(
                    "Hook %r raised in %s",
                    type(hook).__name__,
                    method_name,
                )
