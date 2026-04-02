import threading
import time
from dataclasses import dataclass

import pytest

from flowrun.context import RunCancelledError, RunContext


def test_run_context_delegates_attribute_access():
    @dataclass
    class FakeDeps:
        value: int = 42

        def double(self) -> int:
            return self.value * 2

    ctx = RunContext(FakeDeps())

    assert ctx.deps.value == 42
    assert ctx.value == 42
    assert ctx.double() == 84


def test_run_context_missing_attribute_raises():
    ctx = RunContext(deps={"existing": 1})

    assert getattr(ctx, "existing") == 1
    try:
        _ = ctx.missing
    except AttributeError:
        return
    raise AssertionError("AttributeError not raised for unknown attribute")


def test_run_context_deadline_helpers():
    ctx = RunContext(deps={"existing": 1}).with_deadline_s(0.02)

    assert ctx.has_deadline() is True
    remaining_s = ctx.time_remaining_s()
    assert remaining_s is not None
    assert 0.0 < remaining_s <= 0.02

    time.sleep(0.03)

    assert ctx.deadline_exceeded() is True
    assert ctx.cancelled() is True
    with pytest.raises(TimeoutError, match="deadline exceeded"):
        ctx.raise_if_cancelled()


def test_run_context_cancel_event_helpers():
    cancel_event = threading.Event()
    ctx = RunContext(deps={"existing": 1}).with_cancel_event(cancel_event)

    assert ctx.cancelled() is False

    cancel_event.set()

    assert ctx.cancelled() is True
    with pytest.raises(RunCancelledError, match="cancelled"):
        ctx.raise_if_cancelled()


def test_run_context_keeps_earliest_deadline():
    ctx = RunContext(deps={"existing": 1}).with_deadline_s(0.02)
    widened = ctx.with_deadline_s(0.2)

    assert widened is ctx
