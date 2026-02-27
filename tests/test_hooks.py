"""Tests for the hook / callback system."""

from typing import cast

import pytest

from flowrun.dag import DAG
from flowrun.executor import ExecutionResult, TaskExecutor
from flowrun.hooks import (
    DagEndEvent,
    DagStartEvent,
    HookDispatcher,
    RunHook,
    TaskFailureEvent,
    TaskRetryEvent,
    TaskSkipEvent,
    TaskStartEvent,
    TaskSuccessEvent,
    fn_hook,
)
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import StateStore
from flowrun.task import TaskRegistry, TaskSpec


# ---------------------------------------------------------------------------
# Unit tests for HookDispatcher
# ---------------------------------------------------------------------------


class RecordingHook(RunHook):
    """Hook that records every event it receives."""

    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def on_dag_start(self, event: DagStartEvent) -> None:
        self.events.append(("on_dag_start", event))

    def on_dag_end(self, event: DagEndEvent) -> None:
        self.events.append(("on_dag_end", event))

    def on_task_start(self, event: TaskStartEvent) -> None:
        self.events.append(("on_task_start", event))

    def on_task_success(self, event: TaskSuccessEvent) -> None:
        self.events.append(("on_task_success", event))

    def on_task_failure(self, event: TaskFailureEvent) -> None:
        self.events.append(("on_task_failure", event))

    def on_task_retry(self, event: TaskRetryEvent) -> None:
        self.events.append(("on_task_retry", event))

    def on_task_skip(self, event: TaskSkipEvent) -> None:
        self.events.append(("on_task_skip", event))


def test_dispatcher_fans_out_to_multiple_hooks():
    h1 = RecordingHook()
    h2 = RecordingHook()
    dispatcher = HookDispatcher([h1, h2])

    event = DagStartEvent(run_id="r1", dag_name="test")
    dispatcher.emit("on_dag_start", event)

    assert len(h1.events) == 1
    assert len(h2.events) == 1
    assert h1.events[0] == ("on_dag_start", event)


def test_dispatcher_swallows_hook_exceptions():
    """A broken hook should not prevent other hooks from running."""

    class BrokenHook(RunHook):
        def on_dag_start(self, event: DagStartEvent) -> None:
            raise RuntimeError("kaboom")

    good = RecordingHook()
    dispatcher = HookDispatcher([BrokenHook(), good])

    event = DagStartEvent(run_id="r1", dag_name="test")
    dispatcher.emit("on_dag_start", event)  # should not raise

    assert len(good.events) == 1


def test_dispatcher_ignores_missing_method():
    """Dispatcher should silently skip hooks that don't implement a method."""
    dispatcher = HookDispatcher([RunHook()])  # base class = all no-ops
    event = DagStartEvent(run_id="r1", dag_name="test")
    dispatcher.emit("on_dag_start", event)  # should not raise


# ---------------------------------------------------------------------------
# fn_hook convenience factory
# ---------------------------------------------------------------------------


def test_fn_hook_wires_callbacks():
    captured: list[object] = []
    hook = fn_hook(on_task_failure=lambda e: captured.append(e))

    event = TaskFailureEvent(
        run_id="r1",
        dag_name="d",
        task_name="t",
        attempt=1,
        duration_s=0.1,
        error="boom",
    )
    hook.on_task_failure(event)

    assert len(captured) == 1
    assert captured[0] is event


def test_fn_hook_leaves_unset_methods_as_noop():
    hook = fn_hook(on_task_success=lambda e: None)
    # calling an unset method should be a no-op, not raise
    hook.on_dag_start(DagStartEvent(run_id="r1", dag_name="d"))


# ---------------------------------------------------------------------------
# Integration: hooks fire during scheduler execution
# ---------------------------------------------------------------------------


class CannedExecutor:
    """Executor returning a fixed result per task name."""

    def __init__(self, outcomes: dict[str, ExecutionResult]) -> None:
        self._outcomes = outcomes

    async def run_once(self, spec, timeout_s, context, upstream_results):
        return self._outcomes[spec.name]


@pytest.mark.asyncio
async def test_hooks_fire_on_success_run():
    hook = RecordingHook()
    registry = TaskRegistry()
    registry.register(TaskSpec(name="a", func=lambda: None))
    registry.register(TaskSpec(name="b", func=lambda: None, deps=["a"]))

    executor = CannedExecutor(
        {
            "a": ExecutionResult(ok=True, result="a-res", duration_s=0.01),
            "b": ExecutionResult(ok=True, result="b-res", duration_s=0.02),
        }
    )
    scheduler = Scheduler(
        registry,
        StateStore(),
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
        hooks=[hook],
    )
    dag = DAG(name="hook_dag", nodes=["a", "b"], edges={"a": [], "b": ["a"]})
    await scheduler.run_dag_once(dag)

    event_names = [name for name, _ev in hook.events]
    assert event_names[0] == "on_dag_start"
    assert event_names[-1] == "on_dag_end"
    assert event_names.count("on_task_start") == 2
    assert event_names.count("on_task_success") == 2
    assert "on_task_failure" not in event_names


@pytest.mark.asyncio
async def test_hooks_fire_on_failure_and_skip():
    hook = RecordingHook()
    registry = TaskRegistry()
    registry.register(TaskSpec(name="parent", func=lambda: None))
    registry.register(TaskSpec(name="child", func=lambda: None, deps=["parent"]))

    executor = CannedExecutor(
        {
            "parent": ExecutionResult(ok=False, error="boom", duration_s=0.01),
        }
    )
    scheduler = Scheduler(
        registry,
        StateStore(),
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
        hooks=[hook],
    )
    dag = DAG(
        name="fail_dag",
        nodes=["parent", "child"],
        edges={"parent": [], "child": ["parent"]},
    )
    await scheduler.run_dag_once(dag)

    event_names = [name for name, _ev in hook.events]
    assert "on_task_failure" in event_names
    assert "on_task_skip" in event_names

    # Verify the failure event payload
    fail_events = [ev for name, ev in hook.events if name == "on_task_failure"]
    assert fail_events[0].task_name == "parent"
    assert fail_events[0].error == "boom"

    # Verify skip event payload
    skip_events = [ev for name, ev in hook.events if name == "on_task_skip"]
    assert skip_events[0].task_name == "child"
    assert skip_events[0].reason == "UPSTREAM_FAILED"


@pytest.mark.asyncio
async def test_hooks_fire_on_retry():
    hook = RecordingHook()
    registry = TaskRegistry()
    registry.register(TaskSpec(name="flaky", func=lambda: None, retries=1))

    call_count = 0

    class RetryOnceExecutor:
        async def run_once(self, spec, timeout_s, context, upstream_results):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ExecutionResult(ok=False, error="transient", duration_s=0.01)
            return ExecutionResult(ok=True, result="recovered", duration_s=0.01)

    scheduler = Scheduler(
        registry,
        StateStore(),
        cast(TaskExecutor, RetryOnceExecutor()),
        SchedulerConfig(max_parallel=2),
        hooks=[hook],
    )
    dag = DAG(name="retry_dag", nodes=["flaky"], edges={"flaky": []})
    await scheduler.run_dag_once(dag)

    event_names = [name for name, _ev in hook.events]
    assert "on_task_retry" in event_names
    assert "on_task_success" in event_names

    retry_ev = [ev for name, ev in hook.events if name == "on_task_retry"][0]
    assert retry_ev.task_name == "flaky"
    assert retry_ev.next_attempt == 2


@pytest.mark.asyncio
async def test_hooks_via_build_default_engine():
    """Hooks passed to build_default_engine should reach the scheduler."""
    from flowrun.engine import build_default_engine

    hook = RecordingHook()
    engine = build_default_engine(hooks=[hook])
    engine.registry.register(TaskSpec(name="t1", func=lambda: None))

    async with engine:
        await engine.run_once("via_hooks")

    event_names = [name for name, _ev in hook.events]
    assert "on_dag_start" in event_names
    assert "on_task_success" in event_names
    assert "on_dag_end" in event_names
