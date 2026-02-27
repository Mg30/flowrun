"""Tests for injectable logger support."""

import logging
from typing import cast
from unittest.mock import MagicMock

import pytest

from flowrun.dag import DAG
from flowrun.engine import build_default_engine
from flowrun.executor import ExecutionResult, TaskExecutor
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import StateStore
from flowrun.task import TaskRegistry, TaskSpec


class SimpleExecutor:
    """Executor that returns a canned result for every task."""

    def __init__(self, result: ExecutionResult) -> None:
        self._result = result

    async def run_once(self, spec, timeout_s, context, upstream_results):
        return self._result


@pytest.mark.asyncio
async def test_scheduler_logs_task_success():
    """Scheduler should log an INFO message when a task succeeds."""
    logger = logging.getLogger("test.scheduler.success")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    handler = logging.handlers.MemoryHandler(capacity=100) if hasattr(logging, "handlers") else logging.StreamHandler()
    # Use a simple list collector instead
    records: list[logging.LogRecord] = []

    class Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger.addHandler(Collector())

    registry = TaskRegistry()
    registry.register(TaskSpec(name="t1", func=lambda: None))

    state_store = StateStore()
    executor = SimpleExecutor(ExecutionResult(ok=True, result="ok", duration_s=0.01))
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
        logger=logger,
    )
    dag = DAG(name="log_dag", nodes=["t1"], edges={"t1": []})
    await scheduler.run_dag_once(dag)

    messages = [r.getMessage() for r in records]
    assert any("succeeded" in m and "t1" in m for m in messages)


@pytest.mark.asyncio
async def test_scheduler_logs_task_failure():
    """Scheduler should log a WARNING when a task fails."""
    records: list[logging.LogRecord] = []

    class Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("test.scheduler.failure")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(Collector())

    registry = TaskRegistry()
    registry.register(TaskSpec(name="fail_task", func=lambda: None))

    state_store = StateStore()
    executor = SimpleExecutor(ExecutionResult(ok=False, error="boom", duration_s=0.01))
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
        logger=logger,
    )
    dag = DAG(name="fail_dag", nodes=["fail_task"], edges={"fail_task": []})
    await scheduler.run_dag_once(dag)

    messages = [r.getMessage() for r in records]
    assert any("failed" in m and "fail_task" in m for m in messages)
    assert any(r.levelno == logging.WARNING for r in records)


@pytest.mark.asyncio
async def test_scheduler_logs_retry():
    """Scheduler should log an INFO when scheduling a retry."""
    records: list[logging.LogRecord] = []

    class Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("test.scheduler.retry")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(Collector())

    call_count = 0

    class RetryOnceExecutor:
        async def run_once(self, spec, timeout_s, context, upstream_results):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ExecutionResult(ok=False, error="transient", duration_s=0.01)
            return ExecutionResult(ok=True, result="recovered", duration_s=0.01)

    registry = TaskRegistry()
    registry.register(TaskSpec(name="flaky", func=lambda: None, retries=1))

    state_store = StateStore()
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, RetryOnceExecutor()),
        SchedulerConfig(max_parallel=2),
        logger=logger,
    )
    dag = DAG(name="retry_dag", nodes=["flaky"], edges={"flaky": []})
    await scheduler.run_dag_once(dag)

    messages = [r.getMessage() for r in records]
    assert any("retry" in m.lower() and "flaky" in m for m in messages)


@pytest.mark.asyncio
async def test_scheduler_logs_skip():
    """Scheduler should log when a task is skipped due to upstream failure."""
    records: list[logging.LogRecord] = []

    class Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("test.scheduler.skip")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(Collector())

    class FailExecutor:
        async def run_once(self, spec, timeout_s, context, upstream_results):
            return ExecutionResult(ok=False, error="nope", duration_s=0.01)

    registry = TaskRegistry()
    registry.register(TaskSpec(name="parent", func=lambda: None))
    registry.register(TaskSpec(name="child", func=lambda: None, deps=["parent"]))

    state_store = StateStore()
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, FailExecutor()),
        SchedulerConfig(max_parallel=2),
        logger=logger,
    )
    dag = DAG(name="skip_dag", nodes=["parent", "child"], edges={"parent": [], "child": ["parent"]})
    await scheduler.run_dag_once(dag)

    messages = [r.getMessage() for r in records]
    assert any("skip" in m.lower() and "child" in m for m in messages)


@pytest.mark.asyncio
async def test_engine_logs_dag_start_and_finish():
    """Engine should log DAG start and finish via the injected logger."""
    records: list[logging.LogRecord] = []

    class Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("test.engine.dag")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(Collector())

    engine = build_default_engine(max_workers=1, max_parallel=1, logger=logger)

    # Register a trivial task so the DAG is valid
    registry = engine.registry
    registry.register(TaskSpec(name="noop", func=lambda: None))

    async with engine:
        await engine.run_once("test_dag")

    messages = [r.getMessage() for r in records]
    assert any("Starting DAG" in m for m in messages)
    assert any("Finished DAG" in m for m in messages)


@pytest.mark.asyncio
async def test_build_default_engine_propagates_logger():
    """build_default_engine should propagate the logger to scheduler and executor."""
    logger = logging.getLogger("test.propagate")
    engine = build_default_engine(logger=logger)
    async with engine:
        assert engine._log is logger
        assert engine.scheduler._log is logger
        assert engine.task_executor._log is logger
