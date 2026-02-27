import asyncio
import concurrent.futures
from typing import Any

import pytest

from flowrun.context import RunContext
from flowrun.executor import TaskExecutor
from flowrun.task import TaskSpec


@pytest.mark.asyncio
async def test_task_executor_runs_sync_function_with_context():
    class Deps:
        value = 2

    ctx = RunContext(Deps())

    def sync_task(context) -> int:
        return context.value + 1

    spec = TaskSpec(
        name="sync",
        func=sync_task,
        timeout_s=1.0,
        accepts_context=True,
        requires_context=True,
    )

    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        executor = TaskExecutor(executor=thread_pool)
        result = await executor.run_once(spec, spec.timeout_s, ctx)
    finally:
        thread_pool.shutdown(wait=True)

    assert result.ok is True
    assert result.result == 3


@pytest.mark.asyncio
async def test_task_executor_reports_missing_context():
    def sync_task(context) -> int:
        return 1

    spec = TaskSpec(
        name="needs_ctx",
        func=sync_task,
        timeout_s=1.0,
        accepts_context=True,
        requires_context=True,
    )

    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        executor = TaskExecutor(executor=thread_pool)
        result = await executor.run_once(spec, spec.timeout_s, None)
    finally:
        thread_pool.shutdown(wait=True)

    assert result.ok is False
    assert "requires a RunContext" in (result.error or "")


@pytest.mark.asyncio
async def test_task_executor_times_out_async_task():
    async def slow_task() -> None:
        await asyncio.sleep(0.05)

    spec = TaskSpec(name="slow", func=slow_task, timeout_s=0.01)

    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        executor = TaskExecutor(executor=thread_pool)
        result = await executor.run_once(spec, spec.timeout_s, None)
    finally:
        thread_pool.shutdown(wait=True)

    assert result.ok is False
    assert result.error == "Timeout after 0.01s"


@pytest.mark.asyncio
async def test_task_executor_passes_upstream_results_to_sync_task():
    captured: dict[str, Any] = {}

    def child(upstream):
        captured.update(upstream)
        return upstream["root"] + 1

    spec = TaskSpec(
        name="child",
        func=child,
        timeout_s=1.0,
        accepts_upstream=True,
    )

    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        executor = TaskExecutor(executor=thread_pool)
        result = await executor.run_once(spec, spec.timeout_s, None, {"root": 2})
    finally:
        thread_pool.shutdown(wait=True)

    assert captured == {"root": 2}
    assert result.ok is True
    assert result.result == 3


@pytest.mark.asyncio
async def test_task_executor_injects_named_dependency_results_when_declared():
    def child(root: int) -> int:
        return root + 1

    spec = TaskSpec(
        name="child",
        func=child,
        timeout_s=1.0,
        named_deps=["root"],
    )

    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        executor = TaskExecutor(executor=thread_pool)
        result = await executor.run_once(spec, spec.timeout_s, None, {"root": 2})
    finally:
        thread_pool.shutdown(wait=True)

    assert result.ok is True
    assert result.result == 3
