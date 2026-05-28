"""Tests for the new features: retries and engine context manager."""

import asyncio
from typing import cast

import pytest

from flowrun.context import RunContext
from flowrun.dag import DAG
from flowrun.engine import Engine, build_default_engine
from flowrun.executor import ExecutionResult, TaskExecutor
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import StateStore
from flowrun.task import TaskRegistry, TaskSpec


# ---------------------------------------------------------------------------
# Retry tests
# ---------------------------------------------------------------------------


class RetryExecutor:
    """Executor that fails the first N calls for a task, then succeeds."""

    def __init__(self, fail_until: dict[str, int]) -> None:
        self._fail_until = fail_until  # task_name -> succeed on attempt N
        self._call_counts: dict[str, int] = {}

    async def run_once(self, spec, timeout_s, context, upstream_results):
        count = self._call_counts.get(spec.name, 0) + 1
        self._call_counts[spec.name] = count
        threshold = self._fail_until.get(spec.name, 1)
        if count < threshold:
            return ExecutionResult(ok=False, error=f"fail #{count}", duration_s=0.01)
        return ExecutionResult(ok=True, result=f"{spec.name}-ok", duration_s=0.01)


@pytest.mark.asyncio
async def test_scheduler_retries_task_on_failure():
    """A task with retries=2 should be re-attempted after the first failure."""
    registry = TaskRegistry()
    registry.register(TaskSpec(name="flaky", func=lambda: None, retries=2))

    state_store = StateStore()
    executor = RetryExecutor(fail_until={"flaky": 2})  # succeeds on 2nd try
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
    )
    dag = DAG(name="retry_dag", nodes=["flaky"], edges={"flaky": []})

    run_id = await scheduler.run_dag_once(dag)
    rec = state_store.get_run(run_id)

    assert rec.tasks["flaky"].status == "SUCCESS"
    assert rec.tasks["flaky"].attempt == 2
    assert rec.finished_at is not None


@pytest.mark.asyncio
async def test_scheduler_exhausts_retries_then_fails():
    """When all retries are exhausted the task should end up FAILED."""
    registry = TaskRegistry()
    registry.register(TaskSpec(name="doomed", func=lambda: None, retries=1))

    state_store = StateStore()
    executor = RetryExecutor(fail_until={"doomed": 999})  # never succeeds
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
    )
    dag = DAG(name="retry_dag", nodes=["doomed"], edges={"doomed": []})

    run_id = await scheduler.run_dag_once(dag)
    rec = state_store.get_run(run_id)

    assert rec.tasks["doomed"].status == "FAILED"
    assert rec.tasks["doomed"].attempt == 2  # 1 initial + 1 retry
    assert rec.finished_at is not None


@pytest.mark.asyncio
async def test_scheduler_retries_do_not_skip_children_prematurely():
    """Children should only be skipped once retries are truly exhausted."""
    registry = TaskRegistry()
    registry.register(TaskSpec(name="parent", func=lambda: None, retries=1))
    registry.register(TaskSpec(name="child", func=lambda: None, deps=["parent"]))

    state_store = StateStore()
    executor = RetryExecutor(fail_until={"parent": 2, "child": 1})  # parent succeeds on retry
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
    )
    dag = DAG(
        name="retry_dag",
        nodes=["parent", "child"],
        edges={"parent": [], "child": ["parent"]},
    )

    run_id = await scheduler.run_dag_once(dag)
    rec = state_store.get_run(run_id)

    assert rec.tasks["parent"].status == "SUCCESS"
    assert rec.tasks["child"].status == "SUCCESS"


# ---------------------------------------------------------------------------
# Engine context manager tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_context_manager_shuts_down_pool():
    """Engine used as async context manager should shut down its owned pool."""
    engine = build_default_engine(max_workers=2, max_parallel=2)
    async with engine:
        assert not engine._closed
    assert engine._closed


@pytest.mark.asyncio
async def test_engine_close_is_idempotent():
    """Calling close() multiple times should not raise."""
    engine = build_default_engine(max_workers=1)
    engine.close()
    engine.close()  # should not raise
    assert engine._closed


@pytest.mark.asyncio
async def test_engine_task_decorator_scopes_tasks_to_dag_name():
    """run_once(dag_name) should execute only tasks in that DAG namespace."""
    engine = build_default_engine(max_workers=2, max_parallel=2)
    seen: list[str] = []

    @engine.task(name="a_1", dag="etl_a")
    def a_1() -> str:
        seen.append("a_1")
        return "ok"

    @engine.task(name="a_2", deps=[a_1], dag="etl_a")
    def a_2(a_1: str) -> str:
        seen.append("a_2")
        return a_1

    @engine.task(name="b_1", dag="etl_b")
    def b_1() -> str:
        seen.append("b_1")
        return "ok"

    async with engine:
        run_id = await engine.run_once("etl_a")
        report = engine.get_run_report(run_id)

    assert set(report["tasks"].keys()) == {"a_1", "a_2"}
    assert seen == ["a_1", "a_2"]


@pytest.mark.asyncio
async def test_engine_unknown_dag_raises():
    engine = build_default_engine(max_workers=2, max_parallel=2)

    @engine.task(name="a_1", dag="etl_a")
    def a_1() -> str:
        return "ok"

    async with engine:
        with pytest.raises(ValueError, match="not registered"):
            await engine.run_once("etl_x")


@pytest.mark.asyncio
async def test_engine_empty_unscoped_dag_raises():
    engine = build_default_engine(max_workers=1, max_parallel=1)

    async with engine:
        with pytest.raises(ValueError, match="has no registered tasks"):
            await engine.run_once("missing")


@pytest.mark.asyncio
async def test_engine_validate_and_list_helpers():
    engine = build_default_engine(max_workers=2, max_parallel=2)

    @engine.task(name="a_1", dag="etl_a")
    def a_1() -> str:
        return "ok"

    @engine.task(name="a_2", deps=[a_1], dag="etl_a")
    def a_2(a_1: str) -> str:
        return a_1

    @engine.task(name="b_1", dag="etl_b")
    def b_1() -> str:
        return "ok"

    assert engine.list_dags() == ["etl_a", "etl_b"]
    engine.validate("etl_a")
    assert engine.list_tasks("etl_a") == ["a_1", "a_2"]


@pytest.mark.asyncio
async def test_engine_get_run_report_has_run_level_status():
    engine = build_default_engine(max_workers=2, max_parallel=2)

    @engine.task(name="ok_1", dag="etl")
    def ok_1() -> str:
        return "ok"

    async with engine:
        run_id = await engine.run_once("etl")
        report = engine.get_run_report(run_id)

    assert report["status"] == "SUCCESS"


@pytest.mark.asyncio
async def test_engine_get_run_report_failed_run_level_status():
    engine = build_default_engine(max_workers=2, max_parallel=2)

    @engine.task(name="boom", dag="etl")
    def boom() -> str:
        raise RuntimeError("boom")

    async with engine:
        run_id = await engine.run_once("etl")
        report = engine.get_run_report(run_id)

    assert report["status"] == "FAILED"


@pytest.mark.asyncio
async def test_engine_dag_scope_registers_and_runs_without_repeating_dag():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    etl = engine.dag("daily_etl")
    seen: list[str] = []

    @etl.task(name="extract")
    def extract() -> str:
        seen.append("extract")
        return "raw"

    @etl.task(name="transform", deps=[extract])
    def transform(extract: str) -> str:
        seen.append("transform")
        return extract.upper()

    async with engine:
        run_id = await etl.run_once()
        report = engine.get_run_report(run_id)

    assert report["status"] == "SUCCESS"
    assert set(report["tasks"].keys()) == {"extract", "transform"}
    assert seen == ["extract", "transform"]
    assert etl.list_tasks() == ["extract", "transform"]


@pytest.mark.asyncio
async def test_engine_allows_same_task_names_in_different_dags():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    etl_a = engine.dag("etl_a")
    etl_b = engine.dag("etl_b")

    @etl_a.task(name="extract")
    def extract_a() -> str:
        return "a"

    @etl_b.task(name="extract")
    def extract_b() -> str:
        return "b"

    async with engine:
        run_a = await etl_a.run_once()
        run_b = await etl_b.run_once()
        report_a = engine.get_run_report(run_a)
        report_b = engine.get_run_report(run_b)

    assert report_a["tasks"]["extract"]["result"] == "a"
    assert report_b["tasks"]["extract"]["result"] == "b"


@pytest.mark.asyncio
async def test_engine_injects_context_with_dependency_results():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    etl = engine.dag("ctx_dep")

    @etl.task(name="extract")
    def extract() -> str:
        return "ok"

    @etl.task(name="consume", deps=[extract])
    def consume(extract: str, context: RunContext[dict[str, str]]) -> str:
        return extract + context.suffix

    async with engine:
        run_id = await etl.run_once(RunContext({"suffix": "!"}))
        report = engine.get_run_report(run_id)

    assert report["status"] == "SUCCESS"
    assert report["tasks"]["consume"]["result"] == "ok!"


@pytest.mark.asyncio
async def test_engine_dag_scope_supports_factory_registered_tasks_and_subgraph():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    etl = engine.dag("templated")

    def bind_fetch(*, name: str, table: str):
        @etl.task(name=name)
        def fetch() -> str:
            return table

        return fetch

    fetch_users = bind_fetch(name="fetch_users", table="users")
    fetch_orders = bind_fetch(name="fetch_orders", table="orders")

    @etl.task(name="combine", deps=[fetch_users, fetch_orders])
    def combine(fetch_users: str, fetch_orders: str) -> str:
        return f"{fetch_users}+{fetch_orders}"

    etl.validate()

    async with engine:
        run_id = await etl.run_subgraph(["combine"])
        report = engine.get_run_report(run_id)

    assert report["status"] == "SUCCESS"
    assert set(report["tasks"].keys()) == {"fetch_users", "fetch_orders", "combine"}


@pytest.mark.asyncio
async def test_run_context_metadata_is_reported():
    engine = build_default_engine(max_workers=2, max_parallel=2)

    @engine.task(dag="metadata_demo")
    def extract(context: RunContext[dict[str, int]]) -> int:
        return context.value

    context = RunContext({"value": 3}).with_metadata(batch_id=7, source="api_users")

    async with engine:
        run_id = await engine.run_once("metadata_demo", context=context)
        report = engine.get_run_report(run_id)

    assert report["metadata"] == {"batch_id": 7, "source": "api_users"}


@pytest.mark.asyncio
async def test_run_many_reports_context_metadata_per_run():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    seen: list[tuple[str, int]] = []

    @engine.task(name="input_chunk", dag="micro_batch")
    def input_chunk(context: RunContext[dict[str, int]]) -> dict[str, int]:
        seen.append(("input", context.batch_id))
        return {"batch_id": context.batch_id, "value": context.value}

    @engine.task(name="double", dag="micro_batch", deps=[input_chunk])
    def double(input_chunk: dict[str, int]) -> int:
        seen.append(("double", input_chunk["batch_id"]))
        return input_chunk["value"] * 2

    contexts = [
        RunContext({"batch_id": 1, "value": 3}).with_metadata(batch_id=1, source="users"),
        RunContext({"batch_id": 2, "value": 5}).with_metadata(batch_id=2, source="users"),
    ]

    async with engine:
        run_ids = await engine.run_many("micro_batch", contexts)
        reports = [engine.get_run_report(run_id) for run_id in run_ids]

    assert len(run_ids) == 2
    assert [report["metadata"]["batch_id"] for report in reports] == [1, 2]
    assert [report["tasks"]["double"]["result"] for report in reports] == [6, 10]
    assert seen == [("input", 1), ("double", 1), ("input", 2), ("double", 2)]


@pytest.mark.asyncio
async def test_engine_run_many_supports_iterable_contexts_sequentially():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    seen: list[tuple[str, int]] = []

    @engine.task(name="input_chunk", dag="micro_batch")
    def input_chunk(context: RunContext[dict[str, int]]) -> dict[str, int]:
        seen.append(("input", context.batch_id))
        return {"batch_id": context.batch_id, "value": context.value}

    @engine.task(name="double", dag="micro_batch", deps=[input_chunk])
    def double(input_chunk: dict[str, int]) -> int:
        seen.append(("double", input_chunk["batch_id"]))
        return input_chunk["value"] * 2

    contexts = [
        RunContext({"batch_id": 1, "value": 3}),
        RunContext({"batch_id": 2, "value": 5}),
    ]

    async with engine:
        run_ids = await engine.run_many("micro_batch", contexts)
        reports = [engine.get_run_report(run_id) for run_id in run_ids]

    assert len(run_ids) == 2
    assert [report["tasks"]["double"]["result"] for report in reports] == [6, 10]
    assert seen == [("input", 1), ("double", 1), ("input", 2), ("double", 2)]


@pytest.mark.asyncio
async def test_engine_dag_scope_run_many_supports_async_iterables():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    etl = engine.dag("micro_batch_scope")
    seen: list[tuple[str, int]] = []

    @etl.task(name="input_chunk")
    def input_chunk(context: RunContext[dict[str, int]]) -> dict[str, int]:
        seen.append(("input", context.batch_id))
        return {"batch_id": context.batch_id, "value": context.value}

    @etl.task(name="double", deps=[input_chunk])
    def double(input_chunk: dict[str, int]) -> int:
        seen.append(("double", input_chunk["batch_id"]))
        return input_chunk["value"] * 2

    async def contexts():
        for batch_id, value in [(1, 3), (2, 5), (3, 7)]:
            await asyncio.sleep(0)
            yield RunContext({"batch_id": batch_id, "value": value})

    async with engine:
        run_ids = await etl.run_many(contexts())
        reports = [engine.get_run_report(run_id) for run_id in run_ids]

    assert len(run_ids) == 3
    assert [report["tasks"]["double"]["result"] for report in reports] == [6, 10, 14]
    assert seen == [
        ("input", 1),
        ("double", 1),
        ("input", 2),
        ("double", 2),
        ("input", 3),
        ("double", 3),
    ]


# ---------------------------------------------------------------------------
# State machine: retry transition
# ---------------------------------------------------------------------------


def test_state_mark_retry_resets_to_pending(state_store):
    """mark_retry should transition FAILED -> PENDING and clear error."""
    state_store.create_run("r1", "dag", ["t1"])
    state_store.mark_running("r1", "t1")
    state_store.mark_failed("r1", "t1", err="boom")
    state_store.mark_retry("r1", "t1")

    rec = state_store.get_run("r1")
    assert rec.tasks["t1"].status == "PENDING"
    assert rec.tasks["t1"].error is None
