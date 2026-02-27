"""Tests for the new features: retries, retain_result, engine context manager."""

from typing import cast

import pytest

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
# retain_result tests
# ---------------------------------------------------------------------------


class TrackingExecutor:
    """Executor that returns canned results and records calls."""

    def __init__(self, results: dict[str, object]) -> None:
        self._results = results
        self.calls: list[str] = []

    async def run_once(self, spec, timeout_s, context, upstream_results):
        self.calls.append(spec.name)
        return ExecutionResult(ok=True, result=self._results.get(spec.name), duration_s=0.01)


@pytest.mark.asyncio
async def test_retain_result_false_clears_after_consumers_launch():
    """A task with retain_result=False should have its result cleared after dependents are done."""
    registry = TaskRegistry()
    registry.register(TaskSpec(name="big_df", func=lambda: None, retain_result=False))
    registry.register(TaskSpec(name="consumer", func=lambda: None, deps=["big_df"]))

    state_store = StateStore()
    executor = TrackingExecutor({"big_df": "large-payload", "consumer": "done"})
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
    )
    dag = DAG(
        name="mem_dag",
        nodes=["big_df", "consumer"],
        edges={"big_df": [], "consumer": ["big_df"]},
    )

    run_id = await scheduler.run_dag_once(dag)
    rec = state_store.get_run(run_id)

    assert rec.tasks["big_df"].status == "SUCCESS"
    assert rec.tasks["big_df"].result is None  # cleared
    assert rec.tasks["consumer"].result == "done"


@pytest.mark.asyncio
async def test_retain_result_true_keeps_result():
    """The default retain_result=True should keep the result in state."""
    registry = TaskRegistry()
    registry.register(TaskSpec(name="keep_me", func=lambda: None, retain_result=True))

    state_store = StateStore()
    executor = TrackingExecutor({"keep_me": "important"})
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
    )
    dag = DAG(name="keep_dag", nodes=["keep_me"], edges={"keep_me": []})

    run_id = await scheduler.run_dag_once(dag)
    rec = state_store.get_run(run_id)

    assert rec.tasks["keep_me"].result == "important"


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
async def test_engine_dag_scope_supports_templates_and_subgraph():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    etl = engine.dag("templated")

    def fetch(*, table: str) -> str:
        return table

    tpl = etl.task_template(fetch)
    tpl.bind("fetch_users", table="users")
    tpl.bind("fetch_orders", table="orders")

    @etl.task(name="combine", deps=["fetch_users", "fetch_orders"])
    def combine(fetch_users: str, fetch_orders: str) -> str:
        return f"{fetch_users}+{fetch_orders}"

    etl.validate()

    async with engine:
        run_id = await etl.run_subgraph(["combine"])
        report = engine.get_run_report(run_id)

    assert report["status"] == "SUCCESS"
    assert set(report["tasks"].keys()) == {"fetch_users", "fetch_orders", "combine"}


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


def test_state_clear_result(state_store):
    """clear_result should set the result field to None."""
    state_store.create_run("r1", "dag", ["t1"])
    state_store.mark_running("r1", "t1")
    state_store.mark_success("r1", "t1", result="big-data")
    state_store.clear_result("r1", "t1")

    rec = state_store.get_run("r1")
    assert rec.tasks["t1"].status == "SUCCESS"
    assert rec.tasks["t1"].result is None
