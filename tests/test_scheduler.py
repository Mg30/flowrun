from typing import cast

import pytest

from flowrun.dag import DAG
from flowrun.executor import ExecutionResult, TaskExecutor
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import StateStore
from flowrun.task import TaskRegistry, TaskSpec


class DummyExecutor:
    def __init__(self, outcomes: dict[str, ExecutionResult]) -> None:
        self._outcomes = outcomes
        self.calls: list[str] = []
        self.upstream_seen: list[dict[str, object]] = []

    async def run_once(
        self,
        spec: TaskSpec,
        timeout_s: float | None,
        context,
        upstream_results: dict[str, object] | None,
    ) -> ExecutionResult:
        self.calls.append(spec.name)
        self.upstream_seen.append(upstream_results or {})
        if spec.name not in self._outcomes:
            raise AssertionError(f"Unexpected task execution for {spec.name}")
        return self._outcomes[spec.name]


@pytest.mark.asyncio
async def test_scheduler_runs_tasks_and_records_success():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="root", func=lambda: None))
    registry.register(TaskSpec(name="child", func=lambda: None, deps=["root"]))

    state_store = StateStore()
    executor = DummyExecutor(
        {
            "root": ExecutionResult(ok=True, result="root-result", duration_s=0.01),
            "child": ExecutionResult(ok=True, result="child-result", duration_s=0.01),
        }
    )
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
    )
    dag = DAG(name="demo", nodes=["root", "child"], edges={"root": [], "child": ["root"]})

    run_id = await scheduler.run_dag_once(dag)

    record = state_store.get_run(run_id)
    assert executor.calls == ["root", "child"]
    assert executor.upstream_seen == [{}, {"root": "root-result"}]
    assert record.tasks["root"].status == "SUCCESS"
    assert record.tasks["child"].status == "SUCCESS"
    assert record.tasks["child"].result == "child-result"
    assert record.finished_at is not None


@pytest.mark.asyncio
async def test_scheduler_skips_children_after_failure():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="root", func=lambda: None))
    registry.register(TaskSpec(name="child", func=lambda: None, deps=["root"]))

    state_store = StateStore()
    executor = DummyExecutor(
        {
            "root": ExecutionResult(ok=False, error="boom", duration_s=0.01),
        }
    )
    scheduler = Scheduler(
        registry,
        state_store,
        cast(TaskExecutor, executor),
        SchedulerConfig(max_parallel=2),
    )
    dag = DAG(name="demo", nodes=["root", "child"], edges={"root": [], "child": ["root"]})

    run_id = await scheduler.run_dag_once(dag)

    record = state_store.get_run(run_id)
    assert executor.calls == ["root"]
    assert executor.upstream_seen == [{}]
    assert record.tasks["root"].status == "FAILED"
    assert "boom" in (record.tasks["root"].error or "")
    assert record.tasks["child"].status == "SKIPPED"
    assert record.tasks["child"].error == "UPSTREAM_FAILED"
    assert record.finished_at is not None
