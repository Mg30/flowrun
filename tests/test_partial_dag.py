"""Tests for partial-DAG execution: resume and sub-graph running."""

from typing import cast

import pytest

from flowrun.dag import DAG, DAGBuilder
from flowrun.executor import ExecutionResult, TaskExecutor
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import StateStore
from flowrun.task import TaskRegistry, TaskSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class DummyExecutor:
    """Fake executor that returns pre-configured results."""

    def __init__(self, outcomes: dict[str, ExecutionResult]) -> None:
        self._outcomes = outcomes
        self.calls: list[str] = []

    async def run_once(self, spec, timeout_s, context, upstream_results):
        self.calls.append(spec.name)
        return self._outcomes[spec.name]


def _build_diamond_registry() -> TaskRegistry:
    """Registry for a diamond DAG: A -> B, A -> C, B+C -> D."""
    reg = TaskRegistry()
    reg.register(TaskSpec(name="A", func=lambda: None))
    reg.register(TaskSpec(name="B", func=lambda: None, deps=["A"]))
    reg.register(TaskSpec(name="C", func=lambda: None, deps=["A"]))
    reg.register(TaskSpec(name="D", func=lambda: None, deps=["B", "C"]))
    return reg


DIAMOND_DAG = DAG(
    name="diamond",
    nodes=["A", "B", "C", "D"],
    edges={"A": [], "B": ["A"], "C": ["A"], "D": ["B", "C"]},
)


# ---------------------------------------------------------------------------
# DAG.subgraph
# ---------------------------------------------------------------------------


class TestSubgraph:
    def test_single_root(self):
        sub = DIAMOND_DAG.subgraph(["A"])
        assert sub.nodes == ["A"]
        assert sub.edges == {"A": []}

    def test_single_leaf(self):
        sub = DIAMOND_DAG.subgraph(["D"])
        assert set(sub.nodes) == {"A", "B", "C", "D"}

    def test_middle_branch(self):
        sub = DIAMOND_DAG.subgraph(["B"])
        assert set(sub.nodes) == {"A", "B"}
        assert sub.edges["B"] == ["A"]

    def test_preserves_order(self):
        sub = DIAMOND_DAG.subgraph(["D"])
        # A must come before B and C, both before D
        assert sub.nodes.index("A") < sub.nodes.index("B")
        assert sub.nodes.index("A") < sub.nodes.index("C")
        assert sub.nodes.index("B") < sub.nodes.index("D")
        assert sub.nodes.index("C") < sub.nodes.index("D")

    def test_unknown_target_raises(self):
        with pytest.raises(ValueError, match="not in the DAG"):
            DIAMOND_DAG.subgraph(["NOPE"])


# ---------------------------------------------------------------------------
# DAG.descendants_of
# ---------------------------------------------------------------------------


class TestDescendantsOf:
    def test_leaf_has_no_descendants(self):
        assert DIAMOND_DAG.descendants_of({"D"}) == {"D"}

    def test_root_includes_all(self):
        assert DIAMOND_DAG.descendants_of({"A"}) == {"A", "B", "C", "D"}

    def test_middle_includes_downstream(self):
        assert DIAMOND_DAG.descendants_of({"B"}) == {"B", "D"}

    def test_multiple_seeds(self):
        assert DIAMOND_DAG.descendants_of({"B", "C"}) == {"B", "C", "D"}


# ---------------------------------------------------------------------------
# StateStore.create_resumed_run
# ---------------------------------------------------------------------------


class TestCreateResumedRun:
    def test_copies_success_resets_failed(self):
        store = StateStore()
        store.create_run("r1", "dag", ["A", "B", "C"])
        store.mark_running("r1", "A")
        store.mark_success("r1", "A", "a-result")
        store.mark_running("r1", "B")
        store.mark_failed("r1", "B", "boom")
        # C stays PENDING

        store.create_resumed_run("r2", "r1", "dag", ["A", "B", "C"])
        r2 = store.get_run("r2")

        assert r2.tasks["A"].status == "SUCCESS"
        assert r2.tasks["A"].result == "a-result"
        assert r2.tasks["B"].status == "PENDING"
        assert r2.tasks["C"].status == "PENDING"

    def test_reset_tasks_forces_pending(self):
        store = StateStore()
        store.create_run("r1", "dag", ["A", "B"])
        store.mark_running("r1", "A")
        store.mark_success("r1", "A", "a-result")
        store.mark_running("r1", "B")
        store.mark_success("r1", "B", "b-result")

        store.create_resumed_run("r2", "r1", "dag", ["A", "B"], reset_tasks={"B"})
        r2 = store.get_run("r2")

        assert r2.tasks["A"].status == "SUCCESS"
        assert r2.tasks["B"].status == "PENDING"


# ---------------------------------------------------------------------------
# Scheduler resume (pre-created run with SUCCESS tasks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_skips_preloaded_success_tasks():
    """The scheduler should skip tasks already marked SUCCESS in a resumed run."""
    registry = _build_diamond_registry()
    state = StateStore()

    # Simulate: A succeeded, B failed, C+D were skipped
    state.create_run("old", "diamond", ["A", "B", "C", "D"])
    state.mark_running("old", "A")
    state.mark_success("old", "A", "a-ok")
    state.mark_running("old", "B")
    state.mark_failed("old", "B", "boom")
    state.mark_skipped("old", "C", "UPSTREAM_FAILED")
    state.mark_skipped("old", "D", "UPSTREAM_FAILED")

    # Create a resumed run — A stays SUCCESS, the rest reset to PENDING
    state.create_resumed_run("new", "old", "diamond", ["A", "B", "C", "D"])

    executor = DummyExecutor(
        {
            "B": ExecutionResult(ok=True, result="b-ok", duration_s=0.01),
            "C": ExecutionResult(ok=True, result="c-ok", duration_s=0.01),
            "D": ExecutionResult(ok=True, result="d-ok", duration_s=0.01),
        }
    )
    scheduler = Scheduler(registry, state, cast(TaskExecutor, executor), SchedulerConfig(max_parallel=4))

    run_id = await scheduler.run_dag_once(DIAMOND_DAG, run_id="new")

    assert run_id == "new"
    # A was never executed — it was already SUCCESS
    assert "A" not in executor.calls
    assert set(executor.calls) == {"B", "C", "D"}

    rec = state.get_run("new")
    assert all(rec.tasks[t].status == "SUCCESS" for t in ["A", "B", "C", "D"])


# ---------------------------------------------------------------------------
# Engine.resume (integration)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_resume_skips_successful_tasks():
    """Engine.resume creates a new run preserving SUCCESS states."""
    from flowrun.engine import Engine

    registry = _build_diamond_registry()
    tok = registry.activate()

    state = StateStore()

    # Build a first run where A+B succeeded but C failed
    state.create_run("old", "diamond", ["A", "B", "C", "D"])
    state.mark_running("old", "A")
    state.mark_success("old", "A", "a-ok")
    state.mark_running("old", "B")
    state.mark_success("old", "B", "b-ok")
    state.mark_running("old", "C")
    state.mark_failed("old", "C", "boom")
    state.mark_skipped("old", "D", "UPSTREAM_FAILED")

    executor = DummyExecutor(
        {
            "C": ExecutionResult(ok=True, result="c-ok", duration_s=0.01),
            "D": ExecutionResult(ok=True, result="d-ok", duration_s=0.01),
        }
    )
    scheduler = Scheduler(registry, state, cast(TaskExecutor, executor), SchedulerConfig(max_parallel=4))
    engine = Engine(registry, state, scheduler)

    new_run_id = await engine.resume("old")

    assert "A" not in executor.calls
    assert "B" not in executor.calls
    assert set(executor.calls) == {"C", "D"}

    rec = state.get_run(new_run_id)
    assert all(rec.tasks[t].status == "SUCCESS" for t in ["A", "B", "C", "D"])

    TaskRegistry.deactivate(tok)


@pytest.mark.asyncio
async def test_engine_resume_from_tasks():
    """Engine.resume(from_tasks=...) forces a specific task and its downstream to re-run."""
    from flowrun.engine import Engine

    registry = _build_diamond_registry()
    tok = registry.activate()
    state = StateStore()

    # All tasks succeeded in the original run
    state.create_run("old", "diamond", ["A", "B", "C", "D"])
    for t in ["A", "B", "C", "D"]:
        state.mark_running("old", t)
        state.mark_success("old", t, f"{t}-ok")

    executor = DummyExecutor(
        {
            "B": ExecutionResult(ok=True, result="b-v2", duration_s=0.01),
            "D": ExecutionResult(ok=True, result="d-v2", duration_s=0.01),
        }
    )
    scheduler = Scheduler(registry, state, cast(TaskExecutor, executor), SchedulerConfig(max_parallel=4))
    engine = Engine(registry, state, scheduler)

    new_run_id = await engine.resume("old", from_tasks=["B"])

    # B and its downstream D should re-run; A and C stay preserved
    assert set(executor.calls) == {"B", "D"}

    rec = state.get_run(new_run_id)
    assert rec.tasks["A"].result == "A-ok"  # preserved
    assert rec.tasks["C"].result == "C-ok"  # preserved
    assert rec.tasks["B"].result == "b-v2"  # re-executed
    assert rec.tasks["D"].result == "d-v2"  # re-executed

    TaskRegistry.deactivate(tok)


# ---------------------------------------------------------------------------
# Engine.run_subgraph (integration)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_run_subgraph():
    """Engine.run_subgraph runs only the target tasks and their ancestors."""
    from flowrun.engine import Engine

    registry = _build_diamond_registry()
    tok = registry.activate()
    state = StateStore()

    executor = DummyExecutor(
        {
            "A": ExecutionResult(ok=True, result="a-ok", duration_s=0.01),
            "B": ExecutionResult(ok=True, result="b-ok", duration_s=0.01),
        }
    )
    scheduler = Scheduler(registry, state, cast(TaskExecutor, executor), SchedulerConfig(max_parallel=4))
    engine = Engine(registry, state, scheduler)

    run_id = await engine.run_subgraph("diamond", ["B"])

    # Only A and B should run (B depends on A). C and D are excluded.
    assert set(executor.calls) == {"A", "B"}

    rec = state.get_run(run_id)
    assert "C" not in rec.tasks
    assert "D" not in rec.tasks
    assert rec.tasks["A"].status == "SUCCESS"
    assert rec.tasks["B"].status == "SUCCESS"

    TaskRegistry.deactivate(tok)
