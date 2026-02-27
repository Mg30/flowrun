"""Tests for SqliteStateStore: persistence, TTL, recovery, serializers."""

from __future__ import annotations

import time
from typing import cast

import pytest

from flowrun.dag import DAG
from flowrun.executor import ExecutionResult, TaskExecutor
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.serialization import JsonSerializer, PickleSerializer
from flowrun.sqlite_store import SqliteStateStore
from flowrun.state import InMemoryStateStore, StateStoreProtocol
from flowrun.task import TaskRegistry, TaskSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class DummyExecutor:
    """Fake executor returning pre-configured results."""

    def __init__(self, outcomes: dict[str, ExecutionResult]) -> None:
        self._outcomes = outcomes
        self.calls: list[str] = []

    async def run_once(self, spec, timeout_s, context, upstream_results):
        self.calls.append(spec.name)
        return self._outcomes[spec.name]


DIAMOND_DAG = DAG(
    name="diamond",
    nodes=["A", "B", "C", "D"],
    edges={"A": [], "B": ["A"], "C": ["A"], "D": ["B", "C"]},
)


def _diamond_registry() -> TaskRegistry:
    reg = TaskRegistry()
    reg.register(TaskSpec(name="A", func=lambda: None))
    reg.register(TaskSpec(name="B", func=lambda: None, deps=["A"]))
    reg.register(TaskSpec(name="C", func=lambda: None, deps=["A"]))
    reg.register(TaskSpec(name="D", func=lambda: None, deps=["B", "C"]))
    return reg


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_sqlite_store_satisfies_protocol(self):
        store = SqliteStateStore(":memory:")
        assert isinstance(store, StateStoreProtocol)
        store.close()

    def test_inmemory_store_satisfies_protocol(self):
        store = InMemoryStateStore()
        assert isinstance(store, StateStoreProtocol)


# ---------------------------------------------------------------------------
# Basic state transitions (mirrors test_state_transitions.py)
# ---------------------------------------------------------------------------


class TestSqliteStateTransitions:
    def _store(self) -> SqliteStateStore:
        return SqliteStateStore(":memory:")

    def test_create_and_get_run(self):
        s = self._store()
        s.create_run("r1", "dag", ["a", "b"])
        rec = s.get_run("r1")
        assert rec.run_id == "r1"
        assert set(rec.tasks.keys()) == {"a", "b"}
        assert rec.tasks["a"].status == "PENDING"
        s.close()

    def test_valid_transition_pending_running_success(self):
        s = self._store()
        s.create_run("r1", "dag", ["t"])
        s.mark_running("r1", "t")
        assert s.get_run("r1").tasks["t"].status == "RUNNING"
        s.mark_success("r1", "t", {"val": 42})
        assert s.get_run("r1").tasks["t"].status == "SUCCESS"
        assert s.get_run("r1").tasks["t"].result == {"val": 42}
        s.close()

    def test_valid_transition_pending_running_failed(self):
        s = self._store()
        s.create_run("r1", "dag", ["t"])
        s.mark_running("r1", "t")
        s.mark_failed("r1", "t", "boom")
        tr = s.get_run("r1").tasks["t"]
        assert tr.status == "FAILED"
        assert tr.error == "boom"
        s.close()

    def test_valid_transition_pending_skipped(self):
        s = self._store()
        s.create_run("r1", "dag", ["t"])
        s.mark_skipped("r1", "t", "reason")
        tr = s.get_run("r1").tasks["t"]
        assert tr.status == "SKIPPED"
        assert tr.error == "reason"
        s.close()

    def test_retry_resets_to_pending(self):
        s = self._store()
        s.create_run("r1", "dag", ["t"])
        s.mark_running("r1", "t")
        s.mark_failed("r1", "t", "err")
        s.mark_retry("r1", "t")
        tr = s.get_run("r1").tasks["t"]
        assert tr.status == "PENDING"
        assert tr.error is None
        s.close()

    def test_invalid_transition_raises(self):
        s = self._store()
        s.create_run("r1", "dag", ["t"])
        with pytest.raises(RuntimeError, match="Invalid state transition"):
            s.mark_success("r1", "t", None)
        s.close()

    def test_finalize_run_if_done(self):
        s = self._store()
        s.create_run("r1", "dag", ["a", "b"])
        s.mark_running("r1", "a")
        s.mark_success("r1", "a", None)
        s.finalize_run_if_done("r1")
        assert s.get_run("r1").finished_at is None  # b still PENDING

        s.mark_skipped("r1", "b", "skip")
        s.finalize_run_if_done("r1")
        assert s.get_run("r1").finished_at is not None
        s.close()

    def test_clear_result(self):
        s = self._store()
        s.create_run("r1", "dag", ["t"])
        s.mark_running("r1", "t")
        s.mark_success("r1", "t", "data")
        assert s.get_run("r1").tasks["t"].result == "data"
        s.clear_result("r1", "t")
        assert s.get_run("r1").tasks["t"].result is None
        s.close()

    def test_get_missing_run_raises(self):
        s = self._store()
        with pytest.raises(KeyError):
            s.get_run("nope")
        s.close()


# ---------------------------------------------------------------------------
# create_resumed_run
# ---------------------------------------------------------------------------


class TestSqliteResumedRun:
    def test_copies_success_resets_failed(self):
        s = SqliteStateStore(":memory:")
        s.create_run("r1", "dag", ["A", "B", "C"])
        s.mark_running("r1", "A")
        s.mark_success("r1", "A", "a-result")
        s.mark_running("r1", "B")
        s.mark_failed("r1", "B", "boom")

        s.create_resumed_run("r2", "r1", "dag", ["A", "B", "C"])
        r2 = s.get_run("r2")
        assert r2.tasks["A"].status == "SUCCESS"
        assert r2.tasks["A"].result == "a-result"
        assert r2.tasks["B"].status == "PENDING"
        assert r2.tasks["C"].status == "PENDING"
        s.close()

    def test_reset_tasks_forces_pending(self):
        s = SqliteStateStore(":memory:")
        s.create_run("r1", "dag", ["A", "B"])
        s.mark_running("r1", "A")
        s.mark_success("r1", "A", "a")
        s.mark_running("r1", "B")
        s.mark_success("r1", "B", "b")

        s.create_resumed_run("r2", "r1", "dag", ["A", "B"], reset_tasks={"B"})
        r2 = s.get_run("r2")
        assert r2.tasks["A"].status == "SUCCESS"
        assert r2.tasks["B"].status == "PENDING"
        s.close()


# ---------------------------------------------------------------------------
# File persistence — survives close + reopen
# ---------------------------------------------------------------------------


class TestFilePersistence:
    def test_data_survives_reopen(self, tmp_path):
        db = tmp_path / "test.db"
        s1 = SqliteStateStore(db)
        s1.create_run("r1", "dag", ["t"])
        s1.mark_running("r1", "t")
        s1.mark_success("r1", "t", {"key": "value"})
        s1.close()

        s2 = SqliteStateStore(db)
        rec = s2.get_run("r1")
        assert rec.tasks["t"].status == "SUCCESS"
        assert rec.tasks["t"].result == {"key": "value"}
        s2.close()

    def test_resumed_run_survives_reopen(self, tmp_path):
        db = tmp_path / "test.db"
        s1 = SqliteStateStore(db)
        s1.create_run("r1", "dag", ["A", "B"])
        s1.mark_running("r1", "A")
        s1.mark_success("r1", "A", "a-ok")
        s1.mark_running("r1", "B")
        s1.mark_failed("r1", "B", "boom")
        s1.close()

        s2 = SqliteStateStore(db)
        s2.create_resumed_run("r2", "r1", "dag", ["A", "B"])
        r2 = s2.get_run("r2")
        assert r2.tasks["A"].status == "SUCCESS"
        assert r2.tasks["B"].status == "PENDING"
        s2.close()


# ---------------------------------------------------------------------------
# Crash recovery
# ---------------------------------------------------------------------------


class TestCrashRecovery:
    def test_recover_resets_running_to_failed(self, tmp_path):
        db = tmp_path / "test.db"
        s1 = SqliteStateStore(db)
        s1.create_run("r1", "dag", ["A", "B"])
        s1.mark_running("r1", "A")
        s1.mark_success("r1", "A", "ok")
        s1.mark_running("r1", "B")  # simulate crash: B stays RUNNING
        s1.close()

        # Reopen with recovery
        s2 = SqliteStateStore(db, recover=True)
        rec = s2.get_run("r1")
        assert rec.tasks["A"].status == "SUCCESS"  # untouched
        assert rec.tasks["B"].status == "FAILED"
        assert rec.tasks["B"].error == "PROCESS_CRASH"
        s2.close()

    def test_no_recover_leaves_running(self, tmp_path):
        db = tmp_path / "test.db"
        s1 = SqliteStateStore(db)
        s1.create_run("r1", "dag", ["t"])
        s1.mark_running("r1", "t")
        s1.close()

        # Reopen WITHOUT recovery
        s2 = SqliteStateStore(db, recover=False)
        rec = s2.get_run("r1")
        assert rec.tasks["t"].status == "RUNNING"
        s2.close()


# ---------------------------------------------------------------------------
# Cache TTL
# ---------------------------------------------------------------------------


class TestCacheTTL:
    def test_eviction_removes_stale_entries(self):
        s = SqliteStateStore(":memory:", cache_ttl_s=0.05)
        s.create_run("r1", "dag", ["t"])
        s.create_run("r2", "dag", ["t"])
        assert "r1" in s._cache
        assert "r2" in s._cache

        # Manually backdate r1's access time
        rec, _ = s._cache["r1"]
        s._cache["r1"] = (rec, time.time() - 1.0)

        # Touch r2 to refresh it, which also triggers eviction
        s.get_run("r2")
        s._maybe_evict()

        assert "r1" not in s._cache  # evicted from cache
        assert "r2" in s._cache  # still fresh

        # r1 is still loadable from SQLite
        rec = s.get_run("r1")
        assert rec.run_id == "r1"
        assert "r1" in s._cache  # re-cached on access
        s.close()

    def test_no_ttl_keeps_everything(self):
        s = SqliteStateStore(":memory:", cache_ttl_s=None)
        s.create_run("r1", "dag", ["t"])
        # Backdate heavily
        rec, _ = s._cache["r1"]
        s._cache["r1"] = (rec, time.time() - 9999)
        s._maybe_evict()
        assert "r1" in s._cache
        s.close()


# ---------------------------------------------------------------------------
# Serializer variants
# ---------------------------------------------------------------------------


class TestSerializers:
    def test_json_roundtrip(self, tmp_path):
        db = tmp_path / "test.db"
        s1 = SqliteStateStore(db, serializer=JsonSerializer())
        s1.create_run("r1", "dag", ["t"])
        s1.mark_running("r1", "t")
        s1.mark_success("r1", "t", {"nums": [1, 2, 3], "flag": True})
        s1.close()

        s2 = SqliteStateStore(db, serializer=JsonSerializer())
        result = s2.get_run("r1").tasks["t"].result
        assert result == {"nums": [1, 2, 3], "flag": True}
        s2.close()

    def test_pickle_roundtrip(self, tmp_path):
        db = tmp_path / "test.db"
        s1 = SqliteStateStore(db, serializer=PickleSerializer())
        custom_obj = {"set_data": {1, 2, 3}}  # sets are not JSON-serializable
        s1.create_run("r1", "dag", ["t"])
        s1.mark_running("r1", "t")
        s1.mark_success("r1", "t", custom_obj)
        s1.close()

        s2 = SqliteStateStore(db, serializer=PickleSerializer())
        result = s2.get_run("r1").tasks["t"].result
        assert result == {"set_data": {1, 2, 3}}
        s2.close()

    def test_none_result_roundtrip(self):
        s = SqliteStateStore(":memory:")
        s.create_run("r1", "dag", ["t"])
        s.mark_running("r1", "t")
        s.mark_success("r1", "t", None)
        assert s.get_run("r1").tasks["t"].result is None
        s.close()


# ---------------------------------------------------------------------------
# list_runs
# ---------------------------------------------------------------------------


class TestListRuns:
    def test_list_all(self):
        s = SqliteStateStore(":memory:")
        s.create_run("r1", "dag_a", ["t"])
        s.create_run("r2", "dag_b", ["t"])
        runs = s.list_runs()
        assert [r.run_id for r in runs] == ["r1", "r2"]
        s.close()

    def test_filter_by_dag_name(self):
        s = SqliteStateStore(":memory:")
        s.create_run("r1", "dag_a", ["t"])
        s.create_run("r2", "dag_b", ["t"])
        s.create_run("r3", "dag_a", ["t"])
        runs = s.list_runs(dag_name="dag_a")
        assert [r.run_id for r in runs] == ["r1", "r3"]
        s.close()


# ---------------------------------------------------------------------------
# Integration: scheduler with SqliteStateStore
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_with_sqlite_store():
    """Full DAG run with SqliteStateStore works identically to InMemoryStateStore."""
    registry = _diamond_registry()
    state = SqliteStateStore(":memory:")
    executor = DummyExecutor(
        {
            "A": ExecutionResult(ok=True, result="a", duration_s=0.01),
            "B": ExecutionResult(ok=True, result="b", duration_s=0.01),
            "C": ExecutionResult(ok=True, result="c", duration_s=0.01),
            "D": ExecutionResult(ok=True, result="d", duration_s=0.01),
        }
    )
    scheduler = Scheduler(registry, state, cast(TaskExecutor, executor), SchedulerConfig(max_parallel=4))
    run_id = await scheduler.run_dag_once(DIAMOND_DAG)
    rec = state.get_run(run_id)
    assert all(rec.tasks[t].status == "SUCCESS" for t in ["A", "B", "C", "D"])
    assert rec.finished_at is not None
    state.close()


@pytest.mark.asyncio
async def test_resume_with_sqlite_store():
    """Resume flow works end-to-end with SqliteStateStore."""
    from flowrun.engine import Engine

    registry = _diamond_registry()
    tok = registry.activate()
    state = SqliteStateStore(":memory:")

    # First run: A succeeds, B fails, C+D skipped
    state.create_run("old", "diamond", ["A", "B", "C", "D"])
    state.mark_running("old", "A")
    state.mark_success("old", "A", "a-ok")
    state.mark_running("old", "B")
    state.mark_failed("old", "B", "boom")
    state.mark_skipped("old", "C", "UPSTREAM_FAILED")
    state.mark_skipped("old", "D", "UPSTREAM_FAILED")

    executor = DummyExecutor(
        {
            "B": ExecutionResult(ok=True, result="b-ok", duration_s=0.01),
            "C": ExecutionResult(ok=True, result="c-ok", duration_s=0.01),
            "D": ExecutionResult(ok=True, result="d-ok", duration_s=0.01),
        }
    )
    scheduler = Scheduler(registry, state, cast(TaskExecutor, executor), SchedulerConfig(max_parallel=4))
    engine = Engine(registry, state, scheduler)

    new_run_id = await engine.resume("old")

    assert "A" not in executor.calls
    assert set(executor.calls) == {"B", "C", "D"}
    rec = state.get_run(new_run_id)
    assert all(rec.tasks[t].status == "SUCCESS" for t in ["A", "B", "C", "D"])

    TaskRegistry.deactivate(tok)
    state.close()


@pytest.mark.asyncio
async def test_crash_recovery_then_resume(tmp_path):
    """Simulate a crash mid-run, then recover and resume."""
    from flowrun.engine import Engine

    db = tmp_path / "test.db"
    registry = _diamond_registry()
    tok = registry.activate()

    # Phase 1: start a run, A succeeds, B starts (then "crash")
    s1 = SqliteStateStore(db)
    s1.create_run("r1", "diamond", ["A", "B", "C", "D"])
    s1.mark_running("r1", "A")
    s1.mark_success("r1", "A", "a-ok")
    s1.mark_running("r1", "B")  # B is RUNNING when we "crash"
    s1.close()

    # Phase 2: reopen with recovery, B becomes FAILED
    s2 = SqliteStateStore(db, recover=True)
    rec = s2.get_run("r1")
    assert rec.tasks["B"].status == "FAILED"
    assert rec.tasks["B"].error == "PROCESS_CRASH"

    # Phase 3: resume — A preserved, B+C+D re-run
    executor = DummyExecutor(
        {
            "B": ExecutionResult(ok=True, result="b-ok", duration_s=0.01),
            "C": ExecutionResult(ok=True, result="c-ok", duration_s=0.01),
            "D": ExecutionResult(ok=True, result="d-ok", duration_s=0.01),
        }
    )
    scheduler = Scheduler(registry, s2, cast(TaskExecutor, executor), SchedulerConfig(max_parallel=4))
    engine = Engine(registry, s2, scheduler)

    new_run_id = await engine.resume("r1")
    assert "A" not in executor.calls
    assert set(executor.calls) == {"B", "C", "D"}
    assert all(s2.get_run(new_run_id).tasks[t].status == "SUCCESS" for t in ["A", "B", "C", "D"])

    TaskRegistry.deactivate(tok)
    s2.close()
