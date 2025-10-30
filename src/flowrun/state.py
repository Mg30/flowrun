import time
from dataclasses import dataclass, field
from typing import Literal

TaskStatus = Literal[
    "PENDING",
    "RUNNING",
    "SUCCESS",
    "FAILED",
    "SKIPPED",
]


@dataclass
class TaskRunRecord:
    task_name: str
    attempt: int = 0
    status: TaskStatus = "PENDING"
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    result: object | None = None


@dataclass
class RunRecord:
    """
    Represents one DAG run.
    """

    run_id: str
    dag_name: str
    tasks: dict[str, TaskRunRecord] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None


class StateStore:
    """
    In-memory implementation holding runs and task states.
    SRP: persistence and state transitions only.
    Can later be swapped with SQLite/Postgres/Redis.
    """

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}

    def create_run(self, run_id: str, dag_name: str, task_names: list[str]) -> RunRecord:
        rec = RunRecord(
            run_id=run_id,
            dag_name=dag_name,
            tasks={t: TaskRunRecord(task_name=t) for t in task_names},
        )
        self._runs[run_id] = rec
        return rec

    def get_run(self, run_id: str) -> RunRecord:
        return self._runs[run_id]

    # --- task status helpers ---

    def mark_running(self, run_id: str, task_name: str) -> None:
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "RUNNING"
        tr.started_at = time.time()
        tr.attempt += 1

    def mark_success(self, run_id: str, task_name: str, result: object) -> None:
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "SUCCESS"
        tr.result = result
        tr.finished_at = time.time()

    def mark_failed(self, run_id: str, task_name: str, err: str) -> None:
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "FAILED"
        tr.error = err
        tr.finished_at = time.time()

    def mark_skipped(self, run_id: str, task_name: str, reason: str) -> None:
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "SKIPPED"
        tr.error = reason
        tr.finished_at = time.time()

    def finalize_run_if_done(self, run_id: str) -> None:
        rr = self._runs[run_id]
        if all(t.status in ("SUCCESS", "FAILED", "SKIPPED") for t in rr.tasks.values()):
            rr.finished_at = time.time()
