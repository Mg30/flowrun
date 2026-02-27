from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal, Protocol, runtime_checkable

TaskStatus = Literal[
    "PENDING",
    "RUNNING",
    "SUCCESS",
    "FAILED",
    "SKIPPED",
]


@dataclass
class TaskRunRecord:
    """Data record for a single task execution within a run.

    Attributes
    ----------
    task_name : str
        The name/identifier of the task.
    attempt : int
        Number of execution attempts for the task.
    status : TaskStatus
        Current execution status (PENDING, RUNNING, SUCCESS, FAILED, SKIPPED).
    started_at : float | None
        Timestamp when execution started, or None if not started.
    finished_at : float | None
        Timestamp when execution finished, or None if not finished.
    error : str | None
        Error message when the task failed or was skipped, otherwise None.
    result : object | None
        Result produced by the task when successful, otherwise None.
    """

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


@runtime_checkable
class StateStoreProtocol(Protocol):
    """Structural interface every state-store backend must satisfy."""

    def create_run(self, run_id: str, dag_name: str, task_names: list[str]) -> RunRecord:
        """Create and persist a brand-new run record for *dag_name*."""
        ...

    def create_resumed_run(
        self,
        run_id: str,
        prev_run_id: str,
        dag_name: str,
        task_names: list[str],
        reset_tasks: set[str] | None = None,
    ) -> RunRecord:
        """Create a new run that inherits successful results from *prev_run_id*."""
        ...

    def get_run(self, run_id: str) -> RunRecord:
        """Return the RunRecord for *run_id*."""
        ...

    def mark_running(self, run_id: str, task_name: str) -> None:
        """Transition *task_name* to RUNNING."""
        ...

    def mark_success(self, run_id: str, task_name: str, result: object) -> None:
        """Transition *task_name* to SUCCESS and store *result*."""
        ...

    def mark_failed(self, run_id: str, task_name: str, err: str) -> None:
        """Transition *task_name* to FAILED with the given *err* message."""
        ...

    def mark_skipped(self, run_id: str, task_name: str, reason: str) -> None:
        """Transition *task_name* to SKIPPED with a skip *reason*."""
        ...

    def mark_retry(self, run_id: str, task_name: str) -> None:
        """Reset *task_name* to PENDING so it can be retried."""
        ...

    def clear_result(self, run_id: str, task_name: str) -> None:
        """Remove the stored result for *task_name* in *run_id*."""
        ...

    def finalize_run_if_done(self, run_id: str) -> None:
        """Set the run's finished_at timestamp once all tasks are in a terminal state."""
        ...


class InMemoryStateStore:
    """
    In-memory implementation holding runs and task states.
    SRP: persistence and state transitions only.
    """

    def __init__(self) -> None:
        """Initialize the state store with an empty run registry."""
        self._runs: dict[str, RunRecord] = {}

    def create_run(self, run_id: str, dag_name: str, task_names: list[str]) -> RunRecord:
        """Create a new RunRecord and initialize TaskRunRecord entries for each task.

        Parameters
        ----------
        run_id : str
            Unique identifier for the run.
        dag_name : str
            Name of the DAG.
        task_names : list[str]
            List of task names to initialize in the run.

        Returns
        -------
        RunRecord
            The created run record containing initialized TaskRunRecord objects.
        """
        rec = RunRecord(
            run_id=run_id,
            dag_name=dag_name,
            tasks={t: TaskRunRecord(task_name=t) for t in task_names},
        )
        self._runs[run_id] = rec
        return rec

    def create_resumed_run(
        self,
        run_id: str,
        prev_run_id: str,
        dag_name: str,
        task_names: list[str],
        reset_tasks: set[str] | None = None,
    ) -> RunRecord:
        """Create a new run pre-populated with SUCCESS results from a previous run.

        Tasks that were successful in the previous run (and are not in
        *reset_tasks*) are copied as-is so the scheduler skips them.
        Everything else starts as PENDING.

        Parameters
        ----------
        run_id : str
            Unique identifier for the new run.
        prev_run_id : str
            Identifier of the run to resume from.
        dag_name : str
            Name of the DAG.
        task_names : list[str]
            Full list of task names in the DAG.
        reset_tasks : set[str] | None
            Tasks that must be reset to PENDING even if previously successful
            (e.g. a task the user wants to force re-run plus its downstream).

        Returns
        -------
        RunRecord
            The created run record.
        """
        prev = self._runs[prev_run_id]
        reset_tasks = reset_tasks or set()
        rec = RunRecord(run_id=run_id, dag_name=dag_name, tasks={})
        for t in task_names:
            prev_task = prev.tasks.get(t)
            if prev_task and prev_task.status == "SUCCESS" and t not in reset_tasks:
                rec.tasks[t] = TaskRunRecord(
                    task_name=t,
                    attempt=prev_task.attempt,
                    status="SUCCESS",
                    started_at=prev_task.started_at,
                    finished_at=prev_task.finished_at,
                    result=prev_task.result,
                )
            else:
                rec.tasks[t] = TaskRunRecord(task_name=t)
        self._runs[run_id] = rec
        return rec

    def get_run(self, run_id: str) -> RunRecord:
        """Return the RunRecord for the given run_id.

        Parameters
        ----------
        run_id : str
            Identifier of the run to retrieve.

        Returns
        -------
        RunRecord
            The stored run record.

        Raises
        ------
        KeyError
            If the run_id is not present in the store.
        """
        return self._runs[run_id]

    # --- task status helpers ---

    _VALID_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
        "PENDING": {"RUNNING", "SKIPPED"},
        "RUNNING": {"SUCCESS", "FAILED"},
        "SUCCESS": set(),
        "FAILED": {"PENDING"},  # allows retry
        "SKIPPED": set(),
    }

    def _transition(self, run_id: str, task_name: str, target: TaskStatus) -> TaskRunRecord:
        """Validate and perform a state transition, returning the task record.

        Raises
        ------
        RuntimeError
            If the transition is not allowed.
        """
        tr = self._runs[run_id].tasks[task_name]
        allowed = self._VALID_TRANSITIONS.get(tr.status, set())
        if target not in allowed:
            raise RuntimeError(f"Invalid state transition for task {task_name!r}: {tr.status} -> {target}")
        tr.status = target
        return tr

    def mark_running(self, run_id: str, task_name: str) -> None:
        """Mark the given task as running, record its start time, and increment the attempt counter.

        Parameters
        ----------
        run_id : str
            Identifier of the run containing the task.
        task_name : str
            Name of the task to mark as running.
        """
        tr = self._transition(run_id, task_name, "RUNNING")
        tr.started_at = time.time()
        tr.attempt += 1

    def mark_success(self, run_id: str, task_name: str, result: object) -> None:
        """Mark the given task as successful, store its result, and record the finish time.

        Parameters
        ----------
        run_id : str
            Identifier of the run containing the task.
        task_name : str
            Name of the task to mark as successful.
        result : object
            Result produced by the task.
        """
        tr = self._transition(run_id, task_name, "SUCCESS")
        tr.result = result
        tr.finished_at = time.time()

    def mark_failed(self, run_id: str, task_name: str, err: str) -> None:
        """Mark the given task as failed, record the error message, and set the finish time.

        Parameters
        ----------
        run_id : str
            Identifier of the run containing the task.
        task_name : str
            Name of the task to mark as failed.
        err : str
            Error message or information about the failure.
        """
        tr = self._transition(run_id, task_name, "FAILED")
        tr.error = err
        tr.finished_at = time.time()

    def mark_skipped(self, run_id: str, task_name: str, reason: str) -> None:
        """Mark the given task as skipped, record the reason as error text, and set the finish time.

        Parameters
        ----------
        run_id : str
            Identifier of the run containing the task.
        task_name : str
            Name of the task to mark as skipped.
        reason : str
            Reason why the task was skipped.
        """
        tr = self._transition(run_id, task_name, "SKIPPED")
        tr.error = reason
        tr.finished_at = time.time()

    def mark_retry(self, run_id: str, task_name: str) -> None:
        """Reset a FAILED task back to PENDING so it can be re-scheduled.

        Parameters
        ----------
        run_id : str
            Identifier of the run containing the task.
        task_name : str
            Name of the task to reset.
        """
        tr = self._transition(run_id, task_name, "PENDING")
        tr.error = None
        tr.finished_at = None

    def clear_result(self, run_id: str, task_name: str) -> None:
        """Drop the stored result for a task to free memory.

        Parameters
        ----------
        run_id : str
            Identifier of the run.
        task_name : str
            Name of the task whose result to drop.
        """
        self._runs[run_id].tasks[task_name].result = None

    def finalize_run_if_done(self, run_id: str) -> None:
        """Set the run finished timestamp when all tasks reached a terminal state.

        Parameters
        ----------
        run_id : str
            Identifier of the run to check and possibly finalize.

        Notes
        -----
        A task is considered terminal when its status is one of "SUCCESS", "FAILED",
        or "SKIPPED". If all tasks are terminal, the run's finished_at is set to the
        current time; otherwise the run remains open.
        """
        rr = self._runs[run_id]
        if all(t.status in ("SUCCESS", "FAILED", "SKIPPED") for t in rr.tasks.values()):
            rr.finished_at = time.time()


# Backwards-compatible alias — existing code that imports ``StateStore`` keeps working.
StateStore = InMemoryStateStore
