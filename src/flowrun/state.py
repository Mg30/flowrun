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


class StateStore:
    """
    In-memory implementation holding runs and task states.
    SRP: persistence and state transitions only.
    Can later be swapped with SQLite/Postgres/Redis.
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

    def mark_running(self, run_id: str, task_name: str) -> None:
        """Mark the given task as running, record its start time, and increment the attempt counter.

        Parameters
        ----------
        run_id : str
            Identifier of the run containing the task.
        task_name : str
            Name of the task to mark as running.
        """
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "RUNNING"
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
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "SUCCESS"
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
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "FAILED"
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
        tr = self._runs[run_id].tasks[task_name]
        tr.status = "SKIPPED"
        tr.error = reason
        tr.finished_at = time.time()

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
