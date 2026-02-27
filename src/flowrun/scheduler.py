import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from flowrun.context import RunContext
from flowrun.dag import DAG
from flowrun.executor import TaskExecutor
from flowrun.hooks import (
    DagEndEvent,
    DagStartEvent,
    HookDispatcher,
    RunHook,
    TaskFailureEvent,
    TaskRetryEvent,
    TaskSkipEvent,
    TaskStartEvent,
    TaskSuccessEvent,
)
from flowrun.state import StateStoreProtocol
from flowrun.task import TaskRegistry

_default_logger = logging.getLogger("flowrun.scheduler")


@dataclass
class SchedulerConfig:
    """Configuration for the Scheduler.

    Attributes:
        max_parallel (int): Global cap for the number of tasks that may run concurrently.
        Future: Notes about planned features such as per-key caps and prioritization.
    """

    max_parallel: int = 4  # global cap

    def __post_init__(self) -> None:
        """Validate configuration values after dataclass initialisation."""
        if self.max_parallel < 1:
            raise ValueError("max_parallel must be >= 1")


class Scheduler:
    """Schedule and execute tasks in a DAG while enforcing dependency and concurrency rules.

    The Scheduler coordinates runs of a DAG by creating a run record in the StateStore,
    scheduling tasks whose dependencies are satisfied, invoking the TaskExecutor to run
    task specs obtained from the TaskRegistry, and updating task states (SUCCESS, FAILED,
    SKIPPED, etc.). It also enforces a global concurrency cap provided by SchedulerConfig.
    """

    def __init__(
        self,
        registry: TaskRegistry,
        state_store: StateStoreProtocol,
        executor: TaskExecutor,
        config: SchedulerConfig,
        *,
        logger: logging.Logger | None = None,
        hooks: list[RunHook] | None = None,
    ) -> None:
        """Wire dependencies required to schedule and execute tasks.

        Parameters
        ----------
        logger : logging.Logger | None
            Optional logger instance. Falls back to ``logging.getLogger('flowrun.scheduler')``.
        hooks : list[RunHook] | None
            Optional list of lifecycle hooks invoked on DAG/task events.
        """
        self._registry = registry
        self._state = state_store
        self._executor = executor
        self._cfg = config
        self._log = logger or _default_logger
        self._hooks = HookDispatcher(hooks, logger=self._log)

    @property
    def executor(self) -> TaskExecutor:
        """Return the task executor used to run individual task specs."""
        return self._executor

    async def run_dag_once(self, dag: DAG, context: RunContext[Any] | None = None, *, run_id: str | None = None) -> str:
        """Execute a DAG once, tracking task state and returning the run id.

        When *run_id* is provided the scheduler assumes a run record already
        exists in the state store (e.g. a resumed run with pre-populated
        SUCCESS states).  Otherwise a fresh run record is created.
        """
        if run_id is None:
            run_id = str(uuid.uuid4())
            self._state.create_run(run_id, dag.name, dag.nodes)
        self._hooks.emit("on_dag_start", DagStartEvent(run_id=run_id, dag_name=dag.name))

        inflight: dict[str, asyncio.Task] = {}

        while True:
            # 1. schedule newly-eligible tasks
            for task_name in dag.nodes:
                node_state = self._state.get_run(run_id).tasks[task_name]
                if node_state.status == "PENDING" and task_name not in inflight:
                    if self._deps_success(run_id, dag, task_name):
                        if len(inflight) < self._cfg.max_parallel:
                            inflight[task_name] = self._launch_task(run_id, dag, task_name, context)

            if not inflight:
                # no tasks running and nothing eligible -> we are done
                self._state.finalize_run_if_done(run_id)
                self._hooks.emit("on_dag_end", DagEndEvent(run_id=run_id, dag_name=dag.name))
                break

            # 2. wait for at least one task to finish
            done, _ = await asyncio.wait(
                inflight.values(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            # 3. handle completions (record success/failure, retry logic)
            finished_task_names: set[str] = set()
            for t in done:
                task_name, exec_res = t.result()
                finished_task_names.add(task_name)

                if exec_res.ok:
                    self._state.mark_success(run_id, task_name, exec_res.result)
                    self._log.info(
                        "Task %r succeeded  run_id=%s  duration=%.3fs",
                        task_name,
                        run_id,
                        exec_res.duration_s,
                    )
                    self._hooks.emit(
                        "on_task_success",
                        TaskSuccessEvent(
                            run_id=run_id,
                            dag_name=dag.name,
                            task_name=task_name,
                            attempt=self._state.get_run(run_id).tasks[task_name].attempt,
                            duration_s=exec_res.duration_s,
                            result=exec_res.result,
                        ),
                    )
                else:
                    spec = self._registry.get(task_name)
                    task_rec = self._state.get_run(run_id).tasks[task_name]
                    self._state.mark_failed(run_id, task_name, exec_res.error)
                    self._log.warning(
                        "Task %r failed  run_id=%s  attempt=%d  error=%s",
                        task_name,
                        run_id,
                        task_rec.attempt,
                        (exec_res.error or "")[:200],
                    )
                    self._hooks.emit(
                        "on_task_failure",
                        TaskFailureEvent(
                            run_id=run_id,
                            dag_name=dag.name,
                            task_name=task_name,
                            attempt=task_rec.attempt,
                            duration_s=exec_res.duration_s,
                            error=exec_res.error,
                        ),
                    )
                    # Retry if attempts remain
                    if task_rec.attempt <= spec.retries:
                        self._log.info(
                            "Scheduling retry for task %r  run_id=%s  attempt=%d/%d",
                            task_name,
                            run_id,
                            task_rec.attempt + 1,
                            spec.retries + 1,
                        )
                        self._state.mark_retry(run_id, task_name)
                        self._hooks.emit(
                            "on_task_retry",
                            TaskRetryEvent(
                                run_id=run_id,
                                dag_name=dag.name,
                                task_name=task_name,
                                next_attempt=task_rec.attempt + 1,
                                max_attempts=spec.retries + 1,
                            ),
                        )

            # 4. cleanup inflight entries that are done
            for tn in finished_task_names:
                inflight.pop(tn, None)

            # 5. mark SKIPPED tasks whose parents permanently FAILED
            self._mark_skipped_blocked(run_id, dag)

            # 6. release memory for non-retained results whose consumers are all launched/done
            self._release_non_retained(run_id, dag)

            self._state.finalize_run_if_done(run_id)

            # loop again until nothing left

        return run_id

    def _launch_task(
        self,
        run_id: str,
        dag: DAG,
        task_name: str,
        context: RunContext[Any] | None,
    ) -> asyncio.Task:
        spec = self._registry.get(task_name)
        runrec = self._state.get_run(run_id)
        upstream_results = {
            parent: runrec.tasks[parent].result
            for parent in dag.parents_of(task_name)
            if runrec.tasks[parent].status == "SUCCESS"
        }
        # mark running before execution
        self._state.mark_running(run_id, task_name)
        self._log.debug("Launching task %r  run_id=%s", task_name, run_id)
        self._hooks.emit(
            "on_task_start",
            TaskStartEvent(
                run_id=run_id,
                dag_name=dag.name,
                task_name=task_name,
                attempt=self._state.get_run(run_id).tasks[task_name].attempt,
            ),
        )

        async def _runner():
            exec_res = await self._executor.run_once(
                spec,
                spec.timeout_s,
                context,
                upstream_results,
            )
            return task_name, exec_res

        return asyncio.create_task(_runner())

    def _deps_success(self, run_id: str, dag: DAG, task_name: str) -> bool:
        runrec = self._state.get_run(run_id)
        return all(runrec.tasks[p].status == "SUCCESS" for p in dag.parents_of(task_name))

    def _mark_skipped_blocked(self, run_id: str, dag: DAG) -> None:
        runrec = self._state.get_run(run_id)
        for tname, trec in runrec.tasks.items():
            if trec.status == "PENDING":
                # If any parent FAILED or SKIPPED permanently, this task is never going to be runnable.
                # But only consider a parent permanently failed if its retries are exhausted.
                parents = dag.parents_of(tname)
                bad_parent = any(
                    runrec.tasks[p].status in ("FAILED", "SKIPPED")
                    and (runrec.tasks[p].status == "SKIPPED" or runrec.tasks[p].attempt > self._registry.get(p).retries)
                    for p in parents
                )
                if bad_parent:
                    self._log.info("Skipping task %r  run_id=%s  reason=UPSTREAM_FAILED", tname, run_id)
                    self._state.mark_skipped(run_id, tname, "UPSTREAM_FAILED")
                    self._hooks.emit(
                        "on_task_skip",
                        TaskSkipEvent(
                            run_id=run_id,
                            dag_name=dag.name,
                            task_name=tname,
                            reason="UPSTREAM_FAILED",
                        ),
                    )

    def _release_non_retained(self, run_id: str, dag: DAG) -> None:
        """Clear results for tasks with ``retain_result=False`` once all dependents are done/launched."""
        runrec = self._state.get_run(run_id)
        # Build children map (task -> list of tasks that depend on it)
        children: dict[str, list[str]] = {node: [] for node in dag.nodes}
        for child, parents in dag.edges.items():
            for parent in parents:
                children.setdefault(parent, []).append(child)

        for tname, trec in runrec.tasks.items():
            if trec.status != "SUCCESS" or trec.result is None:
                continue
            spec = self._registry.get(tname)
            if spec.retain_result:
                continue
            # Release if all children are no longer PENDING (launched, done, or skipped)
            if all(runrec.tasks[c].status != "PENDING" for c in children.get(tname, [])):
                self._state.clear_result(run_id, tname)
