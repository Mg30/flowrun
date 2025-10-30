import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

from flowrun.context import RunContext
from flowrun.dag import DAG
from flowrun.executor import TaskExecutor
from flowrun.state import StateStore
from flowrun.task import TaskRegistry


@dataclass
class SchedulerConfig:
    """Configuration for the Scheduler.

    Attributes:
        max_parallel (int): Global cap for the number of tasks that may run concurrently.
        Future: Notes about planned features such as per-key caps and prioritization.
    """

    max_parallel: int = 4  # global cap


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
        state_store: StateStore,
        executor: TaskExecutor,
        config: SchedulerConfig,
    ) -> None:
        """Wire dependencies required to schedule and execute tasks."""
        self._registry = registry
        self._state = state_store
        self._executor = executor
        self._cfg = config

    @property
    def executor(self) -> TaskExecutor:
        """Return the task executor used to run individual task specs."""
        return self._executor

    async def run_dag_once(self, dag: DAG, context: RunContext[Any] | None = None) -> str:
        """Execute a DAG once, tracking task state and returning the run id."""
        run_id = str(uuid.uuid4())
        self._state.create_run(run_id, dag.name, dag.nodes)

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
                break

            # 2. wait for at least one task to finish
            done, _ = await asyncio.wait(
                inflight.values(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            # 3. handle completions (record success/failure)
            finished_task_names: set[str] = set()
            for t in done:
                task_name, exec_res = t.result()
                finished_task_names.add(task_name)

                if exec_res.ok:
                    self._state.mark_success(run_id, task_name, exec_res.result)
                else:
                    self._state.mark_failed(run_id, task_name, exec_res.error)
                    # downstream tasks should become SKIPPED later
                    # (lazy skip logic handled via _deps_success gate)

            # 4. cleanup inflight entries that are done
            for tn in finished_task_names:
                inflight.pop(tn, None)

            # 5. mark SKIPPED tasks whose parents FAILED
            self._mark_skipped_blocked(run_id, dag)

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
        parents = dag.parents_of(task_name)
        runrec = self._state.get_run(run_id)
        for p in parents:
            ps = runrec.tasks[p].status
            if ps == "FAILED":
                return False
            if ps == "SKIPPED":
                return False
            if ps != "SUCCESS":
                return False
        return True

    def _mark_skipped_blocked(self, run_id: str, dag: DAG) -> None:
        runrec = self._state.get_run(run_id)
        for tname, trec in runrec.tasks.items():
            if trec.status == "PENDING":
                # If any parent FAILED or SKIPPED permanently, this task is never going to be runnable.
                parents = dag.parents_of(tname)
                bad_parent = any(runrec.tasks[p].status in ("FAILED", "SKIPPED") for p in parents)
                if bad_parent:
                    self._state.mark_skipped(run_id, tname, "UPSTREAM_FAILED")
