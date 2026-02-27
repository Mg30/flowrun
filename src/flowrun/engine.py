import concurrent.futures
import logging
import uuid
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import Any, Self

from flowrun.context import RunContext
from flowrun.dag import DAGBuilder
from flowrun.executor import TaskExecutor
from flowrun.hooks import RunHook
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import RunRecord, StateStore, StateStoreProtocol
from flowrun.task import TaskRegistry
from flowrun.task import task as task_decorator
from flowrun.task import task_template as task_template_factory

_default_logger = logging.getLogger("flowrun")


class DagScope:
    """DAG-scoped API facade to avoid repeating ``dag=...`` on every task."""

    def __init__(self, engine: "Engine", dag_name: str) -> None:
        """Initialize the DagScope bound to *engine* and *dag_name*."""
        self._engine = engine
        self._dag_name = dag_name

    @property
    def name(self) -> str:
        """Return the DAG namespace this scope is bound to."""
        return self._dag_name

    def task(
        self,
        name: str | None = None,
        deps: Sequence[str | Callable[..., Any]] | None = None,
        timeout_s: float | None = 30.0,
        retries: int = 0,
        retain_result: bool = True,
    ):
        """Return ``@task`` decorator bound to this scope's DAG."""
        return self._engine.task(
            name=name,
            deps=deps,
            timeout_s=timeout_s,
            retries=retries,
            retain_result=retain_result,
            dag=self._dag_name,
        )

    def task_template(
        self,
        func: Callable[..., Any],
        *,
        deps: Sequence[str | Callable[..., Any]] | None = None,
        timeout_s: float | None = 30.0,
    ):
        """Create a task template bound to this scope's DAG."""
        return self._engine.task_template(
            func,
            deps=deps,
            timeout_s=timeout_s,
            dag=self._dag_name,
        )

    async def run_once(self, context: RunContext[Any] | None = None) -> str:
        """Run this DAG once."""
        return await self._engine.run_once(self._dag_name, context=context)

    async def run_subgraph(
        self,
        targets: list[str],
        context: RunContext[Any] | None = None,
    ) -> str:
        """Run only selected targets and their dependencies for this DAG."""
        return await self._engine.run_subgraph(self._dag_name, targets=targets, context=context)

    def validate(self) -> None:
        """Validate this DAG definition without executing it."""
        self._engine.validate(self._dag_name)

    def display(self) -> str:
        """Render this DAG as an ASCII tree."""
        return self._engine.display_dag(self._dag_name)

    def list_tasks(self) -> list[str]:
        """List tasks in topological order for this DAG."""
        return self._engine.list_tasks(self._dag_name)


class Engine:
    """Orchestrates DAG execution by coordinating the task registry, state store, and scheduler.

    Attributes
    ----------
    _registry : TaskRegistry
        Registry containing available tasks.
    _state : StateStore
        Storage for run and task state.
    _scheduler : Scheduler
        Scheduler responsible for executing DAG runs.
    _dag_builder : DAGBuilder
        DAG builder used to construct DAG definitions.
    """

    def __init__(
        self,
        registry: TaskRegistry,
        state_store: StateStoreProtocol,
        scheduler: Scheduler,
        *,
        _owns_pool: concurrent.futures.Executor | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the Engine with a task registry, state store, and scheduler.

        Parameters
        ----------
        registry : TaskRegistry
            Registry containing available tasks.
        state_store : StateStore
            Storage for run and task state.
        scheduler : Scheduler
            Scheduler responsible for executing DAG runs.
        _owns_pool : concurrent.futures.Executor | None
            When set, the engine takes ownership of this executor and will
            shut it down on ``close()`` / ``__aexit__``.
        logger : logging.Logger | None
            Optional logger instance. Falls back to ``logging.getLogger('flowrun')``.
        """
        self._registry = registry
        self._state = state_store
        self._scheduler = scheduler
        self._dag_builder = DAGBuilder(registry)
        self._owns_pool = _owns_pool
        self._closed = False
        self._log = logger or _default_logger

    # ---- async context-manager ----

    async def __aenter__(self) -> Self:
        """Enter the async context, returning the engine."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the async context, shutting down the owned thread pool if any."""
        self.close()

    def close(self) -> None:
        """Shut down the owned thread pool (if any). Safe to call multiple times."""
        if self._closed:
            return
        self._closed = True
        if self._owns_pool is not None:
            self._log.debug("Shutting down owned thread-pool")
            self._owns_pool.shutdown(wait=False)

    async def run_once(self, dag_name: str, context: RunContext[Any] | None = None) -> str:
        """Run a DAG once using the configured scheduler and return the run id.

        Parameters
        ----------
        dag_name : str
            Name of the DAG to execute.
        context : RunContext[Any] | None, optional
            Optional run context to pass to the scheduler, by default None.

        Returns
        -------
        str
            The identifier for the started run.
        """
        dag = self._dag_builder.build(dag_name=dag_name)
        self._log.info("Starting DAG %r", dag_name)
        run_id = await self._scheduler.run_dag_once(dag, context)
        self._log.info("Finished DAG %r  run_id=%s", dag_name, run_id)
        return run_id

    async def resume(
        self,
        run_id: str,
        *,
        from_tasks: list[str] | None = None,
        context: RunContext[Any] | None = None,
    ) -> str:
        """Resume a previous run, re-executing only what is needed.

        Tasks that were ``SUCCESS`` in the original run are preserved (their
        results are copied into a new run record).  Everything else — FAILED,
        SKIPPED, PENDING — becomes PENDING and will be re-scheduled.

        Parameters
        ----------
        run_id : str
            Identifier of the previous (typically failed) run to resume from.
        from_tasks : list[str] | None
            When provided, these tasks **and** all their downstream dependents
            will be forced back to PENDING even if they previously succeeded.
            Useful for "re-run from task X" semantics.
        context : RunContext[Any] | None
            Optional run context forwarded to task functions.

        Returns
        -------
        str
            The run id of the new (resumed) run.
        """
        prev = self._state.get_run(run_id)
        dag = self._dag_builder.build(dag_name=prev.dag_name)

        # Compute which tasks must be reset even if they were SUCCESS.
        reset_tasks: set[str] = set()
        if from_tasks:
            reset_tasks = dag.descendants_of(set(from_tasks))

        new_run_id = str(uuid.uuid4())
        self._state.create_resumed_run(
            run_id=new_run_id,
            prev_run_id=run_id,
            dag_name=prev.dag_name,
            task_names=dag.nodes,
            reset_tasks=reset_tasks,
        )

        self._log.info(
            "Resuming DAG %r from run %s  new_run_id=%s  reset=%s",
            prev.dag_name,
            run_id,
            new_run_id,
            sorted(reset_tasks) if reset_tasks else "(failed/skipped only)",
        )
        await self._scheduler.run_dag_once(dag, context, run_id=new_run_id)
        self._log.info("Finished resumed DAG %r  run_id=%s", prev.dag_name, new_run_id)
        return new_run_id

    async def run_subgraph(
        self,
        dag_name: str,
        targets: list[str],
        context: RunContext[Any] | None = None,
    ) -> str:
        """Run only the named targets and their transitive dependencies.

        Parameters
        ----------
        dag_name : str
            Name of the full DAG to build.
        targets : list[str]
            Task names to run.  All upstream dependencies are included
            automatically.
        context : RunContext[Any] | None
            Optional run context forwarded to task functions.

        Returns
        -------
        str
            The run id of the sub-graph execution.
        """
        full_dag = self._dag_builder.build(dag_name=dag_name)
        sub_dag = full_dag.subgraph(targets)
        self._log.info(
            "Starting sub-DAG %r  targets=%s  nodes=%s",
            dag_name,
            targets,
            sub_dag.nodes,
        )
        run_id = await self._scheduler.run_dag_once(sub_dag, context)
        self._log.info("Finished sub-DAG %r  run_id=%s", dag_name, run_id)
        return run_id

    def display_dag(self, dag_name: str) -> str:
        """
        Render and print the DAG as a tree in a folder-like ASCII layout.

        Parameters
        ----------
        dag_name : str
            Name of the DAG to render.

        Returns
        -------
        str
            The ASCII tree representation (also printed to stdout).
        """
        dag = self._dag_builder.build(dag_name=dag_name)

        # Build adjacency mapping from a task to the tasks that depend on it.
        dependents: dict[str, list[str]] = {node: [] for node in dag.nodes}
        for child, parents in dag.edges.items():
            for parent in parents:
                dependents.setdefault(parent, []).append(child)

        for children in dependents.values():
            children.sort()

        roots = [node for node in dag.nodes if not dag.parents_of(node)]
        if not roots:
            roots = list(dag.nodes)

        lines = [dag.name]
        visited: set[str] = set()

        def visit(node: str, prefix: str, is_last: bool) -> None:
            connector = "\\-- " if is_last else "|-- "
            already_seen = node in visited
            label = f"{node} [shared]" if already_seen else node
            lines.append(f"{prefix}{connector}{label}")
            if already_seen:
                return

            visited.add(node)
            children = dependents.get(node, [])
            if not children:
                return

            child_prefix = f"{prefix}{'    ' if is_last else '|   '}"
            for index, child in enumerate(children):
                visit(child, child_prefix, index == len(children) - 1)

        for index, root in enumerate(roots):
            visit(root, "", index == len(roots) - 1)

        return "\n".join(lines)

    def validate(self, dag_name: str) -> None:
        """Validate a DAG definition without running it."""
        self._dag_builder.build(dag_name=dag_name)

    def list_dags(self) -> list[str]:
        """List explicitly registered DAG namespaces."""
        return self._dag_builder.dag_names()

    def list_tasks(self, dag_name: str) -> list[str]:
        """List tasks in topological order for the given DAG."""
        dag = self._dag_builder.build(dag_name=dag_name)
        return list(dag.nodes)

    def dag(self, dag_name: str) -> DagScope:
        """Return a DAG-scoped facade to avoid repeating ``dag=...``."""
        return DagScope(self, dag_name)

    def get_run_report(self, run_id: str) -> dict[str, Any]:
        """
        Small helper for inspection / UI layer.
        """
        rec = self._state.get_run(run_id)
        run_status = self._compute_run_status(rec)
        return {
            "run_id": rec.run_id,
            "dag_name": rec.dag_name,
            "created_at": rec.created_at,
            "finished_at": rec.finished_at,
            "status": run_status,
            "tasks": {
                tname: {
                    "status": tr.status,
                    "attempt": tr.attempt,
                    "started_at": tr.started_at,
                    "finished_at": tr.finished_at,
                    "error": tr.error,
                    "result": tr.result,
                }
                for tname, tr in rec.tasks.items()
            },
        }

    @staticmethod
    def _compute_run_status(rec: RunRecord) -> str:
        statuses = [task.status for task in rec.tasks.values()]
        if any(status in ("FAILED", "SKIPPED") for status in statuses):
            return "FAILED"
        if statuses and all(status == "SUCCESS" for status in statuses):
            return "SUCCESS"
        return "RUNNING"

    @property
    def registry(self) -> TaskRegistry:
        """Expose the task registry used to build DAGs."""
        return self._registry

    @property
    def scheduler(self) -> Scheduler:
        """Expose the scheduler coordinating task execution."""
        return self._scheduler

    @property
    def task_executor(self) -> TaskExecutor:
        """Shortcut to the task executor backing the scheduler."""
        return self._scheduler.executor

    def task(
        self,
        name: str | None = None,
        deps: Sequence[str | Callable[..., Any]] | None = None,
        timeout_s: float | None = 30.0,
        retries: int = 0,
        retain_result: bool = True,
        dag: str | None = None,
    ):
        """Return a ``@task`` decorator bound to this engine's registry."""
        return task_decorator(
            name=name,
            deps=deps,
            timeout_s=timeout_s,
            retries=retries,
            retain_result=retain_result,
            dag=dag,
            registry=self._registry,
        )

    def task_template(
        self,
        func: Callable[..., Any],
        *,
        deps: Sequence[str | Callable[..., Any]] | None = None,
        timeout_s: float | None = 30.0,
        dag: str | None = None,
    ):
        """Create a task template bound to this engine's registry."""
        return task_template_factory(
            func,
            deps=deps,
            timeout_s=timeout_s,
            dag=dag,
            registry=self._registry,
        )


def build_default_engine(
    *,
    executor: concurrent.futures.Executor | None = None,
    max_workers: int = 8,
    max_parallel: int = 4,
    logger: logging.Logger | None = None,
    hooks: list[RunHook] | None = None,
    state_store: StateStoreProtocol | None = None,
) -> Engine:
    """Convenience constructor that wires up all components into a ready-to-use Engine.

    Parameters
    ----------
    executor : concurrent.futures.Executor | None
        Optional executor to run synchronous tasks. When provided, the caller
        remains responsible for its lifecycle. When omitted, a
        ThreadPoolExecutor is created using ``max_workers`` and owned by the
        returned engine (shut down on ``engine.close()`` / ``async with``).
    max_workers : int
        Worker count for the auto-created ThreadPoolExecutor when ``executor``
        is not provided.
    max_parallel : int
        Maximum number of tasks the scheduler may run concurrently.
    logger : logging.Logger | None
        Optional logger instance injected into all components. Falls back to
        ``logging.getLogger('flowrun')``.
    hooks : list[RunHook] | None
        Optional lifecycle hooks forwarded to the scheduler.
    state_store : StateStoreProtocol | None
        Optional persistent state store (e.g. ``SqliteStateStore``).  When
        omitted an in-memory ``StateStore`` is used.
    """
    log = logger or _default_logger
    registry = TaskRegistry()
    actual_store: StateStoreProtocol = state_store if state_store is not None else StateStore()
    owns_pool: concurrent.futures.Executor | None = None
    if executor is None:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        owns_pool = pool
    else:
        pool = executor
    task_executor = TaskExecutor(executor=pool, logger=log)
    scheduler = Scheduler(
        registry=registry,
        state_store=actual_store,
        executor=task_executor,
        config=SchedulerConfig(max_parallel=max_parallel),
        logger=log,
        hooks=hooks,
    )
    engine = Engine(
        registry=registry,
        state_store=actual_store,
        scheduler=scheduler,
        _owns_pool=owns_pool,
        logger=log,
    )
    return engine
