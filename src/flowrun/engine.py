import concurrent.futures
from typing import Any

from flowrun.context import RunContext
from flowrun.dag import DAGBuilder
from flowrun.executor import TaskExecutor
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import StateStore
from flowrun.task import TaskRegistry


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
        state_store: StateStore,
        scheduler: Scheduler,
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
        """
        self._registry = registry
        self._state = state_store
        self._scheduler = scheduler
        self._dag_builder = DAGBuilder(registry)

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
        run_id = await self._scheduler.run_dag_once(dag, context)
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

        tree = "\n".join(lines)
        print(tree)
        return tree

    def get_run_report(self, run_id: str) -> dict[str, Any]:
        """
        Small helper for inspection / UI layer.
        """
        rec = self._state.get_run(run_id)
        return {
            "run_id": rec.run_id,
            "dag_name": rec.dag_name,
            "created_at": rec.created_at,
            "finished_at": rec.finished_at,
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


def build_default_engine(
    *,
    executor: concurrent.futures.Executor | None = None,
    max_workers: int = 8,
) -> Engine:
    """
    Convenience constructor that wires up:
    - TaskRegistry (provided by caller later via returned engine? no, we return a tuple-like pattern)
    We'll expose registry separately because user code needs it for @task.

    Parameters
    ----------
    executor : concurrent.futures.Executor | None
        Optional executor to run synchronous tasks. When provided, the caller
        remains responsible for its lifecycle. When omitted, a
        ThreadPoolExecutor is created using ``max_workers``.
    max_workers : int
        Worker count for the auto-created ThreadPoolExecutor when ``executor``
        is not provided.
    """
    registry = TaskRegistry()
    registry.as_default()
    state_store = StateStore()
    pool = executor or concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    task_executor = TaskExecutor(executor=pool)
    scheduler = Scheduler(
        registry=registry,
        state_store=state_store,
        executor=task_executor,
        config=SchedulerConfig(max_parallel=4),
    )
    engine = Engine(
        registry=registry,
        state_store=state_store,
        scheduler=scheduler,
    )
    return engine
