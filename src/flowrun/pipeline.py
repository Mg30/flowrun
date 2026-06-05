import concurrent.futures
import importlib
import inspect
import logging
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Mapping, Sequence
from types import TracebackType
from typing import Any, Self

from flowrun.context import RunContext
from flowrun.dag import DAG
from flowrun.hooks import RunHook
from flowrun.state import StateStore
from flowrun.task import (
    TaskRegistry,
    TaskSpec,
    _accepted_named_deps,
    _accepts_upstream,
    _context_signature_flags,
)


async def _iterate_contexts(
    contexts: AsyncIterable[RunContext[Any] | None] | Iterable[RunContext[Any] | None],
) -> AsyncIterator[RunContext[Any] | None]:
    if isinstance(contexts, AsyncIterable):
        async for context in contexts:
            yield context
        return

    for context in contexts:
        yield context


def _constant_override(value: Any) -> Callable[[], Any]:
    def _return_constant() -> Any:
        return value

    return _return_constant


def _override_task_spec(spec: TaskSpec, override: Callable[..., Any] | Any) -> TaskSpec:
    func = override if callable(override) else _constant_override(override)
    timeout_s = spec.timeout_s if inspect.iscoroutinefunction(func) else None

    accepts_context, requires_context, context_param_name, context_positional_only = _context_signature_flags(func)
    accepts_upstream = _accepts_upstream(func)
    named_deps = [] if accepts_upstream else _accepted_named_deps(func, spec.deps)

    return TaskSpec(
        name=spec.name,
        func=func,
        deps=list(spec.deps),
        timeout_s=timeout_s,
        retries=spec.retries,
        dag=spec.dag,
        accepts_context=accepts_context,
        requires_context=requires_context,
        context_param_name=context_param_name,
        context_positional_only=context_positional_only,
        accepts_upstream=accepts_upstream,
        named_deps=named_deps,
    )


class Pipeline:
    """Code-first DAG pipeline backed by an internal execution runtime."""

    def __init__(
        self,
        name: str,
        *,
        executor: concurrent.futures.Executor | None = None,
        max_workers: int = 8,
        max_parallel: int = 4,
        logger: logging.Logger | None = None,
        hooks: list[RunHook] | None = None,
        state_store: StateStore | None = None,
    ) -> None:
        """Create a pipeline and its internal runtime.

        Parameters mirror the runtime configuration previously passed to the
        engine constructor helper, but the engine itself is intentionally hidden
        from the happy-path API.
        """
        engine_module = importlib.import_module("flowrun.engine")
        self._engine = engine_module.build_default_engine(
            executor=executor,
            max_workers=max_workers,
            max_parallel=max_parallel,
            logger=logger,
            hooks=hooks,
            state_store=state_store,
        )
        self._dag_name = name
        self._dag: DAG | None = None
        self._registry: TaskRegistry | None = None

    @classmethod
    def _from_built(cls, engine: Any, dag: DAG, registry: TaskRegistry) -> "Pipeline":
        """Create a built pipeline snapshot for internal/backcompat callers."""
        pipeline = cls.__new__(cls)
        pipeline._engine = engine
        pipeline._dag = dag
        pipeline._registry = registry
        pipeline._dag_name = dag.name
        return pipeline

    async def __aenter__(self) -> Self:
        """Enter the pipeline runtime context."""
        await self._engine.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the pipeline runtime context, closing owned resources."""
        await self._engine.__aexit__(exc_type, exc_val, exc_tb)

    def close(self) -> None:
        """Close resources owned by this pipeline's runtime."""
        self._engine.close()

    def _current_dag(self) -> DAG:
        if self._dag is not None:
            return self._dag
        return self._engine._dag_builder.build(dag_name=self._dag_name)

    def _current_registry(self, dag: DAG) -> TaskRegistry:
        if self._registry is not None:
            return self._registry
        return self._engine._copy_registry_for_nodes(dag.nodes, dag_name=dag.name)

    @property
    def name(self) -> str:
        """Return the DAG name for this pipeline."""
        return self._dag_name

    @property
    def tasks(self) -> tuple[str, ...]:
        """Return task names in topological order."""
        return tuple(self._current_dag().nodes)

    @property
    def dependencies(self) -> dict[str, tuple[str, ...]]:
        """Return a copy of the task dependency map."""
        dag = self._current_dag()
        return {task_name: tuple(dag.edges.get(task_name, ())) for task_name in dag.nodes}

    def task(
        self,
        name: str | None = None,
        deps: Sequence[str | Callable[..., Any]] | None = None,
        timeout_s: float | None = None,
        retries: int = 0,
    ):
        """Return a ``@task`` decorator bound to this pipeline."""
        if self._registry is not None:
            raise TypeError("Cannot register tasks on a built pipeline snapshot.")
        return self._engine.task(
            name=name,
            deps=deps,
            timeout_s=timeout_s,
            retries=retries,
            dag=self._dag_name,
        )

    async def run_once(self, context: RunContext[Any] | None = None) -> str:
        """Run this pipeline once and return the run id."""
        dag = self._current_dag()
        return await self._engine._run_built_dag(dag, context=context, registry=self._current_registry(dag))

    async def run_many(
        self,
        contexts: AsyncIterable[RunContext[Any] | None] | Iterable[RunContext[Any] | None],
    ) -> list[str]:
        """Run this pipeline once per context, sequentially."""
        run_ids: list[str] = []
        async for context in _iterate_contexts(contexts):
            run_ids.append(await self.run_once(context=context))
        return run_ids

    async def run_subgraph(
        self,
        targets: list[str],
        context: RunContext[Any] | None = None,
    ) -> str:
        """Run selected target tasks and their dependencies."""
        return await self.subgraph(targets).run_once(context=context)

    async def resume(
        self,
        run_id: str,
        *,
        from_tasks: list[str] | None = None,
        context: RunContext[Any] | None = None,
    ) -> str:
        """Resume a previous run of this pipeline."""
        previous = self._engine._state.get_run(run_id)
        dag = self._current_dag()
        if previous.dag_name != dag.name:
            raise ValueError(f"Run {run_id!r} belongs to DAG {previous.dag_name!r}, not {dag.name!r}.")

        reset_tasks: set[str] = set()
        if from_tasks:
            reset_tasks = dag.descendants_of(set(from_tasks))

        new_run_id = str(uuid.uuid4())
        self._engine._state.create_resumed_run(
            run_id=new_run_id,
            prev_run_id=run_id,
            dag_name=previous.dag_name,
            task_names=dag.nodes,
            reset_tasks=reset_tasks,
            metadata=context.metadata if context is not None and context.metadata else previous.metadata,
        )

        self._engine._log.info(
            "Resuming DAG %r from run %s  new_run_id=%s  reset=%s",
            previous.dag_name,
            run_id,
            new_run_id,
            sorted(reset_tasks) if reset_tasks else "(failed/skipped only)",
        )
        await self._engine._run_built_dag(
            dag,
            context=context,
            registry=self._current_registry(dag),
            run_id=new_run_id,
        )
        self._engine._log.info("Finished resumed DAG %r  run_id=%s", previous.dag_name, new_run_id)
        return new_run_id

    def subgraph(self, targets: list[str]) -> "Pipeline":
        """Return a pipeline containing selected target tasks and their dependencies."""
        dag = self._current_dag()
        sub_dag = dag.subgraph(targets)
        return Pipeline._from_built(
            self._engine,
            sub_dag,
            self._engine._copy_registry_for_nodes(sub_dag.nodes, self._current_registry(dag), dag_name=dag.name),
        )

    def validate(self) -> None:
        """Validate this pipeline definition without executing it."""
        self._current_dag()
        return None

    def display(self) -> str:
        """Render this pipeline's DAG as an ASCII tree."""
        return self._engine._render_dag(self._current_dag())

    def list_tasks(self) -> list[str]:
        """List task names in topological order."""
        return list(self.tasks)

    def get_run_report(self, run_id: str) -> dict[str, Any]:
        """Return the run report for a run executed by this engine."""
        return self._engine.get_run_report(run_id)

    def override_tasks(
        self,
        overrides: Mapping[str, Callable[..., Any] | Any] | None = None,
        /,
        **named_overrides: Callable[..., Any] | Any,
    ) -> "Pipeline":
        """Return a new pipeline with selected task implementations replaced."""
        merged: dict[str, Callable[..., Any] | Any] = {}
        if overrides is not None:
            merged.update(overrides)
        if named_overrides:
            merged.update(named_overrides)
        if not merged:
            return self

        dag = self._current_dag()
        available = set(dag.nodes)
        unknown = sorted(name for name in merged if name not in available)
        if unknown:
            raise ValueError(
                f"Cannot override unknown pipeline tasks: {', '.join(unknown)}. "
                f"Available tasks: {', '.join(dag.nodes)}."
            )

        registry = TaskRegistry()
        source_registry = self._current_registry(dag)
        for task_name in dag.nodes:
            spec = source_registry.get(task_name, dag.name)
            registry.register(_override_task_spec(spec, merged[task_name]) if task_name in merged else spec)

        return Pipeline._from_built(self._engine, dag, registry)
