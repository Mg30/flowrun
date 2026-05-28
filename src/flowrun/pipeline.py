import inspect
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Mapping
from typing import Any

from flowrun.context import RunContext
from flowrun.dag import DAG
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
    """Executable snapshot of a built DAG with optional task overrides."""

    def __init__(self, engine: Any, dag: DAG, registry: TaskRegistry) -> None:
        """Create a pipeline bound to a built DAG and task registry snapshot."""
        self._engine = engine
        self._dag = dag
        self._registry = registry

    @property
    def name(self) -> str:
        """Return the DAG name for this pipeline."""
        return self._dag.name

    @property
    def tasks(self) -> tuple[str, ...]:
        """Return task names in topological order."""
        return tuple(self._dag.nodes)

    @property
    def dependencies(self) -> dict[str, tuple[str, ...]]:
        """Return a copy of the task dependency map."""
        return {task_name: tuple(self._dag.edges.get(task_name, ())) for task_name in self._dag.nodes}

    async def run_once(self, context: RunContext[Any] | None = None) -> str:
        """Run this pipeline once and return the run id."""
        return await self._engine._run_built_dag(self._dag, context=context, registry=self._registry)

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

    def subgraph(self, targets: list[str]) -> "Pipeline":
        """Return a pipeline containing selected target tasks and their dependencies."""
        sub_dag = self._dag.subgraph(targets)
        return Pipeline(
            self._engine,
            sub_dag,
            self._engine._copy_registry_for_nodes(sub_dag.nodes, self._registry, dag_name=self._dag.name),
        )

    def validate(self) -> None:
        """Validate compatibility helper; built pipelines are already validated."""
        return None

    def display(self) -> str:
        """Render this pipeline's DAG as an ASCII tree."""
        return self._engine._render_dag(self._dag)

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

        available = set(self._dag.nodes)
        unknown = sorted(name for name in merged if name not in available)
        if unknown:
            raise ValueError(
                f"Cannot override unknown pipeline tasks: {', '.join(unknown)}. "
                f"Available tasks: {', '.join(self._dag.nodes)}."
            )

        registry = TaskRegistry()
        for task_name in self._dag.nodes:
            spec = self._registry.get(task_name, self._dag.name)
            registry.register(_override_task_spec(spec, merged[task_name]) if task_name in merged else spec)

        return Pipeline(self._engine, self._dag, registry)
