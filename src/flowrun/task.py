import inspect
import types
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from flowrun.context import RunContext


@dataclass(frozen=True)
class TaskSpec:
    """Specification for a task.

    Attributes
    ----------
    name : str
        Unique name of the task.
    func : Callable[..., Any]
        The callable that implements the task.
    deps : list[str]
        List of task names this task depends on.
    timeout_s : float | None
        Timeout in seconds for async task execution, or None for no timeout.
    accepts_context : bool
        True when the task function signature allows a RunContext argument.
    requires_context : bool
        True when the task function signature requires a RunContext argument.
    accepts_upstream : bool
        True when the task function signature includes an ``upstream`` parameter to
        receive dependency results as a mapping.
    named_deps : list[str]
        Subset of ``deps`` that the task function accepts as explicit parameters.
        When non-empty and the function does not declare an ``upstream`` parameter,
        the executor may pass dependency results as keyword arguments (e.g.,
        ``def consume(fetch_user, fetch_settings): ...``).

    Methods
    -------
    is_async() -> bool
        Return True if the registered function is an async coroutine function.
        Execution mode is inferred automatically when running the task.
    """

    name: str
    func: Callable[..., Any]
    deps: list[str] = field(default_factory=list)
    timeout_s: float | None = None
    retries: int = 0
    dag: str | None = None
    accepts_context: bool = False
    requires_context: bool = False
    context_param_name: str | None = None
    context_positional_only: bool = False
    accepts_upstream: bool = False
    named_deps: list[str] = field(default_factory=list)

    def is_async(self) -> bool:
        """Return True if the registered task function is an async coroutine function.

        This inspects the stored callable and returns True when it was defined with
        'async def', otherwise returns False.
        """
        return inspect.iscoroutinefunction(self.func)


class TaskRegistry:
    """Registry that maps task names to `TaskSpec` objects.

    Supports the standard collection protocol (`in`, `len`, iteration,
    subscript).
    """

    def __init__(self) -> None:
        """Initialize an empty task registry."""
        self._tasks: dict[tuple[str | None, str], TaskSpec] = {}

    # ---- collection protocol ----

    def register(self, spec: TaskSpec) -> None:
        """Store a task specification by its unique name.

        Raises
        ------
        ValueError
            If a task with the same name is already registered in the same DAG namespace.
        """
        _validate_task_spec(spec)
        key = (spec.dag, spec.name)
        if key in self._tasks:
            raise ValueError(
                f"Duplicate task name {spec.name!r} in DAG {spec.dag!r}. "
                "Task names must be unique within a DAG namespace."
            )
        self._tasks[key] = spec

    def get(self, name: str, dag: str | None = None) -> TaskSpec:
        """Fetch a previously registered task specification.

        Raises
        ------
        KeyError
            If no task with the given name is registered.
        """
        if dag is not None:
            if (dag, name) in self._tasks:
                return self._tasks[(dag, name)]
            if (None, name) in self._tasks:
                return self._tasks[(None, name)]
            raise KeyError(f"Task {name!r} is not registered in DAG {dag!r}") from None

        matches = [spec for (task_dag, task_name), spec in self._tasks.items() if task_name == name]
        if not matches:
            raise KeyError(f"Task {name!r} is not registered") from None
        if len(matches) > 1:
            dags = ", ".join(repr(spec.dag) for spec in matches)
            raise KeyError(f"Task {name!r} is ambiguous across DAGs: {dags}. Pass dag=... to disambiguate.")
        return matches[0]

    def contains(self, name: str, dag: str | None = None) -> bool:
        """Return True when *name* exists, optionally inside *dag*."""
        if dag is not None:
            return (dag, name) in self._tasks or (None, name) in self._tasks
        return any(task_name == name for _task_dag, task_name in self._tasks)

    def __contains__(self, name: object) -> bool:
        """Check whether a task name is registered."""
        return isinstance(name, str) and self.contains(name)

    def __len__(self) -> int:
        """Return the number of registered tasks."""
        return len(self._tasks)

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered task names."""
        return (name for _dag, name in self._tasks)

    def __getitem__(self, name: str) -> TaskSpec:
        """Subscript access, delegates to `get()`."""
        return self.get(name)

    @property
    def task_specs(self) -> Mapping[str, TaskSpec]:
        """Read-only view of registered task specifications.

        Globally unique task names use their plain name. Names repeated across
        DAGs use ``dag:name`` keys so every task remains visible.
        """
        counts = Counter(spec.name for spec in self._tasks.values())
        visible: dict[str, TaskSpec] = {}
        for spec in self._tasks.values():
            key = spec.name if counts[spec.name] == 1 else f"{spec.dag}:{spec.name}"
            visible[key] = spec
        return types.MappingProxyType(visible)

    @property
    def specs(self) -> tuple[TaskSpec, ...]:
        """Return all task specs without changing their names."""
        return tuple(self._tasks.values())

    def clear(self) -> None:
        """Remove all registered tasks. Primarily intended for testing."""
        self._tasks.clear()

    def __repr__(self) -> str:
        """Return a human-readable representation of the registry."""
        names = ", ".join(spec.name if spec.dag is None else f"{spec.dag}:{spec.name}" for spec in self._tasks.values())
        return f"TaskRegistry([{names}])"


def _normalize_deps(raw_deps: Sequence[str | Callable[..., Any]] | None) -> list[str]:
    """Resolve dependency entries (strings or decorated callables) to task name strings."""
    normalised: list[str] = []
    if not raw_deps:
        return normalised

    for dep in raw_deps:
        if isinstance(dep, str):
            normalised.append(dep)
        elif callable(dep):
            task_name = getattr(dep, "__flowrun_task_name__", None)
            if task_name is None:
                task_name = getattr(dep, "__name__", None)
            if not isinstance(task_name, str):
                raise TypeError("Task dependency callable must expose a string task name")
            normalised.append(task_name)
        else:
            raise TypeError("deps entries must be task names or task callables")

    return normalised


def _annotation_is_run_context(annotation: Any) -> bool:
    """Return True when *annotation* resolves to `RunContext` (possibly generic/annotated)."""
    if annotation is inspect._empty:
        return False
    if annotation is RunContext:
        return True
    origin = get_origin(annotation)
    if origin is RunContext:
        return True
    if origin is Annotated:
        args = get_args(annotation)
        if args:
            return _annotation_is_run_context(args[0])
    return False


def _type_hints(callable_obj: Callable[..., Any]) -> dict[str, Any]:
    """Resolve annotations when possible, including postponed annotations."""
    try:
        return get_type_hints(callable_obj, include_extras=True)
    except Exception:
        return {}


def _param_annotation(param: inspect.Parameter, hints: Mapping[str, Any]) -> Any:
    return hints.get(param.name, param.annotation)


def _context_signature_flags(callable_obj: Callable[..., Any]) -> tuple[bool, bool, str | None, bool]:
    """Inspect a callable and return context injection details.

    Detection relies **only** on type annotations — parameter names are not
    considered, avoiding false positives.
    """
    sig = inspect.signature(callable_obj)
    hints = _type_hints(callable_obj)
    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            if param.name in {"self", "cls"}:
                continue
            if _annotation_is_run_context(_param_annotation(param, hints)):
                return (
                    True,
                    param.default is inspect._empty,
                    param.name,
                    param.kind is inspect.Parameter.POSITIONAL_ONLY,
                )
    return False, False, None, False


def _accepts_upstream(callable_obj: Callable[..., Any]) -> bool:
    """Return True when *callable_obj* declares an ``upstream`` parameter."""
    sig = inspect.signature(callable_obj)
    return any(
        param.name == "upstream"
        for param in sig.parameters.values()
        if param.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    )


def _infer_required_dep_names(
    callable_obj: Callable[..., Any], registry: TaskRegistry, dag: str | None = None
) -> list[str]:
    """Infer dependency names from required parameters that match registered tasks."""
    sig = inspect.signature(callable_obj)
    hints = _type_hints(callable_obj)
    inferred: list[str] = []
    for param in sig.parameters.values():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if param.name in {"self", "cls"}:
            continue
        if param.default is not inspect._empty:
            continue
        if _annotation_is_run_context(_param_annotation(param, hints)):
            continue
        if param.name == "upstream":
            continue
        if registry.contains(param.name, dag=dag):
            inferred.append(param.name)
    return inferred


def _accepted_named_deps(callable_obj: Callable[..., Any], dep_names: list[str]) -> list[str]:
    """Return the subset of *dep_names* that appear as parameter names in *callable_obj*."""
    sig = inspect.signature(callable_obj)
    return [
        param.name
        for param in sig.parameters.values()
        if param.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
        and param.name in dep_names
    ]


def _unsatisfied_required_params(callable_obj: Callable[..., Any], dep_names: Sequence[str]) -> list[str]:
    """Return required parameters that flowrun cannot satisfy for *callable_obj*."""
    sig = inspect.signature(callable_obj)
    hints = _type_hints(callable_obj)
    unsatisfied: list[str] = []
    for param in sig.parameters.values():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if param.name in {"self", "cls"}:
            continue
        if param.default is not inspect._empty:
            continue
        if _annotation_is_run_context(_param_annotation(param, hints)):
            continue
        if param.name == "upstream":
            continue
        if param.name in dep_names:
            continue
        unsatisfied.append(param.name)
    return unsatisfied


def _validate_task_spec(spec: TaskSpec) -> None:
    """Reject task configurations that would fail late or behave unsafely."""
    if spec.timeout_s is not None and not spec.is_async():
        raise ValueError(
            f"Task {spec.name!r} is synchronous and cannot use timeout_s. "
            "Thread-based timeouts cannot safely stop blocking work. "
            "Use an async task or configure timeouts in the client you call inside the task."
        )

    unsatisfied = _unsatisfied_required_params(spec.func, spec.deps)
    if unsatisfied:
        deps_display = ", ".join(spec.deps) if spec.deps else "(none)"
        raise ValueError(
            f"Task {spec.name!r} has required parameters that flowrun cannot provide: {', '.join(unsatisfied)}. "
            f"Available dependency names: {deps_display}. "
            "Required parameters must either be annotated as RunContext, named 'upstream', "
            "or exactly match a dependency name. When deps is omitted, flowrun only infers already-registered "
            "task names from required parameters. If you use dependency names that are not valid Python "
            "identifiers, consume them through the upstream mapping or rename the task."
        )


def task(
    _func: Callable[..., Any] | str | None = None,
    *,
    name: str | None = None,
    deps: Sequence[str | Callable[..., Any]] | None = None,
    timeout_s: float | None = None,
    retries: int = 0,
    dag: str | None = None,
    registry: TaskRegistry | None = None,
):
    """Decorator that declares a task and registers it.

    Parameters
    ----------
    name : str | None
        Optional explicit name; defaults to ``func.__name__``.
    deps : Sequence[str | Callable] | None
        Task dependencies (names or previously-decorated callables). When omitted,
        required parameter names that match already-registered task names are inferred.
    timeout_s : float | None
        Per-attempt timeout for async tasks, or ``None`` for no timeout.
    retries : int
        Number of times to retry on failure (0 = no retries).
    dag : str | None
        Optional DAG namespace used internally when a pipeline registers tasks.
    registry : TaskRegistry | None
        Registry to register with. Required when using ``task(...)`` directly.
    """
    if registry is None:
        raise TypeError("task(...): registry= is required. Prefer pipeline.task(...).")

    def wrapper(func: Callable[..., Any]):
        dep_names = _normalize_deps(deps) if deps is not None else _infer_required_dep_names(func, registry, dag=dag)
        ctx_accepts, ctx_requires, ctx_name, ctx_positional_only = _context_signature_flags(func)
        has_upstream = _accepts_upstream(func)
        named = [] if has_upstream else _accepted_named_deps(func, dep_names)

        spec = TaskSpec(
            name=name or func.__name__,
            func=func,
            deps=dep_names,
            timeout_s=timeout_s,
            retries=retries,
            dag=dag,
            accepts_context=ctx_accepts,
            requires_context=ctx_requires,
            context_param_name=ctx_name,
            context_positional_only=ctx_positional_only,
            accepts_upstream=has_upstream,
            named_deps=named,
        )
        registry.register(spec)
        func.__flowrun_task_name__ = spec.name  # type: ignore[attr-defined]
        return func

    if isinstance(_func, str):
        if name is not None:
            raise TypeError("Task name provided both positionally and via 'name='.")
        name = _func
        return wrapper

    if _func is None:
        return wrapper
    return wrapper(_func)
