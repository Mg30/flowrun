import contextvars
import functools
import inspect
import types
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Annotated, Any, get_args, get_origin

from flowrun.context import RunContext

_active_registry: contextvars.ContextVar["TaskRegistry | None"] = contextvars.ContextVar(
    "flowrun_active_registry",
    default=None,
)


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
        Timeout in seconds for task execution, or None for no timeout.
    accepts_context : bool
        True when the task function signature allows a positional RunContext argument.
    requires_context : bool
        True when the task function signature requires a positional RunContext argument.
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
    timeout_s: float | None = 30.0
    retries: int = 0
    retain_result: bool = True
    dag: str | None = None
    accepts_context: bool = False
    requires_context: bool = False
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
    subscript) and a `contextvars`-based activation model that is safe
    across async tasks and threads.
    """

    def __init__(self) -> None:
        """Initialize an empty task registry."""
        self._tasks: dict[str, TaskSpec] = {}

    # ---- active-registry management (contextvars, async-safe) ----

    @staticmethod
    def active() -> "TaskRegistry":
        """Return the currently active registry.

        Raises
        ------
        LookupError
            If no registry has been activated.
        """
        reg = _active_registry.get()
        if reg is None:
            raise LookupError(
                "No active TaskRegistry; pass registry=... to @task or activate one with `registry.activate()`."
            )
        return reg

    def activate(self) -> contextvars.Token["TaskRegistry | None"]:
        """Make this registry the active one and return a reset token.

        The token can be passed to `deactivate()` to restore the previous
        registry, or used directly with `contextvars.ContextVar.reset`.
        """
        return _active_registry.set(self)

    @staticmethod
    def deactivate(token: contextvars.Token["TaskRegistry | None"]) -> None:
        """Restore the previous active registry from a token."""
        _active_registry.reset(token)

    # ---- collection protocol ----

    def register(self, spec: TaskSpec) -> None:
        """Store a task specification by its unique name.

        Raises
        ------
        ValueError
            If a task with the same name is already registered.
        """
        if spec.name in self._tasks:
            raise ValueError(f"Duplicate task name: {spec.name!r}")
        self._tasks[spec.name] = spec

    def get(self, name: str) -> TaskSpec:
        """Fetch a previously registered task specification.

        Raises
        ------
        KeyError
            If no task with the given name is registered.
        """
        try:
            return self._tasks[name]
        except KeyError:
            raise KeyError(f"Task {name!r} is not registered") from None

    def __contains__(self, name: object) -> bool:
        """Check whether a task name is registered."""
        return name in self._tasks

    def __len__(self) -> int:
        """Return the number of registered tasks."""
        return len(self._tasks)

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered task names."""
        return iter(self._tasks)

    def __getitem__(self, name: str) -> TaskSpec:
        """Subscript access, delegates to `get()`."""
        return self.get(name)

    @property
    def task_specs(self) -> Mapping[str, TaskSpec]:
        """Read-only view of all registered task specifications."""
        return types.MappingProxyType(self._tasks)

    def clear(self) -> None:
        """Remove all registered tasks. Primarily intended for testing."""
        self._tasks.clear()

    def __repr__(self) -> str:
        """Return a human-readable representation of the registry."""
        names = ", ".join(self._tasks)
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


def _context_signature_flags(callable_obj: Callable[..., Any]) -> tuple[bool, bool]:
    """Inspect a callable and return ``(accepts_context, requires_context)``.

    Detection relies **only** on type annotations — parameter names are not
    considered, avoiding false positives.
    """
    sig = inspect.signature(callable_obj)
    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            if param.name in {"self", "cls"}:
                continue
            if _annotation_is_run_context(param.annotation):
                return True, param.default is inspect._empty
    return False, False


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


def task(
    _func: Callable[..., Any] | str | None = None,
    *,
    name: str | None = None,
    deps: Sequence[str | Callable[..., Any]] | None = None,
    timeout_s: float | None = 30.0,
    retries: int = 0,
    retain_result: bool = True,
    dag: str | None = None,
    registry: TaskRegistry | None = None,
):
    """Decorator that declares a task and registers it.

    Parameters
    ----------
    name : str | None
        Optional explicit name; defaults to ``func.__name__``.
    deps : Sequence[str | Callable] | None
        Task dependencies (names or previously-decorated callables).
    timeout_s : float | None
        Timeout in seconds, or ``None`` for no timeout.
    retries : int
        Number of times to retry on failure (0 = no retries).
    retain_result : bool
        When False, the result is cleared from state once all downstream
        consumers have been launched, freeing memory for large payloads.
    dag : str | None
        Optional DAG namespace used by ``Engine.run_once(dag_name=...)`` to
        select only tasks belonging to that DAG.
    registry : TaskRegistry | None
        Registry to register with.  When omitted, falls back to
        ``TaskRegistry.active()``.
    """
    if registry is None:
        registry = TaskRegistry.active()

    def wrapper(func: Callable[..., Any]):
        dep_names = _normalize_deps(deps)
        ctx_accepts, ctx_requires = _context_signature_flags(func)
        has_upstream = _accepts_upstream(func)
        named = [] if has_upstream else _accepted_named_deps(func, dep_names)

        spec = TaskSpec(
            name=name or func.__name__,
            func=func,
            deps=dep_names,
            timeout_s=timeout_s,
            retries=retries,
            retain_result=retain_result,
            dag=dag,
            accepts_context=ctx_accepts,
            requires_context=ctx_requires,
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


@dataclass(frozen=True)
class TaskTemplate:
    """Reusable template for registering many parameterized tasks.

    This is intended for cases where you want to register the *same* task
    implementation multiple times under different names with some arguments
    pre-bound (e.g. one task per API endpoint).
    """

    func: Callable[..., Any]
    deps: Sequence[str | Callable[..., Any]] | None = None
    timeout_s: float | None = 30.0
    dag: str | None = None
    registry: TaskRegistry | None = None

    def bind(self, name: str, /, *args: Any, **kwargs: Any) -> Callable[..., Any]:
        """Register a new task by binding arguments into the template callable.

        Parameters
        ----------
        name : str
            Name for the registered task.
        *args, **kwargs
            Arguments to pre-bind into the underlying task callable.
            These are applied via ``functools.partial``.

        Returns
        -------
        Callable[..., Any]
            The bound callable that was registered.
        """
        bound = functools.partial(self.func, *args, **kwargs)
        # Reuse the existing @task decorator machinery so dependency normalization
        # and signature-based flags (context/upstream) stay consistent.
        return task(
            name=name,
            deps=self.deps,
            timeout_s=self.timeout_s,
            dag=self.dag,
            registry=self.registry,
        )(bound)


def task_template(
    func: Callable[..., Any],
    *,
    deps: Sequence[str | Callable[..., Any]] | None = None,
    timeout_s: float | None = 30.0,
    dag: str | None = None,
    registry: TaskRegistry | None = None,
) -> TaskTemplate:
    """Create a TaskTemplate for registering parameterized instances of a task."""
    return TaskTemplate(func=func, deps=deps, timeout_s=timeout_s, dag=dag, registry=registry)
