import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar, get_args, get_origin

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
        Timeout in seconds for task execution, or None for no timeout.
    accepts_context : bool
        True when the task function signature allows a positional RunContext argument.
    requires_context : bool
        True when the task function signature requires a positional RunContext argument.
    accepts_upstream : bool
        True when the task function signature includes an ``upstream`` parameter to
        receive dependency results as a mapping.

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
    accepts_context: bool = False
    requires_context: bool = False
    accepts_upstream: bool = False

    def is_async(self) -> bool:
        """Return True if the registered task function is an async coroutine function.

        This inspects the stored callable and returns True when it was defined with
        'async def', otherwise returns False.
        """
        return inspect.iscoroutinefunction(self.func)


class TaskRegistry:
    """
    Global-ish registry, but can be instantiated and injected.
    """

    _default_registry: ClassVar["TaskRegistry | None"] = None

    def __init__(self) -> None:
        """Initialize an empty task registry."""
        self._tasks: dict[str, TaskSpec] = {}

    @classmethod
    def set_default(cls, registry: "TaskRegistry") -> None:
        """Configure the global fallback registry used by @task."""
        cls._default_registry = registry

    @classmethod
    def get_default(cls) -> "TaskRegistry":
        """Return the default registry configured via set_default()."""
        if cls._default_registry is None:
            raise ValueError(
                "No default TaskRegistry set; pass registry=... to @task or call TaskRegistry.set_default()"
            )
        return cls._default_registry

    def as_default(self) -> None:
        """Mark this instance as the default registry for @task usage."""
        self.set_default(self)

    def register(self, spec: TaskSpec) -> None:
        """Store a task specification by its unique name."""
        if spec.name in self._tasks:
            raise ValueError(f"Duplicate task name {spec.name}")
        self._tasks[spec.name] = spec

    def get(self, name: str) -> TaskSpec:
        """Fetch a previously registered task specification."""
        return self._tasks[name]

    def all(self) -> dict[str, TaskSpec]:
        """Return a shallow copy of the registered tasks."""
        return dict(self._tasks)


def task(
    name: str | None = None,
    deps: Sequence[str | Callable[..., Any]] | None = None,
    timeout_s: float | None = 30.0,
    registry: TaskRegistry | None = None,
):
    """
    Decorator for end users:
    - declares a task
    - records metadata in the provided registry
    Execution mode is inferred automatically (async callables run directly,
    sync callables run in a thread pool).
    The registry must be passed explicitly -> promotes DI.
    Tasks may declare an ``upstream`` parameter to receive dependency results as
    a ``dict[str, Any]`` mapping while keeping signatures explicit.
    """
    if registry is None:
        registry = TaskRegistry.get_default()

    def _normalize_deps(raw_deps: Sequence[str | Callable[..., Any]] | None) -> list[str]:
        normalised: list[str] = []
        if not raw_deps:
            return normalised

        for dep in raw_deps:
            if isinstance(dep, str):
                normalised.append(dep)
                continue

            if callable(dep):
                task_name = getattr(dep, "__flowrun_task_name__", dep.__name__)
                if not isinstance(task_name, str):
                    raise TypeError("Task dependency callable must expose a string task name")
                normalised.append(task_name)
                continue

            raise TypeError("deps entries must be task names or task callables")

        return normalised

    def wrapper(func: Callable[..., Any]):
        def _annotation_is_run_context(annotation: Any) -> bool:
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

        def _looks_like_context(param: inspect.Parameter) -> bool:
            if _annotation_is_run_context(param.annotation):
                return True
            return param.name in {"ctx", "context"}

        def _context_signature_flags(callable_obj: Callable[..., Any]) -> tuple[bool, bool]:
            sig = inspect.signature(callable_obj)
            for param in sig.parameters.values():
                if param.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ):
                    if param.name in {"self", "cls"}:
                        continue
                    if _looks_like_context(param):
                        return True, param.default is inspect._empty
            return False, False

        def _accepts_upstream(callable_obj: Callable[..., Any]) -> bool:
            sig = inspect.signature(callable_obj)
            for param in sig.parameters.values():
                if param.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                ):
                    if param.name == "upstream":
                        return True
            return False

        accepts_context, requires_context = _context_signature_flags(func)
        accepts_upstream = _accepts_upstream(func)
        spec = TaskSpec(
            name=name or func.__name__,
            func=func,
            deps=_normalize_deps(deps),
            timeout_s=timeout_s,
            accepts_context=accepts_context,
            requires_context=requires_context,
            accepts_upstream=accepts_upstream,
        )
        registry.register(spec)
        func.__flowrun_task_name__ = spec.name  # type: ignore[attr-defined]
        return func

    return wrapper
