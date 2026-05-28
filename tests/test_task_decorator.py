import pytest

from flowrun.context import RunContext
from flowrun.task import TaskRegistry, task


def test_task_decorator_requires_explicit_registry():
    registry = TaskRegistry()

    with pytest.raises(TypeError, match="registry= is required"):

        @task
        def bare() -> int:
            return 1


def test_task_decorator_supports_positional_name():
    registry = TaskRegistry()

    @task("named", registry=registry)
    def sample() -> int:
        return 1

    spec = registry.get("named")
    assert spec.func is sample


def test_task_decorator_detects_required_context():
    registry = TaskRegistry()

    @task(name="needs_ctx", registry=registry)
    def needs_ctx(ctx: RunContext[dict[str, int]]):
        return ctx.deps["value"]

    spec = registry.get("needs_ctx")

    assert spec.accepts_context is True
    assert spec.requires_context is True
    assert spec.context_param_name == "ctx"


def test_task_decorator_detects_postponed_context_annotations():
    namespace = {"RunContext": RunContext, "TaskRegistry": TaskRegistry, "task": task}
    exec(
        """
from __future__ import annotations

registry = TaskRegistry()

@task(name="needs_ctx", registry=registry)
def needs_ctx(ctx: RunContext[dict[str, int]]):
    return ctx.deps["value"]
""",
        namespace,
    )

    spec = namespace["registry"].get("needs_ctx")

    assert spec.accepts_context is True
    assert spec.requires_context is True
    assert spec.context_param_name == "ctx"


def test_task_decorator_normalizes_callable_dependencies():
    registry = TaskRegistry()

    @task(name="producer", registry=registry)
    def producer() -> int:
        return 1

    @task(deps=[producer], registry=registry)
    def consumer() -> int:
        return 2

    spec = registry.get("consumer")

    assert spec.deps == ["producer"]
    assert registry.get("producer").deps == []


def test_task_decorator_infers_dependencies_from_required_parameter_names():
    registry = TaskRegistry()

    @task(name="producer", registry=registry)
    def producer() -> int:
        return 1

    @task(registry=registry)
    def consumer(producer: int) -> int:
        return producer + 1

    spec = registry.get("consumer")

    assert spec.deps == ["producer"]
    assert spec.named_deps == ["producer"]


def test_task_decorator_inference_requires_registered_dependency_names():
    registry = TaskRegistry()

    with pytest.raises(ValueError, match="already-registered task names"):

        @task(name="consumer", registry=registry)
        def consumer(producer: int) -> int:
            return producer


def test_task_decorator_rejects_unsatisfied_required_parameters():
    registry = TaskRegistry()

    with pytest.raises(ValueError, match="cannot provide"):

        @task(name="consumer", deps=["fetch-users"], registry=registry)
        def consumer(fetch_users: int) -> int:
            return fetch_users


def test_task_decorator_rejects_sync_timeouts():
    registry = TaskRegistry()

    with pytest.raises(ValueError, match="cannot use timeout_s"):

        @task(name="sync_task", timeout_s=1.0, registry=registry)
        def sync_task() -> int:
            return 1


def test_task_registry_allows_duplicate_task_names_in_different_dags():
    registry = TaskRegistry()

    @task(name="extract", dag="a", registry=registry)
    def extract_a() -> int:
        return 1

    @task(name="extract", dag="b", registry=registry)
    def extract_b() -> int:
        return 2

    assert registry.get("extract", "a").func is extract_a
    assert registry.get("extract", "b").func is extract_b


def test_task_registry_rejects_duplicate_task_names_in_same_dag():
    registry = TaskRegistry()

    @task(name="extract", dag="etl", registry=registry)
    def extract_a() -> int:
        return 1

    with pytest.raises(ValueError, match="within a DAG namespace"):

        @task(name="extract", dag="etl", registry=registry)
        def extract_b() -> int:
            return 2
