from flowrun.context import RunContext
from flowrun.task import TaskRegistry, task


def test_task_decorator_detects_required_context():
    registry = TaskRegistry()

    @task(name="needs_ctx", registry=registry)
    def needs_ctx(ctx: RunContext[dict[str, int]]):
        return ctx.deps["value"]

    spec = registry.get("needs_ctx")

    assert spec.accepts_context is True
    assert spec.requires_context is True


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
