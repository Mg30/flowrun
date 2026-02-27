from flowrun.context import RunContext
from flowrun.task import TaskRegistry, task, task_template


def test_task_decorator_supports_bare_usage():
    registry = TaskRegistry()
    token = registry.activate()
    try:
        @task
        def bare() -> int:
            return 1
    finally:
        TaskRegistry.deactivate(token)

    spec = registry.get("bare")
    assert spec.func is bare


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


def test_task_template_registers_parameterized_tasks_and_preserves_upstream_acceptance():
    registry = TaskRegistry()

    def fetch(*, value: str, upstream=None) -> str:
        return value

    tpl = task_template(fetch, registry=registry)
    tpl.bind("fetch_alpha", value="alpha")
    tpl.bind("fetch_beta", value="beta")

    spec_a = registry.get("fetch_alpha")
    spec_b = registry.get("fetch_beta")

    assert spec_a.func() == "alpha"
    assert spec_b.func() == "beta"
    assert spec_a.accepts_upstream is True
    assert spec_b.accepts_upstream is True


def test_task_template_bound_callables_can_be_used_directly_as_dependencies():
    registry = TaskRegistry()

    def fetch(*, value: str) -> str:
        return value

    tpl = task_template(fetch, registry=registry)
    fetch_alpha = tpl.bind("fetch_alpha", value="alpha")
    fetch_beta = tpl.bind("fetch_beta", value="beta")

    @task(name="combine", deps=[fetch_alpha, fetch_beta], registry=registry)
    def combine(fetch_alpha: str, fetch_beta: str) -> str:
        return f"{fetch_alpha}+{fetch_beta}"

    spec = registry.get("combine")
    assert spec.deps == ["fetch_alpha", "fetch_beta"]
