flowrun
=======

flowrun is a minimalist orchestration runtime for IO-bound workloads. It ships with zero runtime dependencies yet still gives you task decorators, DAG validation, concurrency control, and run reporting in one tiny package.

## Why flowrun?

- Declarative tasks: annotate functions with `@task` and declare dependencies via names or callables.
- Strong typing without ceremony: `RunContext[...]` keeps dependency bundles explicit and IDE-friendly.
- Async-native execution: coroutines run directly, sync callables are offloaded to an injected executor.
- Deterministic scheduling: a topologically sorted DAG, per-task timeouts, and automatic skip-on-failure semantics.
- Simple state inspection: in-memory `StateStore` surfaces run metadata you can hand off to a CLI or UI.

## Installation

```bash
pip install flowrun
```

## Quick Start

```python
import asyncio
from dataclasses import dataclass

from flowrun.context import RunContext
from flowrun.engine import build_default_engine
from flowrun.task import task


engine = build_default_engine()
registry = engine.registry  # same registry used by the decorator fallback


@dataclass(frozen=True)
class Deps:
    base_url: str
    send_email: callable


ctx = RunContext(Deps(base_url="https://api.example", send_email=lambda body: None))


@task(name="fetch_user", timeout_s=5)
async def fetch_user(context: RunContext[Deps]) -> dict:
    print(f"fetching via {context.base_url}")
    return {"id": "123", "name": "Ada"}


@task(name="fetch_settings", timeout_s=3)
async def fetch_settings() -> dict:
    return {"locale": "en_GB"}


@task(name="email_user", deps=[fetch_user, fetch_settings])
def email_user(upstream: dict[str, dict]) -> None:
    user = upstream["fetch_user"]
    settings = upstream["fetch_settings"]
    print(f"Emailing {user['name']} with locale {settings['locale']}")


async def main() -> None:
    engine.display_dag("user_onboarding")
    run_id = await engine.run_once("user_onboarding", context=ctx)
    report = engine.get_run_report(run_id)
    print(report["tasks"]["email_user"]["status"])  # SUCCESS


asyncio.run(main())
```

What happened?

1. `build_default_engine()` wires the default registry, state store, scheduler, and executor.
2. Tasks are registered at import time; dependencies can be strings or callable references.
3. Tasks may opt into the `RunContext` via a positional parameter and can receive upstream results by naming an `upstream` argument.
4. `Engine.run_once()` snapshots the registry into a DAG, enforces dependencies and timeouts, and records results.

## Core Modules (High Level)

| Module | Role | Notes |
| ------ | ---- | ----- |
| `flowrun.task` | Task authoring | `@task` decorator produces `TaskSpec` entries. Detects context and `upstream` parameters automatically. |
| `flowrun.dag` | Structure builder | Validates dependencies against the registry, performs topological sort, raises on cycles or missing tasks. |
| `flowrun.executor` | Task execution | Runs async specs natively and sync specs in any `concurrent.futures.Executor`. Enforces per-task timeout and captures results/errors. |
| `flowrun.scheduler` | Concurrency + orchestration | Applies dependency gates, respects `SchedulerConfig.max_parallel`, forwards upstream results, and updates run state. |
| `flowrun.state` | Run ledger | Stores `RunRecord` + `TaskRunRecord` objects. Used for reporting (`Engine.get_run_report`). |
| `flowrun.engine` | Facade | Bridges registry, scheduler, and DAG builder. Provides `run_once`, `display_dag`, and the default wiring helper. |

These components are intentionally decoupled so you can replace one without rewriting the others—for example, swap the state store, inject a process pool, or build DAGs from configuration files.

## Authoring Tasks

### Context Injection

```python
from dataclasses import dataclass
from flowrun.context import RunContext
from flowrun.task import task


@dataclass(frozen=True)
class Deps:
    s3_client: object


@task
def pull_report(context: RunContext[Deps]):
    return context.s3_client.pull()
```

Add a `RunContext[...]` parameter when you need shared dependencies. The decorator detects it automatically.

### Upstream Results

```python
@task(deps=[pull_report])
def aggregate(upstream: dict[str, object]):
    report = upstream["pull_report"]
    return summarize(report)
```

Name an `upstream` parameter to receive a mapping of dependency names to their returned values. Only successful parents appear in the dict; failed or skipped parents short-circuit the run before the child launches.

### Dynamic Task Registration

Tasks register at import time. Generate families of tasks by looping over parameters before you start the engine:

```python
from flowrun.task import task, TaskRegistry


def register_fetchers(registry: TaskRegistry, values: list[str]) -> None:
    for value in values:
        @task(name=f"fetch_{value}", registry=registry)
        def _fetch(upstream=None, value=value):  # capture value in default
            return call_api(value)


registry = TaskRegistry()
register_fetchers(registry, ["alpha", "beta", "gamma"])
```

Import this module inside your entrypoint to populate the registry before calling `engine.run_once(...)`.

## Execution Lifecycle

1. **Register tasks** – via decorators, optionally using helper functions to generate parameterized tasks.
2. **Build the engine** – inject your `TaskRegistry`, `StateStore`, executor, and scheduler configuration.
3. **Display / inspect** – call `engine.display_dag(name)` for a textual view; useful in CLIs and tests.
4. **Run the DAG** – `await engine.run_once(name, context=RunContext(...))` orchestrates tasks with respect to dependencies and concurrency limits.
5. **Report results** – `engine.get_run_report(run_id)` returns timestamps, status, attempts, results, and errors for each task.

## Extensibility Hooks

- **Custom executors**: pass any `concurrent.futures.Executor` (thread or process pool) to `build_default_engine(executor=...)`. Lifecycle ownership stays with the caller.
- **Persistent state**: reimplement `StateStore` with the same interface (create/get/mark/finalize) and inject it into the scheduler.
- **Alternative DAG sources**: populate a registry from YAML, a database, or generated code before calling `Engine.run_once`.

## Testing

Install dev dependencies and execute the test suite:

```bash
pip install -e .[dev]
pytest -v
```

## License

flowrun is released under the MIT License.
