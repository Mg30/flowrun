flowrun
=======

flowrun is a minimalist orchestration runtime for IO-bound workloads. It ships zero runtime dependencies, but still gives you task decorators, DAG validation, concurrency control, and run reporting in one tiny package.

## Highlights

- Task decorator that understands optional `RunContext` parameters and dependency declarations
- Deterministic DAG builder with cycle detection and topological ordering
- Async-first scheduler with configurable parallelism and automatic skip-on-failure semantics
- Execution engine that runs `async` tasks directly and sync tasks in a thread pool
- In-memory state store with run reports you can hand off to a UI or CLI

## Installation

```bash
pip install flowrun
```

## Define and Run a DAG

```python
import asyncio
from dataclasses import dataclass

from flowrun.context import RunContext
from flowrun.engine import build_default_engine
from flowrun.task import TaskRegistry, task


engine = build_default_engine()           # wires registry, state store, scheduler, executor
registry = TaskRegistry.get_default()     # decorator fallback; useful when you need direct access


@dataclass(frozen=True)
class Deps:
	base_url: str


ctx = RunContext(Deps(base_url="https://api.example"))


@task(name="fetch_user", timeout_s=5)
async def fetch_user(context: RunContext[Deps]) -> dict:
	print(f"fetching via {context.base_url}")
	return {"id": "123", "name": "Ada"}


@task(name="email_user", deps=[fetch_user])
def email_user() -> None:
	print("sending email")


async def main() -> None:
	engine.display_dag("user_onboarding")           # ASCII tree for a quick sanity check
	run_id = await engine.run_once("user_onboarding", context=ctx)
	report = engine.get_run_report(run_id)
	print(report["tasks"]["fetch_user"]["status"])  # SUCCESS


asyncio.run(main())
```

Key ideas in that example:

- Decorate callables with `@task`. Dependencies can be strings or direct references to other task functions.
- Add a `RunContext[...]` parameter when you want strongly-typed access to shared dependencies.
- `Engine.run_once()` builds a DAG from every task currently in the registry and executes it once, honoring the declared dependencies and timeouts.
- Tasks that fail mark their downstream dependants as `SKIPPED` so the run finalizes quickly.

## Core Concepts

- **TaskRegistry** – stores `TaskSpec` metadata; you can instantiate your own and inject it everywhere, or rely on the default configured by `build_default_engine()`.
- **RunContext** – lightweight wrapper around any dependency bundle (dataclass, namespace, mapping). Attribute access falls through to the wrapped object for ergonomics and typing support.
- **DAGBuilder** – validates dependencies, rejects cycles, and produces topologically sorted node lists.
- **TaskExecutor** – dispatches async tasks natively and sync tasks via `ThreadPoolExecutor`, enforcing per-task timeouts.
- **Scheduler** – coordinates eligibility, concurrency, and state transitions. Once a parent fails, children become `SKIPPED`.
- **StateStore** – in-memory record of runs and task attempts. Query it with `Engine.get_run_report()` to build dashboards or CLIs.

Every component is intentionally small so you can swap in your own threading model, persistence layer, or DAG source without touching the rest of the stack.

## Extending flowrun

- Replace the thread pool: pass your own `TaskExecutor` instance to `Scheduler`.
- Integrate durable storage: provide a custom `StateStore` implementation with the same interface.
- Build DAGs from config: populate a `TaskRegistry` programmatically before calling `Engine.run_once()`.

## Testing

Install dev dependencies and execute the test suite:

```bash
pip install -e .[dev]
pytest -v
```

## License

flowrun is released under the MIT License.
