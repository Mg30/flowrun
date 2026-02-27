flowrun
=======

`flowrun` is a lightweight async DAG orchestrator for small to medium ETL pipelines.
It is designed for low operational overhead and low complexity workloads (for example
Polars pipelines, API ingest + transform + load jobs, or periodic data sync tasks).

Core ideas:

- Keep orchestration simple: declare tasks + dependencies, run a DAG.
- Keep runtime dependency-free: stdlib-based implementation.
- Keep behavior explicit: retries, timeouts, skip semantics, run reports.

## Installation

```bash
pip install flowrun-dag
```

> The import name remains `flowrun`:
> ```python
> import flowrun
> ```

For development:

```bash
git clone https://github.com/Mg30/flowrun.git
cd flowrun
uv sync --group dev
uv run pytest -q
```

## Quick Start

```python
import asyncio
from dataclasses import dataclass

from flowrun import RunContext, build_default_engine

engine = build_default_engine(max_workers=4, max_parallel=3)


@dataclass(frozen=True)
class Deps:
    source_path: str


@engine.task(name="extract", dag="daily_etl")
def extract(context: RunContext[Deps]) -> list[dict]:
    # In real jobs, read from file/API/db
    return [{"id": 1, "amount": 10}, {"id": 2, "amount": 15}]


@engine.task(name="transform", dag="daily_etl", deps=[extract])
def transform(extract: list[dict]) -> dict[str, int]:
    total = sum(row["amount"] for row in extract)
    return {"rows": len(extract), "total": total}


@engine.task(name="load", dag="daily_etl", deps=[transform])
def load(transform: dict[str, int]) -> str:
    # Persist results
    return f"loaded rows={transform['rows']} total={transform['total']}"


async def main() -> None:
    ctx = RunContext(Deps(source_path="/tmp/data.json"))
    async with engine:
        engine.validate("daily_etl")
        run_id = await engine.run_once("daily_etl", context=ctx)
        report = engine.get_run_report(run_id)
        print(report["status"])  # SUCCESS | FAILED | RUNNING


asyncio.run(main())
```

## Concepts

- Task: Python callable registered with `@engine.task(...)` or `@task(...)`.
- DAG: namespace (`dag="name"`) plus dependency edges between tasks.
- Run: one execution instance of a DAG (`run_id`).
- State store: tracks run/task status, timing, errors, and results.

Task status lifecycle:

- `PENDING -> RUNNING -> SUCCESS`
- `PENDING -> RUNNING -> FAILED -> PENDING` (retry path)
- `PENDING -> SKIPPED` (blocked by failed upstream)

## API Guide

### `build_default_engine(...)`

```python
engine = build_default_engine(
    executor=None,
    max_workers=8,
    max_parallel=4,
    logger=None,
    hooks=None,
    state_store=None,
)
```

Parameters:

- `executor`: optional `concurrent.futures.Executor` for sync tasks.
- `max_workers`: thread pool size if `executor` is not provided.
- `max_parallel`: max concurrent scheduled tasks, must be `>= 1`.
- `logger`: optional `logging.Logger` used across components.
- `hooks`: optional list of `RunHook` handlers.
- `state_store`: optional custom state store (`StateStoreProtocol`).

Returns: configured `Engine`.

### `Engine` methods

Run control:

- `await engine.run_once(dag_name, context=None) -> str`
- `await engine.resume(run_id, from_tasks=None, context=None) -> str`
- `await engine.run_subgraph(dag_name, targets, context=None) -> str`

Validation and discovery:

- `engine.validate(dag_name) -> None`
- `engine.list_dags() -> list[str]`
- `engine.list_tasks(dag_name) -> list[str]`
- `engine.display_dag(dag_name) -> str`

Reporting:

- `engine.get_run_report(run_id) -> dict`

Resource lifecycle:

- `engine.close() -> None`
- `async with engine:` closes owned thread pool on exit.

### Task registration

Preferred style (bound to engine registry):

```python
@engine.task(name="task_a", dag="etl", deps=[...], timeout_s=30.0, retries=1, retain_result=True)
def task_a(...):
    ...
```

Arguments:

- `name`: optional, defaults to function name.
- `dag`: DAG namespace for selection via `run_once(dag_name)`.
- `deps`: list of task names or decorated task callables.
- `timeout_s`: per-attempt timeout (`None` disables timeout).
- `retries`: retry count after failures.
- `retain_result`: if `False`, clear result from state when safe.

Avoid repeating `dag=...` with a DAG-scoped container:

```python
etl = engine.dag("daily_etl")

@etl.task(name="extract")
def extract() -> list[int]:
    return [1, 2, 3]

@etl.task(name="sum_values", deps=[extract])
def sum_values(extract: list[int]) -> int:
    return sum(extract)

run_id = await etl.run_once()
```

Available on the scope:

- `etl.task(...)`
- `etl.task_template(...)`
- `await etl.run_once(context=None)`
- `await etl.run_subgraph(targets, context=None)`
- `etl.validate()`, `etl.display()`, `etl.list_tasks()`

Also available as global decorator:

```python
from flowrun import task, TaskRegistry

registry = TaskRegistry()
token = registry.activate()

@task
def my_task():
    return 1

TaskRegistry.deactivate(token)
```

Notes:

- `@task(...)`, `@task`, and `@task("name", ...)` are supported.
- If using global `@task`, provide `registry=...` or activate one.

### Dependency result injection

Named dependency injection:

```python
@engine.task(name="extract", dag="etl")
def extract() -> list[int]:
    return [1, 2, 3]

@engine.task(name="sum_values", dag="etl", deps=[extract])
def sum_values(extract: list[int]) -> int:
    return sum(extract)
```

Generic `upstream` injection:

```python
@engine.task(name="combine", dag="etl", deps=["a", "b"])
def combine(upstream: dict[str, object]) -> object:
    return (upstream["a"], upstream["b"])
```

If `upstream` is declared, named dependency injection is disabled.

### Context injection

Tasks can accept a typed `RunContext[...]` as a positional parameter.

```python
@dataclass(frozen=True)
class Deps:
    api_base: str

@engine.task(name="pull", dag="etl")
def pull(context: RunContext[Deps]) -> dict:
    return {"base": context.api_base}
```

### Task templates

Register parameterized task variants.

```python
def fetch_table(*, table: str) -> str:
    return f"select * from {table}"

tpl = engine.task_template(fetch_table, dag="etl")
tpl.bind("fetch_users", table="users")
tpl.bind("fetch_orders", table="orders")
```

## Execution Semantics

### DAG scoping and unknown DAG behavior

- If tasks use explicit `dag=...` namespaces, unknown DAG names raise `ValueError`.
- Error messages include available DAG names and a close-match suggestion.
- Legacy behavior remains for unscoped registries (single implicit DAG).

### Dependency validation

Build-time validation catches:

- Missing dependencies.
- Cross-DAG dependencies.
- Cycles.

Missing dependency errors include close-match suggestions when available.

### Retries

- Retries are per task and per run attempt.
- Downstream tasks are skipped only after upstream retries are exhausted.

### Timeouts

- Applied per attempt.
- Async tasks use `asyncio.wait_for`.
- Sync tasks run in executor and are awaited with timeout.

### Result retention

- `retain_result=True` (default): keep result in state.
- `retain_result=False`: clear result once all downstream consumers are launched/done.
- Useful to reduce memory when passing larger intermediate objects.

## Run Report Format

`engine.get_run_report(run_id)` returns:

```python
{
  "run_id": "...",
  "dag_name": "...",
  "created_at": 0.0,
  "finished_at": 0.0,
  "status": "SUCCESS",  # SUCCESS | FAILED | RUNNING
  "tasks": {
    "task_name": {
      "status": "SUCCESS",
      "attempt": 1,
      "started_at": 0.0,
      "finished_at": 0.0,
      "error": None,
      "result": {...}
    }
  }
}
```

Run-level status rules:

- `FAILED` if any task is `FAILED` or `SKIPPED`.
- `SUCCESS` if all tasks are `SUCCESS`.
- `RUNNING` otherwise.

## Hooks

Use hooks to emit metrics, alerts, or tracing signals.

```python
from flowrun import fn_hook, build_default_engine

hook = fn_hook(
    on_task_failure=lambda e: print(f"FAIL {e.task_name}: {e.error}"),
    on_dag_end=lambda e: print(f"DAG done: {e.dag_name}"),
)

engine = build_default_engine(hooks=[hook])
```

Hook API:

- `RunHook` class with overridable methods.
- `fn_hook(...)` for function-based handlers.
- Hook errors are caught and logged (do not crash runs).

Events:

- `DagStartEvent`, `DagEndEvent`
- `TaskStartEvent`, `TaskSuccessEvent`, `TaskFailureEvent`
- `TaskRetryEvent`, `TaskSkipEvent`

## State Stores

In-memory (default):

- `StateStore` / `InMemoryStateStore`
- Fast, process-local, ephemeral.

SQLite persistent backend:

- `SqliteStateStore(db_path, serializer=..., cache_ttl_s=None, recover=False)`
- Persists run and task state.
- Optional crash recovery marks orphaned `RUNNING` tasks as failed.

Serialization options for persisted results:

- `JsonSerializer` (default for SQLite backend)
- `PickleSerializer`
- custom `ResultSerializer` implementation

## Practical ETL Patterns

### Small Polars pipeline pattern

- Keep each task focused (`extract`, `transform`, `load`).
- Set `retain_result=False` on large intermediate transforms.
- Use `retries` on flaky IO tasks, not pure transforms.
- Keep `max_parallel` modest for predictable resource use.

### Re-run from a checkpoint task

```python
new_run_id = await engine.resume(old_run_id, from_tasks=["transform"], context=ctx)
```

This re-executes `transform` and all downstream tasks, while preserving unaffected successful upstream tasks.

### Run only a target branch

```python
run_id = await engine.run_subgraph("daily_etl", targets=["load"], context=ctx)
```

This executes `load` plus all transitive dependencies required for `load`.

## Logging

Pass a logger to `build_default_engine(logger=...)`.

Typical levels:

- `INFO`: DAG start/finish, task success, retries, skips.
- `WARNING`: task failures, timeouts.
- `DEBUG`: task launch details, tracebacks, shutdown details.

## Testing Your DAGs

Recommended test layers:

- Unit test each task function directly.
- Integration test DAG execution with `build_default_engine()`.
- Validate topology with `engine.validate(...)` and `engine.list_tasks(...)`.
- Assert on `get_run_report(...)` for end-to-end behavior.

## Public API Surface

Top-level exports in `flowrun`:

- `Engine`, `build_default_engine`
- `RunContext`
- `task`, `task_template`, `TaskSpec`, `TaskRegistry`
- `SchedulerConfig`
- `RunHook`, `fn_hook`
- `StateStore`, `InMemoryStateStore`, `StateStoreProtocol`
- `SqliteStateStore`
- `JsonSerializer`, `PickleSerializer`, `ResultSerializer`

## License

MIT
