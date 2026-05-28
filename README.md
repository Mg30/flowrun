flowrun
=======

`flowrun` is a compact, dependency-free DAG execution engine for local ETL and
micro-batch workflows.

It is designed for code-first jobs that live inside one Python process:

- API ingest -> transform -> validate/quarantine -> sink
- small to medium DAGs declared in Python
- sequential micro-batch runs driven by external chunk sources

Core ideas:

- Keep orchestration small: declare tasks, wire dependencies, run a DAG.
- Keep runtime light: stdlib-only core.
- Keep behavior explicit: retries, async timeouts, run reports, resume, subgraphs.

`flowrun` is not a durable scheduler, distributed worker system, queue, or
control plane. If you need persistent workers, cron scheduling, cross-process
recovery guarantees, or dynamic scaling, use a heavier orchestrator.

## Installation

```bash
pip install flowrun-dag
```

Optional example dependencies:

```bash
pip install "flowrun-dag[examples]"
```

The import name stays `flowrun`:

```python
import flowrun
```

For development:

```bash
git clone https://github.com/Mg30/flowrun.git
cd flowrun
uv sync --group dev
uv sync --group dev --extra examples
uv run pytest -q
```

## Quick Start

The main flow is:

1. Create a `Pipeline`.
2. Register tasks on it.
3. Run the pipeline.

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from flowrun import Pipeline, RunContext


pipeline = Pipeline("daily_etl", max_workers=4, max_parallel=3)


@dataclass(frozen=True)
class Deps:
    source_path: str


@pipeline.task()
def extract(context: RunContext[Deps]) -> list[dict[str, int]]:
    return [{"id": 1, "amount": 10}, {"id": 2, "amount": 15}]


@pipeline.task(deps=[extract])
def transform(extract: list[dict[str, int]]) -> dict[str, int]:
    return {
        "rows": len(extract),
        "total": sum(row["amount"] for row in extract),
    }


@pipeline.task(deps=[transform])
def load(transform: dict[str, int], context: RunContext[Deps]) -> str:
    return f"loaded {transform['rows']} rows from {context.source_path}"


async def main() -> None:
    context = RunContext(Deps(source_path="/tmp/orders.json")).with_metadata(
        source="orders",
        batch_id="2026-05-28",
    )

    async with pipeline:
        run_id = await pipeline.run_once(context=context)
        report = pipeline.get_run_report(run_id)
        print(report["status"])
        print(report["tasks"]["load"]["result"])


asyncio.run(main())
```

## What To Use

### `Pipeline`

`Pipeline` is the public authoring and execution API. It owns one named DAG and
the runtime resources needed to execute it.

Create one with:

```python
pipeline = Pipeline(
    "daily_etl",
    executor=None,
    max_workers=8,
    max_parallel=4,
    logger=None,
    hooks=None,
    state_store=None,
)
```

Register tasks directly on the pipeline:

```python
@pipeline.task()
def extract() -> list[int]:
    return [1, 2, 3]
```

Useful pipeline methods:

- `pipeline.name`
- `pipeline.task(...)`
- `pipeline.tasks`
- `pipeline.dependencies`
- `pipeline.validate()`
- `pipeline.display()`
- `pipeline.list_tasks()`
- `await pipeline.run_once(context=None)`
- `await pipeline.run_many(contexts)`
- `await pipeline.run_subgraph(targets, context=None)`
- `await pipeline.resume(run_id, from_tasks=None, context=None)`
- `pipeline.subgraph(targets)`
- `pipeline.get_run_report(run_id)`
- `pipeline.override_tasks(...)`

## Task Registration

Tasks are normal Python callables.

```python
@pipeline.task(name="extract_orders", retries=2, timeout_s=10.0)
async def extract_orders(context: RunContext[Deps]) -> list[dict]:
    return await fetch_orders(context.api_base)
```

Arguments:

- `name`: optional, defaults to the function name.
- `deps`: optional dependency list using task names or decorated task callables.
- `timeout_s`: per-attempt timeout for async tasks only.
- `retries`: retry count after the first failed attempt.

If `deps` is omitted, required parameter names that match already-registered
task names are inferred.

```python
@pipeline.task()
def extract() -> list[int]:
    return [1, 2, 3]


@pipeline.task()
def sum_values(extract: list[int]) -> int:
    return sum(extract)
```

Use explicit `deps=` when you want clearer edges, aliases, non-identifier task
names, or forward references.

```python
@pipeline.task(name="sum_values", deps=[extract])
def total(extract: list[int]) -> int:
    return sum(extract)
```

### DAG-local task names

Task names must be unique within one DAG namespace. Different DAGs may reuse
natural names such as `extract`, `transform`, and `load`.

```python
users = Pipeline("users")
orders = Pipeline("orders")


@users.task(name="extract")
def extract_users() -> list[str]:
    return ["ada"]


@orders.task(name="extract")
def extract_orders() -> list[int]:
    return [1]
```

### Multi-module applications

Flowrun does not use an ambient "current DAG" or a process-wide task registry.
Tasks register when their decorators run, so a package split across modules
should choose one explicit registration point for each workflow.

For small to medium applications, export the pipeline-bound task decorator from a
runtime module and import it where tasks are defined:

```text
src/acme_etl/
    workflows/
        sales/
            runtime.py
            extract.py
            transform.py
            load.py
            run.py
```

```python
# workflows/sales/runtime.py
from flowrun import Pipeline


pipeline = Pipeline("sales", max_workers=4, max_parallel=3)
task = pipeline.task
```

```python
# workflows/sales/extract.py
from .runtime import task


@task()
def extract_orders() -> list[dict[str, int]]:
    return [{"id": 1, "amount": 10}]
```

```python
# workflows/sales/transform.py
from .runtime import task


@task(deps=["extract_orders"])
def summarize_orders(extract_orders: list[dict[str, int]]) -> dict[str, int]:
    return {
        "rows": len(extract_orders),
        "total": sum(row["amount"] for row in extract_orders),
    }
```

Make sure the task modules are imported before building or running the DAG:

```python
# workflows/sales/run.py
import asyncio

from . import extract, load, transform  # noqa: F401
from .runtime import pipeline


async def main() -> None:
    async with pipeline:
        run_id = await pipeline.run_once()
        print(pipeline.get_run_report(run_id)["status"])


asyncio.run(main())
```

For larger applications or tests that need a fresh pipeline, prefer explicit
registration functions. This avoids import-time side effects in task modules and
makes composition easier to control:

```python
# workflows/sales/users.py
def register(task):
    @task(name="extract_users")
    def extract_users() -> list[str]:
        return ["ada"]
```

```python
# workflows/sales/build.py
from flowrun import Pipeline

from . import orders, users


def build_sales_pipeline():
    pipeline = Pipeline("sales", max_workers=4, max_parallel=3)

    users.register(pipeline.task)
    orders.register(pipeline.task)

    return pipeline
```

In multi-module workflows, prefer explicit string dependencies for edges that
cross module boundaries. Inferred dependencies only see tasks that have already
registered, and callable dependencies can create import cycles between task
modules.

## Dependency Injection

Flowrun supports two dependency-result styles.

### Named dependency parameters

```python
@pipeline.task()
def extract() -> list[int]:
    return [1, 2, 3]


@pipeline.task()
def sum_values(extract: list[int]) -> int:
    return sum(extract)
```

### Generic `upstream`

```python
@pipeline.task(deps=["users", "orders"])
def combine(upstream: dict[str, object]) -> tuple[object, object]:
    return upstream["users"], upstream["orders"]
```

If `upstream` is declared, named dependency injection is disabled for that task.

## `RunContext`

`RunContext` carries typed runtime dependencies and optional reporting metadata.

```python
@pipeline.task()
def pull(context: RunContext[Deps]) -> dict:
    return {"base": context.api_base}
```

It also works alongside dependency-result parameters:

```python
@pipeline.task(deps=[transform])
def load(transform: dict, context: RunContext[Deps]) -> str:
    return write_rows(context.sink_path, transform)
```

Flowrun resolves normal and postponed annotations, so
`from __future__ import annotations` works.

### Run metadata

```python
context = RunContext(deps).with_metadata(
    batch_id=42,
    source="users_api",
)
```

Metadata is stored in the run report without adding more orchestration
parameters to every task.

### Deadlines and cooperative cancellation

```python
@pipeline.task(timeout_s=30.0)
async def pull(context: RunContext[Deps]) -> list[dict]:
    context.raise_if_cancelled()
    timeout_s = context.time_remaining_s() or 10.0
    return await call_api(context.api_base, timeout=timeout_s)
```

Synchronous tasks cannot use framework-level `timeout_s`; configure timeouts in
the client or library you call inside the task.

## Run Reports

`pipeline.get_run_report(run_id)` returns a
plain dictionary:

```python
{
    "run_id": "...",
    "dag_name": "etl",
    "metadata": {"batch_id": 42},
    "created_at": 0.0,
    "finished_at": 0.0,
    "status": "SUCCESS",
    "tasks": {
        "extract": {
            "status": "SUCCESS",
            "attempt": 1,
            "started_at": 0.0,
            "finished_at": 0.0,
            "error": None,
            "result": [],
        }
    },
}
```

Run-level status rules:

- `SUCCESS` when all tasks are `SUCCESS`
- `FAILED` when any task is `FAILED` or `SKIPPED`
- `RUNNING` otherwise

## Resume And Subgraphs

Run only one target branch and its transitive dependencies:

```python
run_id = await pipeline.run_subgraph(["load"], context=context)
```

Resume a previous run while preserving successful upstream tasks:

```python
new_run_id = await pipeline.resume(old_run_id, context=context)
```

Resume from a checkpoint task and all downstream dependents:

```python
new_run_id = await pipeline.resume(
    old_run_id,
    from_tasks=["transform"],
    context=context,
)
```

Unknown checkpoint names raise `ValueError`.

## Pipeline Overrides For Tests

Use `Pipeline.override_tasks(...)` to replace selected task implementations while
keeping the original graph.

```python
test_pipeline = pipeline.override_tasks(
    extract=[{"id": 1, "amount": 10}],
)

run_id = await test_pipeline.run_once(context=context)
report = test_pipeline.get_run_report(run_id)
```

Override values may be constants or callables.

## Hooks

Hooks are small synchronous callbacks for alerts, metrics, and tracing. Hook
errors are caught and logged so they do not crash a run.

```python
from flowrun import Pipeline, fn_hook


hook = fn_hook(
    on_task_failure=lambda event: print(f"FAIL {event.task_name}: {event.error}"),
    on_dag_end=lambda event: print(f"DONE {event.dag_name}"),
)

pipeline = Pipeline("etl", hooks=[hook])
```

Events:

- `DagStartEvent`, `DagEndEvent`
- `TaskStartEvent`, `TaskSuccessEvent`, `TaskFailureEvent`
- `TaskRetryEvent`, `TaskSkipEvent`

## Practical Patterns

### Small ETL

- Keep task wrappers thin.
- Put business logic in normal functions that can be unit tested directly.
- Use retries on flaky IO tasks, not pure transforms.
- Keep `max_parallel` modest for predictable local resource use.

### Sequential micro-batches

Drive chunks from outside the DAG, then run the same pipeline once per chunk.

```python
async def chunk_contexts():
    for chunk_index in range(3):
        rows = [{"value": chunk_index * 10 + offset} for offset in range(3)]
        yield RunContext(ChunkDeps(chunk_index=chunk_index, rows=rows)).with_metadata(
            batch_id=chunk_index,
            source="users_api",
        )


async with pipeline:
    run_ids = await pipeline.run_many(chunk_contexts())
```

### Polars and validation

For Polars/Pandera workflows, keep the orchestration layer thin:

- async extraction functions for raw payloads
- pure Polars transform functions
- validation functions that split accepted and rejected rows
- quarantine sink functions for rejected rows
- a final join/aggregation function
- thin Flowrun task wrappers that express orchestration only

See `examples/polars_workflow_demo.py` for a complete example.

## Validation Rules

Flowrun validates DAGs before execution and catches:

- empty DAGs
- missing dependencies
- cross-DAG dependencies
- cycles
- duplicate task names inside one DAG
- required task parameters Flowrun cannot provide
- unknown subgraph targets
- unknown resume checkpoint tasks

## Public API

Top-level exports from `flowrun`:

- `Pipeline`
- `RunContext`, `RunCancelledError`
- `TaskSpec`, `TaskRegistry`
- `SchedulerConfig`
- `RunHook`, `fn_hook`
- `StateStore`, `InMemoryStateStore`

The package includes `py.typed` for type checkers.

## Logging

Pass a logger to `Pipeline("name", logger=...)`.

Typical levels:

- `INFO`: DAG start/finish, task success, retries, skips
- `WARNING`: task failures and timeouts
- `DEBUG`: task launch details, tracebacks, shutdown details

## Testing Your DAGs

- Unit test business functions directly.
- Build pipelines and assert `pipeline.tasks` / `pipeline.dependencies`.
- Use `pipeline.override_tasks(...)` for controlled end-to-end tests.
- Assert on `pipeline.get_run_report(run_id)` for run behavior.

## License

MIT
