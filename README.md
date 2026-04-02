flowrun
=======

`flowrun` is a compact DAG execution engine for small to medium ETL jobs.
It is designed for local, code-first workflows such as API ingest -> Polars transform
-> validation/quarantine -> sink, plus sequential micro-batch data sync jobs.

Core ideas:

- Keep orchestration simple: declare tasks + dependencies, run a DAG.
- Keep runtime dependency-free: stdlib-based implementation.
- Keep behavior explicit: retries, timeouts, skip semantics, run reports.

## Positioning

`flowrun` is a good fit when your workflow lives inside one Python process,
the DAG is declared in code, and you want a small execution layer around ETL
functions rather than a full workflow platform.

It is not positioned as a durable scheduler, distributed orchestrator, or
policy-heavy control plane. If you need persistent workers, cron scheduling,
cross-process recovery guarantees, dynamic scaling, or extensive execution
policies, you should use a heavier system.

## Strengths

- Clear fit for API -> transform -> validate -> load pipelines.
- Works well with Polars-style business logic and thin orchestration wrappers.
- Small API surface and low operational overhead.
- Explicit execution model: retries, DAG validation, run reports, hooks, resume, and subgraph runs.
- Good match for sequential micro-batch jobs where context such as `batch_id`, `source`, or `window` matters.

## Tradeoffs

- In-process execution only; no distributed workers or durable queueing.
- No built-in scheduling layer; run triggering belongs outside the framework.
- Recovery is scoped to stored run state in the current process, not a full external orchestration backend.
- Retry behavior is intentionally simple; API-specific backoff and resilience policies belong in user code.
- Best for low-to-moderate workflow complexity, not platform-scale orchestration.

## Installation

```bash
pip install flowrun-dag
```

Optional example dependencies:

```bash
pip install "flowrun-dag[examples]"
```

This installs the libraries used by the example workflows, including Polars and
Pandera's Polars integration.

> The import name remains `flowrun`:
> ```python
> import flowrun
> ```

For development:

```bash
git clone https://github.com/Mg30/flowrun.git
cd flowrun
uv sync --group dev
uv sync --group dev --extra examples
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


# Task names default to the Python function name. Use name="daily_extract"
# only when you need an explicit alias or a stable external task name.
@engine.task(dag="daily_etl")
def extract(context: RunContext[Deps]) -> list[dict]:
    # In real jobs, read from file/API/db
    return [{"id": 1, "amount": 10}, {"id": 2, "amount": 15}]


@engine.task(dag="daily_etl", deps=[extract])
def transform(extract: list[dict]) -> dict[str, int]:
    total = sum(row["amount"] for row in extract)
    return {"rows": len(extract), "total": total}


@engine.task(dag="daily_etl", deps=[transform])
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

- Task: Python callable registered with `@engine.task(...)`.
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
- `state_store`: optional custom in-memory state store instance.

Returns: configured `Engine`.

### `Engine` methods

Run control:

- `await engine.run_once(dag_name, context=None) -> str`
- `await engine.run_many(dag_name, contexts) -> list[str]`
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
@engine.task(name="task_a", dag="etl", deps=[...], retries=1)
def task_a(...):
    ...
```

Arguments:

- `name`: optional, defaults to function name.
- `dag`: DAG namespace for selection via `run_once(dag_name)`.
- `deps`: optional list of task names or decorated task callables. When omitted,
  required parameter names that match already-registered task names are inferred.
- `timeout_s`: per-attempt timeout for async tasks (`None` disables timeout).
- `retries`: retry count after failures.

For synchronous tasks, configure timeouts in the client you call inside the task.
`flowrun` intentionally rejects framework-level timeouts for sync callables because
thread-based timeouts cannot safely stop side effects.

Use explicit `deps=` when you need `upstream`, dependency aliases, non-identifier
task names, or forward references to tasks registered later.

Avoid repeating `dag=...` with a DAG-scoped container:

```python
etl = engine.dag("daily_etl")

@etl.task(name="extract")
def extract() -> list[int]:
    return [1, 2, 3]

@etl.task(name="sum_values")
def sum_values(extract: list[int]) -> int:
    return sum(extract)

run_id = await etl.run_once()
```

Available on the scope:

- `etl.task(...)`
- `await etl.run_once(context=None)`
- `await etl.run_many(contexts)`
- `await etl.run_subgraph(targets, context=None)`
- `etl.validate()`, `etl.display()`, `etl.list_tasks()`

### Dependency result injection

Named dependency injection with inferred dependencies:

```python
@engine.task(name="extract", dag="etl")
def extract() -> list[int]:
    return [1, 2, 3]

@engine.task(name="sum_values", dag="etl")
def sum_values(extract: list[int]) -> int:
    return sum(extract)
```

Explicit dependencies remain available when you prefer the edges in the decorator:

```python
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

`RunContext` can also carry an ambient deadline or cancellation event when a task
needs to pass timeouts into a client or stop cooperatively at a safe checkpoint.

```python
import threading

cancel_event = threading.Event()
ctx = RunContext(Deps(api_base="https://api.example.com"))
ctx = ctx.with_deadline_s(30.0).with_cancel_event(cancel_event)

@engine.task(name="pull", dag="etl")
def pull(context: RunContext[Deps]) -> dict:
    context.raise_if_cancelled()
    timeout_s = context.time_remaining_s() or 10.0
    return call_api(context.api_base, timeout=timeout_s)
```

This is optional. Most tasks do not need these helpers.

### Run metadata

Attach lightweight reporting metadata to a run through `RunContext`.

```python
ctx = RunContext(Deps(api_base="https://api.example.com")).with_metadata(
    batch_id=42,
    source="users_api",
    window="2026-04-01",
)

run_id = await engine.run_once("etl", context=ctx)
report = engine.get_run_report(run_id)
print(report["metadata"])  # {"batch_id": 42, "source": "users_api", ...}
```

This is useful for ETL-style identifiers such as batch ids, partitions, sources,
or time windows without adding more orchestration parameters.

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
- Required task parameters that do not match an inferred or explicit dependency, `RunContext`, or `upstream`.

Missing dependency errors include close-match suggestions when available.

### Retries

- Retries are per task and per run attempt.
- Downstream tasks are skipped only after upstream retries are exhausted.

### Timeouts

- Applied per attempt for async tasks.
- Async tasks use `asyncio.wait_for`.
- Sync tasks do not support framework-level timeouts; use client/library timeouts inside the task.

## Run Report Format

`engine.get_run_report(run_id)` returns:

```python
{
  "run_id": "...",
  "dag_name": "...",
  "metadata": {"batch_id": 42, "source": "users_api"},
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

## Practical ETL Patterns

### Small Polars pipeline pattern

- Keep each task focused (`extract`, `transform`, `load`).
- Use `retries` on flaky IO tasks, not pure transforms.
- Keep `max_parallel` modest for predictable resource use.

### Sequential micro-batch pattern

When chunks are fetched outside the DAG, run the full DAG once per chunk in a
sequential loop. This is a micro-batch pattern, not end-to-end streaming.

```python
import asyncio
from dataclasses import dataclass

from flowrun import RunContext, build_default_engine

engine = build_default_engine(max_workers=4, max_parallel=2)
etl = engine.dag("users")


@dataclass(frozen=True)
class ChunkDeps:
    chunk_index: int
    rows: list[dict[str, int]]


@etl.task()
def input_chunk(context: RunContext[ChunkDeps]) -> list[dict[str, int]]:
    return context.rows


@etl.task(deps=[input_chunk])
def transform_chunk(input_chunk: list[dict[str, int]]) -> dict[str, int]:
    return {
        "rows": len(input_chunk),
        "total": sum(row["value"] for row in input_chunk),
    }


@etl.task(deps=[transform_chunk])
def load_chunk(transform_chunk: dict[str, int]) -> str:
    return f"loaded rows={transform_chunk['rows']} total={transform_chunk['total']}"


async def chunk_contexts():
    for chunk_index in range(3):
        rows = [{"value": chunk_index * 10 + offset} for offset in range(3)]
        yield RunContext(ChunkDeps(chunk_index=chunk_index, rows=rows)).with_metadata(
            batch_id=chunk_index,
            source="users_api",
        )


async def main() -> None:
    async with engine:
        etl.validate()
        run_ids = await etl.run_many(chunk_contexts())
        print(run_ids)


asyncio.run(main())
```

This keeps chunk fetching outside the DAG while preserving plain task boundaries
inside the graph.

### Layered Polars workflow pattern

For teams that need clearer structure, keep undecorated business functions in one
layer and add a thin Flowrun orchestration layer on top.

Recommended split:

- async extraction functions that fetch raw endpoint payloads
- pure Polars functions that normalise each dataset independently
- Pandera validation functions that split validated and rejected rows
- quarantine sink functions for rejected rows
- a pure join/aggregation function that combines the processed frames
- a plain sink function
- small task wrappers that call those functions and express orchestration only

See `examples/polars_workflow_demo.py` for a concrete example with two fake API
endpoints fetched in parallel, separate Polars processing branches, schema
validation with quarantine, a join step, and a fake sink.

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
- `TaskSpec`, `TaskRegistry`
- `SchedulerConfig`
- `RunHook`, `fn_hook`
- `StateStore`, `InMemoryStateStore`

## License

MIT
