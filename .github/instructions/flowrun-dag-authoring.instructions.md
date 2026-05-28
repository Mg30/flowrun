---
description: "Use when creating or modifying Flowrun DAGs, ETL pipelines, task graphs, RunContext usage, hooks, micro-batch workflows, or Polars/Pandera examples in this repository. Covers Flowrun API features, DAG authoring philosophy, dependency inference, retries, async timeouts, resume/subgraph runs, and example workflow patterns."
name: "Flowrun DAG Authoring"
---
# Flowrun DAG Authoring

## What Flowrun Is

Flowrun is a compact, in-process DAG engine for small to medium ETL jobs.
Use it for code-first workflows such as API ingest -> transform -> validate -> quarantine -> sink,
or for sequential micro-batch runs where each batch executes the same graph with a different
`RunContext`.

Do not treat Flowrun as a durable scheduler, distributed orchestrator, or policy-heavy control
plane. Do not invent features such as cron scheduling, external workers, durable queues,
cross-process recovery, or platform-style retry policies.

## Authoring Philosophy

- Keep orchestration thin. Put business logic in plain Python helpers and make tasks small wrappers.
- Prefer typed interfaces. Use `dataclass` dependency bundles, `TypedDict` payloads, and precise return types.
- Keep behavior explicit. Declare retries, async timeouts, validation, hooks, and run reporting in code.
- Keep DAGs readable. Prefer clear task names and branch structure over dense decorator tricks.
- Keep runtime concerns local. Client-level timeouts, backoff, API retry policies, and sink semantics belong in user code.

## Principal API Features

### Engine setup

Create the engine with the public API:

```python
from flowrun import build_default_engine

engine = build_default_engine(
    max_workers=4,
    max_parallel=3,
    logger=logger,
    hooks=[hook],
)
```

- `max_workers` controls the thread pool for synchronous tasks.
- `max_parallel` caps concurrent scheduled work.
- `hooks` accepts `RunHook` handlers, often created with `fn_hook(...)`.

### DAG scoping

Prefer a DAG scope instead of repeating `dag="..."` on every task:

```python
etl = engine.dag("sales_summary")

@etl.task()
def extract() -> list[dict]:
    ...
```

Use `engine.task(dag="...")` only when a DAG-scoped facade would make the code less clear.

### Task registration

- Task names default to the Python function name.
- Only set `name="..."` when you need an alias or a stable orchestration name during a refactor.
- Use `retries=` for simple retry behavior.
- Use `timeout_s=` only on `async def` tasks.

Important constraint: synchronous tasks cannot use `timeout_s`. If a sync task performs IO,
configure timeouts in the client it calls.

### Dependency declaration

Flowrun supports two main styles:

1. Inferred dependencies: omit `deps=` and use required parameter names that exactly match
   already-registered task names.
2. Explicit dependencies: pass `deps=[task_a, task_b]` when you want edges declared in the
   decorator or when inference is not appropriate.

Use explicit `deps=` for:

- forward references to tasks registered later
- non-identifier task names
- dependency aliases
- cases where you want the graph edge visible at the decorator site

If a task declares `upstream`, Flowrun passes a mapping of dependency results instead of named
keyword arguments.

### RunContext

Use `RunContext[Deps]` for runtime dependencies and run metadata.

```python
from dataclasses import dataclass

from flowrun import RunContext


@dataclass(frozen=True)
class ApiDeps:
    api_base: str
    auth_token: str


@etl.task(timeout_s=3.0)
async def fetch_users(context: RunContext[ApiDeps]) -> list[dict]:
    return await fetch_users_records(api_base=context.api_base, auth_token=context.auth_token)
```

- Access dependency data through `context.deps` or delegated attributes such as `context.api_base`.
- Use `with_metadata(...)` for identifiers such as `batch_id`, `source`, `window`, or `pipeline`.
- Flowrun can attach deadlines and cooperative cancellation to the context. If task code needs to
  cooperate, check `time_remaining_s()`, `cancelled()`, or `raise_if_cancelled()`.

### Execution helpers

Prefer the DAG-scoped helpers when writing examples or application code:

- `etl.validate()` before execution
- `await etl.run_once(context=context)` for one run
- `await etl.run_many(contexts)` for sequential micro-batches
- `await etl.run_subgraph(targets=[...], context=context)` for a partial DAG
- `engine.resume(run_id, from_tasks=[...], context=context)` for rerunning failed or selected downstream work
- `engine.get_run_report(run_id)` to inspect task statuses, results, errors, and metadata

### Hooks

Use `fn_hook(...)` for lightweight observability, notifications, or demo output. Keep hooks small
and side-effect aware.

## Recommended Coding Pattern

Structure Flowrun code in layers:

1. Plain helper functions for domain logic.
2. Typed models or schemas for inputs and outputs.
3. Thin task wrappers that connect helpers to orchestration.
4. A short `main()` that validates, runs, and prints or inspects the run report.

Good task wrappers usually do one of these:

- fetch data using values from `RunContext`
- pass an upstream result into a pure transform helper
- route a validated or aggregated result into a sink helper

Avoid putting large amounts of business logic directly inside decorated task functions unless the
task is trivial.

## Example Patterns In This Repository

### Basic demo DAG

Follow the shape in `examples/demo.py`:

- a small typed dependency bundle in `RunContext`
- one or two source tasks
- a transform task that combines upstream outputs
- a sink task that returns a location-like string
- optional hooks plus a final run report

Use this style when the goal is to explain Flowrun basics rather than showcase a specific data tool.

### Micro-batch orchestration

Follow `examples/micro_batch_demo.py` when each batch should run the same DAG with a different
context.

- Build one DAG with `etl = engine.dag("...")`.
- Expose the current batch through `RunContext`.
- Generate contexts outside the DAG.
- Use `await etl.run_many(contexts)` for sequential processing.
- Attach batch metadata with `with_metadata(...)` so the run report carries identifiers such as `batch_id`.

This pattern is for orchestration over batches, not for dynamic task generation inside the DAG.

### Polars and Pandera workflow

Follow `examples/polars_workflow_demo.py` for a realistic ETL example with validation and quarantine.

- Keep raw API calls in async helper functions.
- Normalize payloads into `pl.DataFrame` objects in plain helpers.
- Validate frames with Pandera `DataFrameModel` classes.
- Use typed wrappers like `DataFrame[UsersSchema]` or `DataFrame[OrdersSchema]` once data is validated.
- Split validation output into accepted and rejected rows, then route rejected rows to quarantine sinks.
- Keep orchestration readable by separating extract, prepare, validate, select, aggregate, and sink steps.

Useful conventions from that example:

- A generic `ValidationSplit[...]` container is a good pattern when a validation step feeds both the
  happy path and a quarantine branch.
- Inferred dependencies work well for linear branches such as `prepare_users -> validate_users -> active_users`.
- Explicit `deps=` works well when you want branch edges stated at the decorator, as in the orders branch.
- Hooks are appropriate for surfacing quarantine results or DAG completion during demos.

When authoring a new Polars workflow, prefer wrappers like this:

```python
@etl.task()
def validate_users(prepare_users: pl.DataFrame) -> ValidationSplit[UsersSchema]:
    return validate_frame(prepare_users, UsersSchema, business_object="users")
```

That keeps schema logic in reusable helpers and the DAG node focused on orchestration.

## Rules For Generated Flowrun Code

- Import from Flowrun's public API: `RunContext`, `build_default_engine`, `fn_hook`, and DAG-scope helpers.
- Use Python 3.12-compatible typing and syntax.
- Keep task names valid Python identifiers when relying on inferred dependencies.
- Register upstream tasks before downstream tasks when using inferred dependency names.
- Call `validate()` before running example DAGs unless the surrounding code already guarantees validation.
- Return structured values that are easy to inspect in `engine.get_run_report(...)`.
- Use `TypedDict`, `dataclass`, or typed DataFrame aliases instead of loose `dict[str, Any]` when practical.
- Prefer `run_many()` over manually looping `run_once()` when modeling sequential micro-batch execution.
- Do not add framework-level timeout settings to synchronous tasks.
- Do not hide major orchestration choices in metaprogramming or factories unless the user explicitly asks for that pattern.

## What To Avoid

- Monolithic task bodies that combine API calls, transforms, validation, and sink IO in one function.
- Untyped task boundaries when the workflow already has clear domain shapes.
- Forward references that rely on inferred dependencies.
- Invented Flowrun APIs that do not exist in this repo.
- Treating Flowrun as a scheduler instead of a DAG execution layer.