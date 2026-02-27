import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypedDict

from flowrun import RunContext, build_default_engine, fn_hook

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)-22s  %(levelname)-7s  %(message)s")
logger = logging.getLogger("demo_etl")

# --- Simple hook: print alerts on failure, celebrate on DAG completion ---
demo_hook = fn_hook(
    on_task_failure=lambda e: print(
        f"\n⚠ ALERT: task {e.task_name!r} failed (attempt {e.attempt}): {(e.error or '')[:80]}"
    ),
    on_dag_end=lambda e: print(f"\n✓ DAG {e.dag_name!r} finished  run_id={e.run_id}"),
)

engine = build_default_engine(max_workers=4, max_parallel=3, logger=logger, hooks=[demo_hook])


@dataclass(frozen=True)
class DemoDeps:
    """Demo dependency bundle used by the sample DAG."""

    fake_dep: Callable[[], dict]


def demo_session_factory() -> dict:
    """Return a fake API session representation for the demo DAG."""
    return {
        "base_url": "https://fake.api",
    }


demo_context = RunContext(
    DemoDeps(
        fake_dep=demo_session_factory,
    )
)


class FetchApiResult(TypedDict):
    """Return shape for `fetch_api`."""

    data: list[int]
    base_url: str


class FetchMetadataResult(TypedDict):
    """Return shape for `fetch_metadata`."""

    source: str
    version: int


class ProcessDataResult(TypedDict):
    """Return shape for `process_data`."""

    rows: int
    source: str
    version: int


@engine.task(
    name="fetch_api",
    dag="demo_dag",
    deps=[],
    timeout_s=5.0,
    retries=1,
)
def fetch_api(ctx: RunContext[DemoDeps]):
    """Simulate a synchronous IO task that requires the run context."""
    # pretend IO call
    print("[fetch_api] hitting remote API ...")
    time.sleep(0.5)
    session = ctx.deps.fake_dep()
    print(f"[fetch_api] using session {session}")
    return FetchApiResult(data=[1, 2, 3], base_url=session["base_url"])


@engine.task(
    name="fetch_metadata",
    dag="demo_dag",
    deps=[],
    timeout_s=5.0,
)
async def fetch_metadata():
    """Simulate an asynchronous metadata fetch for the demo DAG."""
    print("[fetch_metadata] async metadata fetch ...")
    await asyncio.sleep(0.5)
    return FetchMetadataResult(source="meta-service", version=42)


@engine.task(
    name="process_data",
    dag="demo_dag",
    deps=[fetch_api, fetch_metadata],
    timeout_s=10.0,
    retain_result=False,  # free intermediate memory after consumers finish
)
def process_data(fetch_api: FetchApiResult, fetch_metadata: FetchMetadataResult) -> ProcessDataResult:
    """Pretend to transform upstream results into a final data artifact."""
    print("[process_data] processing ...")
    time.sleep(0.5)
    api_payload = fetch_api["data"]
    metadata = fetch_metadata
    return ProcessDataResult(
        rows=len(api_payload),
        source=metadata["source"],
        version=metadata["version"],
    )


@engine.task(
    name="store_results",
    dag="demo_dag",
    deps=[process_data],
    timeout_s=10.0,
)
def store_results(process_data: ProcessDataResult) -> str:
    """Fake persistence step that stores the processed result."""
    print("[store_results] storing final results ...")
    time.sleep(0.2)
    # pretend to write to S3/db/etc.
    return f"stored://location/report?rows={process_data['rows']}&version={process_data['version']}"


async def main():
    """Run the demonstration DAG once and print the resulting report."""
    async with engine:
        tree = engine.display_dag(dag_name="demo_dag")
        print(tree)

        run_id = await engine.run_once(dag_name="demo_dag", context=demo_context)
        report = engine.get_run_report(run_id)

    print("\n=== RUN REPORT ===")
    print(f"run_id      : {report['run_id']}")
    print(f"dag_name    : {report['dag_name']}")
    print(f"finished_at : {report['finished_at']}")
    print("tasks:")
    for tname, info in report["tasks"].items():
        print(f"  - {tname}: {info['status']} (attempt {info['attempt']})")
        if info["error"]:
            print(f"      error  : {info['error'][:120]}...")
        if info["result"]:
            print(f"      result : {info['result']}")


async def demo_resume():
    """Show resuming a failed run (only failed/skipped tasks re-execute)."""
    async with engine:
        # First run — will succeed normally
        run_id = await engine.run_once(dag_name="demo_dag", context=demo_context)
        print(f"\n--- Original run finished: {run_id}")

        # Resume from a specific task (re-runs it + downstream)
        resumed_id = await engine.resume(run_id, from_tasks=["process_data"], context=demo_context)
        report = engine.get_run_report(resumed_id)
        print(f"\n--- Resumed run finished: {resumed_id}")
        for tname, info in report["tasks"].items():
            print(f"  {tname}: {info['status']}  (attempt {info['attempt']})")


async def demo_subgraph():
    """Show running only a sub-graph of the DAG."""
    async with engine:
        # Run only process_data and its ancestors (fetch_api, fetch_metadata)
        run_id = await engine.run_subgraph(
            dag_name="demo_dag",
            targets=["process_data"],
            context=demo_context,
        )
        report = engine.get_run_report(run_id)
        print(f"\n--- Sub-graph run finished: {run_id}")
        for tname, info in report["tasks"].items():
            print(f"  {tname}: {info['status']}  (attempt {info['attempt']})")
        # store_results is excluded — not in the sub-graph
        assert "store_results" not in report["tasks"]
        print("  (store_results was NOT part of the sub-graph)")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "resume":
        asyncio.run(demo_resume())
    elif len(sys.argv) > 1 and sys.argv[1] == "subgraph":
        asyncio.run(demo_subgraph())
    else:
        asyncio.run(main())
