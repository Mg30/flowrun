import asyncio
import concurrent.futures
import time
from collections.abc import Callable
from dataclasses import dataclass

from flowrun import RunContext
from flowrun.engine import Engine
from flowrun.executor import TaskExecutor
from flowrun.scheduler import Scheduler, SchedulerConfig
from flowrun.state import StateStore
from flowrun.task import TaskRegistry, task

registry = TaskRegistry()
registry.as_default()
state_store = StateStore()
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
executor = TaskExecutor(thread_pool=thread_pool)
scheduler = Scheduler(
    registry=registry,
    state_store=state_store,
    executor=executor,
    config=SchedulerConfig(max_parallel=3),
)
eng = Engine(
    registry=registry,
    state_store=state_store,
    scheduler=scheduler,
)


@dataclass(frozen=True)
class DemoDeps:
    fake_dep: Callable[[], dict]


def demo_session_factory() -> dict:
    return {
        "base_url": "https://fake.api",
    }


demo_context = RunContext(
    DemoDeps(
        fake_dep=demo_session_factory,
    )
)


@task(
    name="fetch_api",
    deps=[],
    timeout_s=5.0,
)
def fetch_api(ctx: RunContext[DemoDeps]):
    # pretend IO call
    print("[fetch_api] hitting remote API ...")
    time.sleep(0.5)
    session = ctx.deps.fake_dep()
    print(f"[fetch_api] using session {session}")
    return {"data": [1, 2, 3], "base_url": session["base_url"]}


@task(
    name="fetch_metadata",
    deps=[],
    timeout_s=5.0,
)
async def fetch_metadata():
    print("[fetch_metadata] async metadata fetch ...")
    await asyncio.sleep(0.5)
    return {"source": "meta-service", "version": 42}


@task(
    name="process_data",
    deps=[fetch_api, fetch_metadata],
    timeout_s=10.0,
)
def process_data():
    # In MVP we don't do param passing from upstream; just pretend combine.
    print("[process_data] processing ...")
    time.sleep(0.5)
    # imagine building a report:
    return "OK: processed"


@task(
    name="store_results",
    deps=[process_data],
    timeout_s=10.0,
)
def store_results():
    print("[store_results] storing final results ...")
    time.sleep(0.2)
    # pretend to write to S3/db/etc.
    return "stored://location/key123"


async def main():
    eng.display_dag(dag_name="demo_dag")

    run_id = await eng.run_once(dag_name="demo_dag", context=demo_context)
    report = eng.get_run_report(run_id)

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


if __name__ == "__main__":
    asyncio.run(main())
