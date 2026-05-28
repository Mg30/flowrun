import pytest

from flowrun.engine import build_default_engine


@pytest.mark.asyncio
async def test_pipeline_override_tasks_replaces_upstream_result_by_name():
    engine = build_default_engine(max_workers=2, max_parallel=2)
    etl = engine.dag("etl")
    seen: list[str] = []

    @etl.task(name="extract")
    def extract() -> list[int]:
        seen.append("extract")
        return [1, 2]

    @etl.task(name="sum_values", deps=[extract])
    def sum_values(extract: list[int]) -> int:
        seen.append(f"sum:{sum(extract)}")
        return sum(extract)

    pipeline = etl.build().override_tasks(extract=[10, 20])

    async with engine:
        run_id = await pipeline.run_once()
        report = pipeline.get_run_report(run_id)

    assert report["tasks"]["extract"]["result"] == [10, 20]
    assert report["tasks"]["sum_values"]["result"] == 30
    assert seen == ["sum:30"]


def test_pipeline_override_tasks_rejects_unknown_task_names():
    engine = build_default_engine(max_workers=1, max_parallel=1)
    etl = engine.dag("etl")

    @etl.task(name="extract")
    def extract() -> str:
        return "ok"

    pipeline = engine.build("etl")

    with pytest.raises(ValueError, match="unknown pipeline tasks"):
        pipeline.override_tasks(missing="nope")


def test_pipeline_exposes_built_dag_introspection():
    engine = build_default_engine(max_workers=1, max_parallel=1)
    etl = engine.dag("etl")

    @etl.task(name="extract")
    def extract() -> str:
        return "ok"

    @etl.task(name="load", deps=[extract])
    def load(extract: str) -> str:
        return extract

    pipeline = etl.build()

    assert pipeline.name == "etl"
    assert pipeline.tasks == ("extract", "load")
    assert pipeline.dependencies == {"extract": (), "load": ("extract",)}
    assert pipeline.list_tasks() == ["extract", "load"]


@pytest.mark.asyncio
async def test_pipeline_get_run_report_delegates_to_engine_report():
    engine = build_default_engine(max_workers=1, max_parallel=1)
    etl = engine.dag("etl")

    @etl.task(name="extract")
    def extract() -> str:
        return "ok"

    pipeline = etl.build()

    async with engine:
        run_id = await pipeline.run_once()
        report = pipeline.get_run_report(run_id)

    assert report["run_id"] == run_id
    assert report["dag_name"] == "etl"
    assert report["tasks"]["extract"]["result"] == "ok"
