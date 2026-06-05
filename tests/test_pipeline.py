import pytest

from flowrun import Pipeline


@pytest.mark.asyncio
async def test_pipeline_override_tasks_replaces_upstream_result_by_name():
    pipeline = Pipeline("etl", max_workers=2, max_parallel=2)
    seen: list[str] = []

    @pipeline.task(name="extract")
    def extract() -> list[int]:
        seen.append("extract")
        return [1, 2]

    @pipeline.task(name="sum_values", deps=[extract])
    def sum_values(extract: list[int]) -> int:
        seen.append(f"sum:{sum(extract)}")
        return sum(extract)

    test_pipeline = pipeline.override_tasks(extract=[10, 20])

    async with pipeline:
        run_id = await test_pipeline.run_once()
        report = test_pipeline.get_run_report(run_id)

    assert report["tasks"]["extract"]["result"] == [10, 20]
    assert report["tasks"]["sum_values"]["result"] == 30
    assert seen == ["sum:30"]


def test_pipeline_override_tasks_rejects_unknown_task_names():
    pipeline = Pipeline("etl", max_workers=1, max_parallel=1)

    @pipeline.task(name="extract")
    def extract() -> str:
        return "ok"

    with pytest.raises(ValueError, match="unknown pipeline tasks"):
        pipeline.override_tasks(missing="nope")


def test_pipeline_exposes_built_dag_introspection():
    pipeline = Pipeline("etl", max_workers=1, max_parallel=1)

    @pipeline.task(name="extract")
    def extract() -> str:
        return "ok"

    @pipeline.task(name="load", deps=[extract])
    def load(extract: str) -> str:
        return extract

    assert pipeline.name == "etl"
    assert pipeline.tasks == ("extract", "load")
    assert pipeline.dependencies == {"extract": (), "load": ("extract",)}
    assert pipeline.list_tasks() == ["extract", "load"]


@pytest.mark.asyncio
async def test_pipeline_get_run_report_delegates_to_engine_report():
    pipeline = Pipeline("etl", max_workers=1, max_parallel=1)

    @pipeline.task(name="extract")
    def extract() -> str:
        return "ok"

    async with pipeline:
        run_id = await pipeline.run_once()
        report = pipeline.get_run_report(run_id)

    assert report["run_id"] == run_id
    assert report["dag_name"] == "etl"
    assert report["tasks"]["extract"]["result"] == "ok"


@pytest.mark.asyncio
async def test_pipeline_resume_reruns_selected_downstream_tasks():
    pipeline = Pipeline("etl", max_workers=1, max_parallel=1)
    seen: list[str] = []

    @pipeline.task(name="extract")
    def extract() -> str:
        seen.append("extract")
        return "raw"

    @pipeline.task(name="load", deps=[extract])
    def load(extract: str) -> str:
        seen.append("load")
        return extract.upper()

    async with pipeline:
        run_id = await pipeline.run_once()
        resumed_id = await pipeline.resume(run_id, from_tasks=["load"])
        report = pipeline.get_run_report(resumed_id)

    assert report["tasks"]["extract"]["result"] == "raw"
    assert report["tasks"]["load"]["result"] == "RAW"
    assert seen == ["extract", "load", "load"]


def test_top_level_api_does_not_export_engine_helpers():
    import flowrun

    assert hasattr(flowrun, "Pipeline")
    assert not hasattr(flowrun, "Engine")
    assert not hasattr(flowrun, "DagScope")
    assert not hasattr(flowrun, "build_default_engine")
