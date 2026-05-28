import asyncio
import logging
from dataclasses import dataclass
from typing import TypedDict

from flowrun import RunContext, build_default_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)-22s  %(levelname)-7s  %(message)s")
logger = logging.getLogger("micro_batch_demo")

engine = build_default_engine(max_workers=4, max_parallel=2, logger=logger)
etl = engine.dag("micro_batch_demo")


@dataclass(frozen=True)
class ChunkDeps:
    """Per-chunk dependencies passed into one DAG run."""

    chunk_index: int
    rows: list[dict[str, int]]


class InputChunkResult(TypedDict):
    """Structured payload produced by the input adapter task."""

    chunk_index: int
    rows: list[dict[str, int]]


# Task names default to the Python function name. Use name="chunk_input_v2"
# only when you need an alias or a stable orchestration name during refactors.
@etl.task()
def input_chunk(context: RunContext[ChunkDeps]) -> InputChunkResult:
    """Expose the current chunk from the run context as normal task input."""
    return {
        "chunk_index": context.chunk_index,
        "rows": context.rows,
    }


@etl.task(deps=[input_chunk])
def transform_chunk(input_chunk: InputChunkResult) -> dict[str, int]:
    """Summarise the current chunk without knowing about orchestration."""
    rows = input_chunk["rows"]
    chunk_index = input_chunk["chunk_index"]
    return {
        "chunk_index": chunk_index,
        "rows": len(rows),
        "total": sum(row["value"] for row in rows),
    }


@etl.task(deps=[transform_chunk])
def load_chunk(transform_chunk: dict[str, int]) -> str:
    """Return a fake sink result for the processed chunk."""
    return (
        f"chunk={transform_chunk['chunk_index']} loaded rows={transform_chunk['rows']} total={transform_chunk['total']}"
    )


async def fetch_chunk_contexts():
    """Yield fake chunk contexts from an async source outside the DAG."""
    for chunk_index in range(3):
        await asyncio.sleep(0.1)
        rows = [{"value": chunk_index * 10 + offset} for offset in range(3)]
        yield RunContext(ChunkDeps(chunk_index=chunk_index, rows=rows)).with_metadata(
            batch_id=chunk_index,
            source="demo_chunks",
        )


async def main() -> None:
    """Run the same DAG once per chunk from the async source."""
    async with engine:
        pipeline = etl.build()
        run_ids = await pipeline.run_many(fetch_chunk_contexts())

        print("=== MICRO-BATCH RUNS ===")
        for run_id in run_ids:
            report = pipeline.get_run_report(run_id)
            batch_id = report["metadata"]["batch_id"]
            print(f"batch={batch_id}  {run_id}: {report['tasks']['load_chunk']['result']}")


if __name__ == "__main__":
    asyncio.run(main())
