"""Layered Flowrun example with Polars validation, quarantine, and orchestration."""

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import TypedDict, cast

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaErrors
from pandera.typing.polars import DataFrame, Series

from flowrun import Pipeline, RunContext, fn_hook

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)-22s  %(levelname)-7s  %(message)s")
logger = logging.getLogger("polars_workflow_demo")

# Small example hook: surface quarantine outputs and mark the end of the run.
polars_hook = fn_hook(
    on_task_success=lambda e: print(f"[hook] {e.task_name}: {e.result}")
    if e.task_name in {"quarantine_users", "quarantine_orders"}
    else None,
    on_dag_end=lambda e: print(f"[hook] DAG {e.dag_name} finished  run_id={e.run_id}"),
)

pipeline = Pipeline("polars_workflow_demo", max_workers=4, max_parallel=3, logger=logger, hooks=[polars_hook])


@dataclass(frozen=True)
class ApiDeps:
    """Runtime dependencies shared by the fake API tasks."""

    api_base: str
    auth_token: str


class UserRecord(TypedDict):
    """Raw payload row returned by the fake users endpoint."""

    user_id: int | None
    country: str | None
    segment: str | None
    is_active: bool | None


class OrderRecord(TypedDict):
    """Raw payload row returned by the fake orders endpoint."""

    order_id: int | None
    user_id: int | None
    amount: float | None
    status: str | None


@dataclass(frozen=True)
class ValidationSplit[SchemaModel: pa.DataFrameModel]:
    """Validated and rejected rows produced by one schema check."""

    validated: DataFrame[SchemaModel]
    rejected: pl.DataFrame


class UsersSchema(pa.DataFrameModel):
    """Validated users schema used after the users quality gate."""

    user_id: Series[int] = pa.Field(gt=0)
    country: Series[str] = pa.Field(isin=["FR", "DE", "ES"])
    segment: Series[str] = pa.Field(isin=["enterprise", "mid_market", "startup"])
    is_active: Series[bool]


class OrdersSchema(pa.DataFrameModel):
    """Validated orders schema used after the orders quality gate."""

    order_id: Series[int] = pa.Field(gt=0)
    user_id: Series[int] = pa.Field(gt=0)
    amount: Series[float] = pa.Field(gt=0)
    status: Series[str] = pa.Field(isin=["paid", "cancelled"])


class ActiveUsersSchema(pa.DataFrameModel):
    """Projected active-users schema used by the summary join."""

    user_id: Series[int] = pa.Field(gt=0)
    country: Series[str] = pa.Field(isin=["FR", "DE", "ES"])
    segment: Series[str] = pa.Field(isin=["enterprise", "mid_market", "startup"])


class PaidOrdersSchema(pa.DataFrameModel):
    """Projected paid-orders schema used by the summary join."""

    order_id: Series[int] = pa.Field(gt=0)
    user_id: Series[int] = pa.Field(gt=0)
    amount: Series[float] = pa.Field(gt=0)


class SalesSummarySchema(pa.DataFrameModel):
    """Aggregated sales summary schema produced by the workflow."""

    country: Series[str] = pa.Field(isin=["FR", "DE", "ES"])
    segment: Series[str] = pa.Field(isin=["enterprise", "mid_market", "startup"])
    orders: Series[int] = pa.Field(ge=0)
    revenue: Series[float] = pa.Field(ge=0)
    customers: Series[int] = pa.Field(ge=0)


async def fetch_users_records(*, api_base: str, auth_token: str) -> list[UserRecord]:
    """Return fake users data after simulating remote latency."""
    del auth_token
    await asyncio.sleep(0.2)
    logger.info("Fetched users from %s/users", api_base)
    return [
        {"user_id": 1, "country": "fr", "segment": "enterprise", "is_active": True},
        {"user_id": 2, "country": "de", "segment": "mid_market", "is_active": True},
        {"user_id": 3, "country": "zz", "segment": "startup", "is_active": True},
        {"user_id": 4, "country": "fr", "segment": None, "is_active": True},
        {"user_id": 4, "country": "es", "segment": "enterprise", "is_active": True},
    ]


async def fetch_orders_records(*, api_base: str, auth_token: str) -> list[OrderRecord]:
    """Return fake orders data after simulating remote latency."""
    del auth_token
    await asyncio.sleep(0.2)
    logger.info("Fetched orders from %s/orders", api_base)
    return [
        {"order_id": 101, "user_id": 1, "amount": 120.0, "status": "PAID"},
        {"order_id": 102, "user_id": 1, "amount": 80.0, "status": "PAID"},
        {"order_id": 103, "user_id": 2, "amount": 50.0, "status": "CANCELLED"},
        {"order_id": 104, "user_id": 2, "amount": -10.0, "status": "PAID"},
        {"order_id": 105, "user_id": None, "amount": 25.0, "status": "PAID"},
        {"order_id": 104, "user_id": 4, "amount": 210.0, "status": "PAID"},
    ]


def normalize_users(records: list[UserRecord]) -> pl.DataFrame:
    """Convert raw users payloads into a clean users DataFrame."""
    return (
        pl.DataFrame(records)
        .with_columns(
            pl.col("country").str.to_uppercase(),
            pl.col("segment").str.to_lowercase(),
        )
        .select(["user_id", "country", "segment", "is_active"])
        .sort("user_id")
    )


def normalize_orders(records: list[OrderRecord]) -> pl.DataFrame:
    """Convert raw orders payloads into a clean orders DataFrame."""
    return (
        pl.DataFrame(records)
        .with_columns(pl.col("status").str.to_lowercase())
        .select(["order_id", "user_id", "amount", "status"])
        .sort("order_id")
    )


def validate_frame[SchemaModel: pa.DataFrameModel](
    df: pl.DataFrame,
    schema: type[SchemaModel],
    *,
    business_object: str,
) -> ValidationSplit[SchemaModel]:
    """Validate a frame and route bad rows into a quarantine-ready DataFrame."""
    indexed_df = df.with_row_index("index")
    metadata_exprs = [
        pl.lit("fake_api").alias("source_system"),
        pl.lit(business_object).alias("business_object"),
        pl.lit(date.today()).alias("ingestion_date"),
    ]

    try:
        validated_df = schema.validate(df, lazy=True).with_columns(*metadata_exprs)
        rejected_df = df.head(0).with_columns(
            pl.lit(None, dtype=pl.List(pl.String)).alias("rejection_reason"),
            *metadata_exprs,
        )
        return ValidationSplit(validated=cast(DataFrame[SchemaModel], validated_df), rejected=rejected_df)
    except SchemaErrors as exc:
        rejection_map = (
            exc.failure_cases.with_columns(
                pl.col("index").cast(pl.UInt32),
                pl.concat_str(
                    [
                        pl.col("column").fill_null("__dataframe__"),
                        pl.col("check").fill_null("schema_error"),
                    ],
                    separator=": ",
                ).alias("rejection_reason"),
            )
            .group_by("index")
            .agg(pl.col("rejection_reason"))
        )

        validated_df = (
            indexed_df.join(rejection_map.select("index"), on="index", how="anti")
            .drop("index")
            .with_columns(*metadata_exprs)
        )
        rejected_df = (
            indexed_df.join(rejection_map, on="index", how="inner").drop("index").with_columns(*metadata_exprs)
        )
        return ValidationSplit(validated=cast(DataFrame[SchemaModel], validated_df), rejected=rejected_df)


def select_active_users(users_df: DataFrame[UsersSchema]) -> DataFrame[ActiveUsersSchema]:
    """Keep only active validated users for downstream joins."""
    result = users_df.filter(pl.col("is_active")).select(["user_id", "country", "segment"])
    return cast(DataFrame[ActiveUsersSchema], result)


def select_paid_orders(orders_df: DataFrame[OrdersSchema]) -> DataFrame[PaidOrdersSchema]:
    """Keep only paid validated orders for downstream joins."""
    result = orders_df.filter(pl.col("status") == "paid").select(["order_id", "user_id", "amount"])
    return cast(DataFrame[PaidOrdersSchema], result)


def build_sales_summary(
    users_df: DataFrame[ActiveUsersSchema],
    orders_df: DataFrame[PaidOrdersSchema],
) -> DataFrame[SalesSummarySchema]:
    """Join processed users and orders, then aggregate a small sales summary."""
    result = (
        orders_df.join(users_df, on="user_id", how="inner")
        .group_by(["country", "segment"])
        .agg(
            pl.len().alias("orders"),
            pl.col("amount").sum().alias("revenue"),
            pl.col("user_id").n_unique().alias("customers"),
        )
        .sort(["country", "segment"])
    )
    return cast(DataFrame[SalesSummarySchema], result)


def fake_sink(summary_df: DataFrame[SalesSummarySchema]) -> str:
    """Pretend to persist the summary and return a sink location."""
    total_revenue = float(summary_df["revenue"].sum()) if summary_df.height else 0.0
    return f"sink://sales-summary?rows={summary_df.height}&revenue={total_revenue:.2f}"


def fake_quarantine_sink(rejected_df: pl.DataFrame, *, quarantine_name: str) -> str:
    """Pretend to persist rejected rows into a quarantine location."""
    return f"quarantine://{quarantine_name}?rows={rejected_df.height}"


# Task names default to the function name. Pass name="users_extract_v2" only
# when you want a task name that differs from the Python symbol.
@pipeline.task(timeout_s=3.0)
async def fetch_users_raw(context: RunContext[ApiDeps]) -> list[UserRecord]:
    """Thin orchestration wrapper for the users endpoint."""
    return await fetch_users_records(api_base=context.api_base, auth_token=context.auth_token)


@pipeline.task(timeout_s=3.0)
async def fetch_orders_raw(context: RunContext[ApiDeps]) -> list[OrderRecord]:
    """Thin orchestration wrapper for the orders endpoint."""
    return await fetch_orders_records(api_base=context.api_base, auth_token=context.auth_token)


# Users branch: infer dependency edges from required parameter names.
@pipeline.task()
def prepare_users(fetch_users_raw: list[UserRecord]) -> pl.DataFrame:
    """Thin orchestration wrapper around the users normalisation function."""
    return normalize_users(fetch_users_raw)


@pipeline.task()
def validate_users(prepare_users: pl.DataFrame) -> ValidationSplit[UsersSchema]:
    """Thin orchestration wrapper around the users schema validation function."""
    return validate_frame(prepare_users, UsersSchema, business_object="users")


@pipeline.task()
def active_users(validate_users: ValidationSplit[UsersSchema]) -> DataFrame[ActiveUsersSchema]:
    """Thin orchestration wrapper around the active-users filter."""
    return select_active_users(validate_users.validated)


@pipeline.task()
def quarantine_users(validate_users: ValidationSplit[UsersSchema]) -> str:
    """Thin orchestration wrapper around the users quarantine sink."""
    return fake_quarantine_sink(validate_users.rejected, quarantine_name="users")


# Orders branch: keep explicit deps when you want graph edges declared in the decorator.
@pipeline.task(deps=[fetch_orders_raw])
def prepare_orders(fetch_orders_raw: list[OrderRecord]) -> pl.DataFrame:
    """Thin orchestration wrapper around the orders normalisation function."""
    return normalize_orders(fetch_orders_raw)


@pipeline.task(deps=[prepare_orders])
def validate_orders(prepare_orders: pl.DataFrame) -> ValidationSplit[OrdersSchema]:
    """Thin orchestration wrapper around the orders schema validation function."""
    return validate_frame(prepare_orders, OrdersSchema, business_object="orders")


@pipeline.task(deps=[validate_orders])
def paid_orders(validate_orders: ValidationSplit[OrdersSchema]) -> DataFrame[PaidOrdersSchema]:
    """Thin orchestration wrapper around the paid-orders filter."""
    return select_paid_orders(validate_orders.validated)


@pipeline.task(deps=[validate_orders])
def quarantine_orders(validate_orders: ValidationSplit[OrdersSchema]) -> str:
    """Thin orchestration wrapper around the orders quarantine sink."""
    return fake_quarantine_sink(validate_orders.rejected, quarantine_name="orders")


@pipeline.task(deps=[active_users, paid_orders])
def build_summary(
    active_users: DataFrame[ActiveUsersSchema],
    paid_orders: DataFrame[PaidOrdersSchema],
) -> DataFrame[SalesSummarySchema]:
    """Thin orchestration wrapper around the join and aggregation logic."""
    return build_sales_summary(active_users, paid_orders)


@pipeline.task(deps=[build_summary])
def sink_summary(build_summary: DataFrame[SalesSummarySchema]) -> str:
    """Thin orchestration wrapper around the sink function."""
    return fake_sink(build_summary)


async def main() -> None:
    """Run the layered Polars workflow once and print the final outputs."""
    demo_token = os.environ.get("FLOWRUN_DEMO_TOKEN", "demo-token")
    context = RunContext(ApiDeps(api_base="https://fake.api.local", auth_token=demo_token)).with_metadata(
        source="fake_api",
        pipeline="sales_summary",
        batch_date=str(date.today()),
    )

    async with pipeline:
        print(pipeline.display())
        run_id = await pipeline.run_once(context=context)
        report = pipeline.get_run_report(run_id)

    print("\n=== FINAL SUMMARY ===")
    print(report["tasks"]["build_summary"]["result"])
    print("\n=== RUN METADATA ===")
    print(report["metadata"])
    print("\n=== SINK RESULT ===")
    print(report["tasks"]["sink_summary"]["result"])
    print("\n=== QUARANTINE RESULTS ===")
    print(report["tasks"]["quarantine_users"]["result"])
    print(report["tasks"]["quarantine_orders"]["result"])


if __name__ == "__main__":
    asyncio.run(main())
