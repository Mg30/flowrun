"""Shared fixtures for the flowrun test suite."""

import concurrent.futures

import pytest

from flowrun.executor import TaskExecutor
from flowrun.state import StateStore
from flowrun.task import TaskRegistry


@pytest.fixture
def registry() -> TaskRegistry:
    """Return a fresh TaskRegistry."""
    return TaskRegistry()


@pytest.fixture
def state_store() -> StateStore:
    """Return a fresh in-memory StateStore."""
    return StateStore()


@pytest.fixture
def thread_pool():
    """Yield a single-worker ThreadPoolExecutor, shut down after use."""
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    yield pool
    pool.shutdown(wait=True)


@pytest.fixture
def task_executor(thread_pool) -> TaskExecutor:
    """Return a TaskExecutor backed by the shared thread pool."""
    return TaskExecutor(executor=thread_pool)
