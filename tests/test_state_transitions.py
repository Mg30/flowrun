"""Tests for StateStore transition validation."""

import pytest

from flowrun.state import StateStore


def test_invalid_transition_pending_to_success_raises():
    store = StateStore()
    store.create_run("r1", "dag", ["t1"])

    with pytest.raises(RuntimeError, match="Invalid state transition"):
        store.mark_success("r1", "t1", result="ok")


def test_invalid_transition_pending_to_failed_raises():
    store = StateStore()
    store.create_run("r1", "dag", ["t1"])

    with pytest.raises(RuntimeError, match="Invalid state transition"):
        store.mark_failed("r1", "t1", err="boom")


def test_valid_transition_pending_running_success():
    store = StateStore()
    store.create_run("r1", "dag", ["t1"])

    store.mark_running("r1", "t1")
    store.mark_success("r1", "t1", result=42)

    rec = store.get_run("r1")
    assert rec.tasks["t1"].status == "SUCCESS"
    assert rec.tasks["t1"].result == 42


def test_valid_transition_pending_running_failed():
    store = StateStore()
    store.create_run("r1", "dag", ["t1"])

    store.mark_running("r1", "t1")
    store.mark_failed("r1", "t1", err="oops")

    rec = store.get_run("r1")
    assert rec.tasks["t1"].status == "FAILED"
    assert rec.tasks["t1"].error == "oops"


def test_valid_transition_pending_skipped():
    store = StateStore()
    store.create_run("r1", "dag", ["t1"])

    store.mark_skipped("r1", "t1", reason="upstream")

    rec = store.get_run("r1")
    assert rec.tasks["t1"].status == "SKIPPED"


def test_double_success_raises():
    store = StateStore()
    store.create_run("r1", "dag", ["t1"])

    store.mark_running("r1", "t1")
    store.mark_success("r1", "t1", result=1)

    with pytest.raises(RuntimeError, match="Invalid state transition"):
        store.mark_success("r1", "t1", result=2)
