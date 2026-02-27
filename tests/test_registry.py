"""Tests for the redesigned TaskRegistry."""

import pytest

from flowrun.engine import build_default_engine
from flowrun.task import TaskRegistry, TaskSpec


# ---- collection protocol ----


def test_registry_contains():
    reg = TaskRegistry()
    reg.register(TaskSpec(name="a", func=lambda: None))

    assert "a" in reg
    assert "missing" not in reg


def test_registry_len():
    reg = TaskRegistry()
    assert len(reg) == 0

    reg.register(TaskSpec(name="x", func=lambda: None))
    assert len(reg) == 1


def test_registry_iter():
    reg = TaskRegistry()
    reg.register(TaskSpec(name="a", func=lambda: None))
    reg.register(TaskSpec(name="b", func=lambda: None))

    assert sorted(reg) == ["a", "b"]


def test_registry_getitem():
    reg = TaskRegistry()
    spec = TaskSpec(name="t", func=lambda: None)
    reg.register(spec)

    assert reg["t"] is spec


def test_registry_get_raises_with_context():
    reg = TaskRegistry()

    with pytest.raises(KeyError, match="not registered"):
        reg.get("nope")


def test_registry_duplicate_raises():
    reg = TaskRegistry()
    reg.register(TaskSpec(name="dup", func=lambda: None))

    with pytest.raises(ValueError, match="Duplicate"):
        reg.register(TaskSpec(name="dup", func=lambda: None))


def test_registry_task_specs_is_read_only():
    reg = TaskRegistry()
    reg.register(TaskSpec(name="a", func=lambda: None))

    view = reg.task_specs
    assert "a" in view

    with pytest.raises(TypeError):
        view["b"] = TaskSpec(name="b", func=lambda: None)  # type: ignore[index]


def test_registry_clear():
    reg = TaskRegistry()
    reg.register(TaskSpec(name="a", func=lambda: None))
    reg.clear()

    assert len(reg) == 0
    assert "a" not in reg


def test_registry_repr():
    reg = TaskRegistry()
    reg.register(TaskSpec(name="alpha", func=lambda: None))
    assert "alpha" in repr(reg)


# ---- activate / deactivate ----


def test_registry_activate_and_active():
    reg = TaskRegistry()
    token = reg.activate()
    try:
        assert TaskRegistry.active() is reg
    finally:
        TaskRegistry.deactivate(token)


def test_registry_active_raises_when_none():
    """Ensure active() raises if no registry has been activated in this context."""
    from flowrun.task import _active_registry

    token = _active_registry.set(None)
    try:
        with pytest.raises(LookupError, match="No active"):
            TaskRegistry.active()
    finally:
        _active_registry.reset(token)


def test_build_default_engine_does_not_activate_registry_globally():
    """Engine construction should not leak a global active registry."""
    from flowrun.task import _active_registry

    token = _active_registry.set(None)
    try:
        engine = build_default_engine()
        engine.close()
        with pytest.raises(LookupError, match="No active"):
            TaskRegistry.active()
    finally:
        _active_registry.reset(token)
