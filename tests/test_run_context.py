from dataclasses import dataclass

from flowrun.context import RunContext


def test_run_context_delegates_attribute_access():
    @dataclass
    class FakeDeps:
        value: int = 42

        def double(self) -> int:
            return self.value * 2

    ctx = RunContext(FakeDeps())

    assert ctx.deps.value == 42
    assert ctx.value == 42
    assert ctx.double() == 84


def test_run_context_missing_attribute_raises():
    ctx = RunContext(deps={"existing": 1})

    assert getattr(ctx, "existing") == 1
    try:
        _ = ctx.missing
    except AttributeError:
        return
    raise AssertionError("AttributeError not raised for unknown attribute")
