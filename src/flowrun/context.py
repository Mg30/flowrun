from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RunContext[DepsT]:
    """Container that exposes user-defined dependencies to task functions.

    The user is responsible for instantiating this context with a strongly-typed
    dependency bundle; tasks can then declare ``RunContext[MyDeps]`` to gain IDE
    and type-checker support while still accessing attributes directly.
    """

    deps: DepsT

    def __getattr__(self, item: str) -> Any:
        """Delegate attribute access to the wrapped dependency bundle.

        Allows attribute lookup on the RunContext to fall back to the underlying
        deps object (e.g., ctx.session_factory -> deps.session_factory).
        """
        # Delegate attribute access to the wrapped dependency bundle. This keeps
        # task code ergonomic: ctx.session_factory -> deps.session_factory.
        try:
            return getattr(self.deps, item)
        except AttributeError as exc:
            if isinstance(self.deps, Mapping) and item in self.deps:
                return self.deps[item]
            raise AttributeError(item) from exc
