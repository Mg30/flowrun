import difflib
from dataclasses import dataclass

from flowrun.task import TaskRegistry


@dataclass(frozen=True)
class DAG:
    """
    Immutable, validated DAG view.
    """

    name: str
    nodes: list[str]  # task names (topologically sorted)
    edges: dict[str, list[str]]  # task_name -> list of deps

    def parents_of(self, task_name: str) -> list[str]:
        """Return the parent task names (dependencies) of the given task.

        Parameters
        ----------
        task_name : str
            Name of the task whose parents to retrieve.

        Returns
        -------
        list[str]
            List of parent task names (may be empty).
        """
        return self.edges.get(task_name, [])

    def descendants_of(self, tasks: set[str]) -> set[str]:
        """Return *tasks* plus all their transitive dependents (downstream closure).

        Parameters
        ----------
        tasks : set[str]
            Seed task names.

        Returns
        -------
        set[str]
            The union of *tasks* and every task that transitively depends on them.
        """
        children: dict[str, list[str]] = {n: [] for n in self.nodes}
        for child, parents in self.edges.items():
            for p in parents:
                children.setdefault(p, []).append(child)

        result: set[str] = set()
        stack = list(tasks)
        while stack:
            node = stack.pop()
            if node in result:
                continue
            result.add(node)
            stack.extend(children.get(node, []))
        return result

    def subgraph(self, targets: list[str]) -> "DAG":
        """Return a new DAG containing only the given targets and their transitive dependencies.

        Parameters
        ----------
        targets : list[str]
            Task names whose ancestor sub-graph should be extracted.

        Returns
        -------
        DAG
            A new, smaller DAG preserving topological order.

        Raises
        ------
        ValueError
            If any target is not present in this DAG.
        """
        needed: set[str] = set()
        stack = list(targets)
        while stack:
            node = stack.pop()
            if node in needed:
                continue
            if node not in self.edges and node not in set(self.nodes):
                raise ValueError(f"Task {node!r} is not in the DAG")
            needed.add(node)
            stack.extend(self.parents_of(node))

        nodes = [n for n in self.nodes if n in needed]
        edges = {n: [p for p in self.edges.get(n, []) if p in needed] for n in nodes}
        return DAG(name=self.name, nodes=nodes, edges=edges)


class DAGBuilder:
    """
    Builds a DAG from a TaskRegistry.
    Responsible ONLY for structure & validation.
    """

    def __init__(self, registry: TaskRegistry) -> None:
        """Initialize the DAGBuilder with the given TaskRegistry.

        Parameters
        ----------
        registry : TaskRegistry
            Registry containing task specifications.
        """
        self._registry = registry

    def dag_names(self) -> list[str]:
        """Return sorted DAG names explicitly declared on task specs."""
        return sorted({spec.dag for spec in self._registry.task_specs.values() if spec.dag is not None})

    def build(self, dag_name: str) -> DAG:
        """Build a DAG from the registered tasks, validating dependencies and ordering.

        Parameters
        ----------
        dag_name : str
            Name to assign to the constructed DAG.

        Returns
        -------
        DAG
            Immutable, validated DAG view containing nodes in topological order and the edges map.

        Raises
        ------
        ValueError
            If a task depends on another task that is not registered, or if a cyclic dependency is detected.
        """
        all_tasks = self._registry.task_specs
        scoped = {tname: spec for tname, spec in all_tasks.items() if spec.dag == dag_name}
        if scoped:
            tasks = scoped
        else:
            explicit_dags = self.dag_names()
            if explicit_dags:
                msg = f"DAG {dag_name!r} is not registered."
                suggestion = difflib.get_close_matches(dag_name, explicit_dags, n=1)
                if suggestion:
                    msg += f" Did you mean {suggestion[0]!r}?"
                msg += f" Available DAGs: {', '.join(explicit_dags)}."
                raise ValueError(msg)
            # Legacy mode: if no task is explicitly scoped, preserve old behavior
            # where one registry corresponds to one DAG.
            tasks = all_tasks

        # 1. validate missing deps
        for tname, spec in tasks.items():
            for d in spec.deps:
                if d not in tasks:
                    if d in all_tasks:
                        raise ValueError(
                            f"Task {tname} depends on {d} which is outside DAG {dag_name!r}. "
                            "Dependencies must be in the same DAG namespace."
                        )
                    msg = f"Task {tname} depends on {d} which is not registered."
                    suggestion = difflib.get_close_matches(d, list(all_tasks.keys()), n=1)
                    if suggestion:
                        msg += f" Did you mean {suggestion[0]!r}?"
                    raise ValueError(msg)

        # 2. topo sort + cycle detection
        edges = {tname: list(spec.deps) for tname, spec in tasks.items()}
        order = self._toposort(edges)

        return DAG(name=dag_name, nodes=order, edges=edges)

    def _toposort(self, edges: dict[str, list[str]]) -> list[str]:
        visited: set[str] = set()
        temp: set[str] = set()
        result: list[str] = []

        def visit(node: str):
            if node in temp:
                raise ValueError(f"Cyclic dependency detected at {node}")
            if node not in visited:
                temp.add(node)
                for parent in edges.get(node, []):
                    visit(parent)
                temp.remove(node)
                visited.add(node)
                result.append(node)

        for node in edges.keys():
            visit(node)

        # DFS appends nodes after visiting their parents, so result already
        # places dependencies before dependents. Preserve that deterministic
        # order.
        return result
