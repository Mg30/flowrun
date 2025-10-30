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
        tasks = self._registry.all()
        # 1. validate missing deps
        for tname, spec in tasks.items():
            for d in spec.deps:
                if d not in tasks:
                    raise ValueError(f"Task {tname} depends on {d} which is not registered")

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
