import pytest

from flowrun.dag import DAGBuilder
from flowrun.task import TaskRegistry, TaskSpec


def test_dag_builder_orders_tasks_topologically():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="extract", func=lambda: None))
    registry.register(TaskSpec(name="transform", func=lambda: None, deps=["extract"]))
    registry.register(TaskSpec(name="load", func=lambda: None, deps=["transform"]))

    builder = DAGBuilder(registry)
    dag = builder.build("etl")

    assert dag.name == "etl"
    assert dag.parents_of("extract") == []
    assert dag.parents_of("transform") == ["extract"]
    assert dag.parents_of("load") == ["transform"]

    extract_idx = dag.nodes.index("extract")
    transform_idx = dag.nodes.index("transform")
    load_idx = dag.nodes.index("load")

    assert extract_idx < transform_idx < load_idx


def test_dag_builder_raises_for_missing_dependency():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="consumer", func=lambda: None, deps=["missing"]))

    builder = DAGBuilder(registry)

    with pytest.raises(ValueError, match="which is not registered"):
        builder.build("bad")


def test_dag_builder_missing_dependency_suggests_similar_name():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="producer", func=lambda: None))
    registry.register(TaskSpec(name="consumer", func=lambda: None, deps=["producerr"]))

    builder = DAGBuilder(registry)

    with pytest.raises(ValueError, match="Did you mean 'producer'"):
        builder.build("bad")


def test_dag_builder_raises_for_cycles():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="a", func=lambda: None, deps=["b"]))
    registry.register(TaskSpec(name="b", func=lambda: None, deps=["a"]))

    builder = DAGBuilder(registry)

    with pytest.raises(ValueError, match="Cyclic dependency"):
        builder.build("cycle")


def test_dag_builder_filters_tasks_by_dag_namespace():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="extract_a", func=lambda: None, dag="etl_a"))
    registry.register(TaskSpec(name="load_a", func=lambda: None, deps=["extract_a"], dag="etl_a"))
    registry.register(TaskSpec(name="extract_b", func=lambda: None, dag="etl_b"))

    builder = DAGBuilder(registry)
    dag = builder.build("etl_a")

    assert dag.nodes == ["extract_a", "load_a"]
    assert dag.edges == {"extract_a": [], "load_a": ["extract_a"]}


def test_dag_builder_raises_when_dependency_crosses_dag_namespace():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="extract_a", func=lambda: None, dag="etl_a"))
    registry.register(TaskSpec(name="load_a", func=lambda: None, deps=["extract_b"], dag="etl_a"))
    registry.register(TaskSpec(name="extract_b", func=lambda: None, dag="etl_b"))

    builder = DAGBuilder(registry)

    with pytest.raises(ValueError, match="outside DAG"):
        builder.build("etl_a")


def test_dag_builder_raises_for_unknown_dag_when_registry_is_scoped():
    registry = TaskRegistry()
    registry.register(TaskSpec(name="extract_a", func=lambda: None, dag="etl_a"))
    registry.register(TaskSpec(name="extract_b", func=lambda: None, dag="etl_b"))

    builder = DAGBuilder(registry)

    with pytest.raises(ValueError, match="DAG 'etl_x' is not registered"):
        builder.build("etl_x")
