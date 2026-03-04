"""Tests for the decorator registry wiring (TaskFlow pattern)."""

import warnings

import pytest

from databricks_bundle_decorators.registry import (
    ClusterMeta,
    DuplicateResourceError,
    _CLUSTER_REGISTRY,
    _JOB_REGISTRY,
    _TASK_REGISTRY,
    reset_registries,
)
from databricks_bundle_decorators.decorators import (
    job,
    job_cluster,
    task,
    for_each_task,
    task_value,
)


class TestTaskDecorator:
    def setup_method(self):
        reset_registries()

    def test_standalone_task(self):
        """A @task defined outside a @job body is callable normally."""

        @task
        def my_task():
            return 42

        assert "my_task" in _TASK_REGISTRY
        assert _TASK_REGISTRY["my_task"].io_manager is None
        assert my_task() == 42

    def test_with_io_manager(self):
        from databricks_bundle_decorators.io_manager import (
            IoManager,
            OutputContext,
            InputContext,
        )
        from typing import Any

        class FakeIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                return None

        fake = FakeIo()

        @task(io_manager=fake)
        def my_task():
            return 1

        assert _TASK_REGISTRY["my_task"].io_manager is fake


class TestJobCluster:
    def setup_method(self):
        reset_registries()

    def test_cluster_registration(self):
        job_cluster(
            name="test_cluster", spark_version="13.2.x-scala2.12", num_workers=1
        )

        assert "test_cluster" in _CLUSTER_REGISTRY
        assert _CLUSTER_REGISTRY["test_cluster"].spec["num_workers"] == 1

    def test_cluster_returns_cluster_meta(self):
        result = job_cluster(name="my_cluster", spark_version="14.0.x-scala2.12")

        assert isinstance(result, ClusterMeta)
        assert result.name == "my_cluster"
        assert "my_cluster" in _CLUSTER_REGISTRY

    def test_duplicate_cluster_raises(self):
        job_cluster(name="dup_cluster", spark_version="13.2.x-scala2.12")

        with pytest.raises(
            DuplicateResourceError, match="Duplicate job_cluster 'dup_cluster'"
        ):
            job_cluster(name="dup_cluster", spark_version="14.0.x-scala2.12")


class TestJobDecorator:
    def setup_method(self):
        reset_registries()

    def test_inline_dag_extraction(self):
        """Tasks defined inside @job body produce correct DAG."""
        test_cluster = job_cluster(
            name="default", spark_version="13.2.x-scala2.12", num_workers=1
        )

        @job(
            tags={"env": "test"},
            params={"url": "http://example.com"},
            cluster=test_cluster,
        )
        def my_job():
            @task
            def step_a():
                pass

            @task
            def step_b(data):
                pass

            x = step_a()
            step_b(x)

        assert "my_job" in _JOB_REGISTRY
        meta = _JOB_REGISTRY["my_job"]
        assert meta.sdk_config["tags"] == {"env": "test"}
        assert meta.dag["step_a"] == []
        assert meta.dag["step_b"] == ["step_a"]
        assert meta.dag_edges["step_b"] == {"data": "step_a"}

    def test_qualified_task_keys_registered(self):
        @job
        def my_job():
            @task
            def alpha():
                pass

            @task
            def beta(x):
                pass

            r = alpha()
            beta(r)

        assert "my_job.alpha" in _TASK_REGISTRY
        assert "my_job.beta" in _TASK_REGISTRY

    def test_fan_in_dag(self):
        """Multiple upstream tasks feeding into one downstream task."""

        @job
        def fan_job():
            @task
            def fan_a():
                pass

            @task
            def fan_b():
                pass

            @task
            def merge(a, b):
                pass

            a = fan_a()
            b = fan_b()
            merge(a, b)

        meta = _JOB_REGISTRY["fan_job"]
        assert meta.dag["fan_a"] == []
        assert meta.dag["fan_b"] == []
        assert set(meta.dag["merge"]) == {"fan_a", "fan_b"}
        assert meta.dag_edges["merge"] == {"a": "fan_a", "b": "fan_b"}

    def test_independent_tasks(self):
        """Tasks with no data dependency (side effects only)."""

        @job
        def side_job():
            @task
            def task_a():
                pass

            @task
            def task_b():
                pass

            task_a()
            task_b()

        meta = _JOB_REGISTRY["side_job"]
        assert meta.dag["task_a"] == []
        assert meta.dag["task_b"] == []

    def test_kwarg_edge(self):
        """Dependency passed as keyword argument."""

        @job
        def kw_job():
            @task
            def producer():
                pass

            @task
            def consumer(df):
                pass

            result = producer()
            consumer(df=result)

        meta = _JOB_REGISTRY["kw_job"]
        assert meta.dag["consumer"] == ["producer"]
        assert meta.dag_edges["consumer"] == {"df": "producer"}

    def test_duplicate_job_raises(self):
        @job
        def dup_job():
            @task
            def noop():
                pass

            noop()

        with pytest.raises(DuplicateResourceError, match="Duplicate job 'dup_job'"):

            @job
            def dup_job():
                @task
                def noop():
                    pass

                noop()

    def test_string_cluster_raises_type_error(self):
        """Passing a string instead of ClusterMeta raises TypeError."""
        with pytest.raises(TypeError, match="expects a ClusterMeta"):

            @job(cluster="some_cluster")  # type: ignore[arg-type]  # intentional wrong type
            def bad_job():
                @task
                def noop():
                    pass

                noop()


class TestSdkConfigForwarding:
    """SDK-native fields passed via **kwargs are stored in meta."""

    def setup_method(self):
        reset_registries()

    def test_task_sdk_config(self):
        @job
        def cfg_job():
            @task(max_retries=3, timeout_seconds=1800)
            def my_task():
                pass

            my_task()

        meta = _TASK_REGISTRY["cfg_job.my_task"]
        assert meta.sdk_config == {"max_retries": 3, "timeout_seconds": 1800}

    def test_task_sdk_config_default_empty(self):
        @task
        def plain():
            pass

        assert _TASK_REGISTRY["plain"].sdk_config == {}

    def test_job_sdk_config(self):
        @job(max_concurrent_runs=2, timeout_seconds=7200)
        def cfg_job():
            @task
            def noop():
                pass

            noop()

        meta = _JOB_REGISTRY["cfg_job"]
        assert meta.sdk_config == {"max_concurrent_runs": 2, "timeout_seconds": 7200}

    def test_job_sdk_config_default_empty(self):
        @job
        def plain_job():
            @task
            def noop():
                pass

            noop()

        assert _JOB_REGISTRY["plain_job"].sdk_config == {}

    def test_libraries_default_none(self):
        """Libraries default to None (codegen uses dist/*.whl fallback)."""

        @job
        def lib_default_job():
            @task
            def noop():
                pass

            noop()

        assert _JOB_REGISTRY["lib_default_job"].libraries is None

    def test_libraries_empty_list(self):
        """Setting libraries=[] suppresses the default wheel library (Docker images)."""

        @job(libraries=[])
        def docker_job():
            @task
            def noop():
                pass

            noop()

        assert _JOB_REGISTRY["docker_job"].libraries == []

    def test_libraries_custom_list(self):
        """Custom library objects are stored as-is."""
        sentinel = object()

        @job(libraries=[sentinel])
        def custom_lib_job():
            @task
            def noop():
                pass

            noop()

        assert _JOB_REGISTRY["custom_lib_job"].libraries == [sentinel]

    def test_job_convenience_and_sdk_combined(self):
        """Managed params and SDK params coexist."""
        test_cluster = job_cluster(
            name="combo_cluster", spark_version="13.2.x-scala2.12", num_workers=1
        )

        @job(
            tags={"team": "data"},
            params={"url": "http://example.com"},
            cluster=test_cluster,
            max_concurrent_runs=1,
            description="My pipeline",
        )
        def combo_job():
            @task(max_retries=2)
            def step():
                pass

            step()

        job_meta = _JOB_REGISTRY["combo_job"]
        assert job_meta.params == {"url": "http://example.com"}
        assert job_meta.sdk_config == {
            "tags": {"team": "data"},
            "max_concurrent_runs": 1,
            "description": "My pipeline",
        }
        task_meta = _TASK_REGISTRY["combo_job.step"]
        assert task_meta.sdk_config == {"max_retries": 2}


class TestJobBodySafeguard:
    """Warn when non-TaskProxy arguments are passed to task calls in @job body."""

    def setup_method(self):
        reset_registries()

    def test_non_proxy_positional_arg_warns(self):
        """Passing real data as a positional arg triggers a warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @job
            def bad_job():
                @task
                def process(data):
                    pass

                process([1, 2, 3])

            assert len(w) == 1
            assert "non-TaskProxy argument" in str(w[0].message)
            assert "'data'" in str(w[0].message)
            assert "'list'" in str(w[0].message)

    def test_non_proxy_kwarg_warns(self):
        """Passing real data as a keyword arg triggers a warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @job
            def kw_bad_job():
                @task
                def process(data):
                    pass

                process(data={"key": "value"})

            assert len(w) == 1
            assert "non-TaskProxy argument" in str(w[0].message)
            assert "'data'" in str(w[0].message)
            assert "'dict'" in str(w[0].message)

    def test_none_arg_no_warning(self):
        """Passing None does not trigger a warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @job
            def none_job():
                @task
                def process(data):
                    pass

                process(None)

            assert len(w) == 0

    def test_task_proxy_arg_no_warning(self):
        """Passing a TaskProxy (normal DAG wiring) does not trigger a warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @job
            def good_job():
                @task
                def step_a():
                    pass

                @task
                def step_b(data):
                    pass

                x = step_a()
                step_b(x)

            assert len(w) == 0

    def test_string_arg_warns(self):
        """Passing a string constant triggers a warning — it's discarded at runtime."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @job
            def str_job():
                @task
                def process(mode):
                    pass

                process("fast")

            assert len(w) == 1
            assert "'str'" in str(w[0].message)

    def test_mixed_proxy_and_literal_warns_once(self):
        """Only the literal argument triggers a warning, not the proxy."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @job
            def mixed_job():
                @task
                def step_a():
                    pass

                @task
                def step_b(data, extra):
                    pass

                x = step_a()
                step_b(x, "oops")

            assert len(w) == 1
            assert "'extra'" in str(w[0].message)

    def test_dag_still_built_despite_warning(self):
        """The DAG is still built correctly even when warnings fire."""
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")

            @job
            def dag_job():
                @task
                def a():
                    pass

                @task
                def b(data, extra):
                    pass

                x = a()
                b(x, 42)

        meta = _JOB_REGISTRY["dag_job"]
        assert meta.dag["b"] == ["a"]
        assert meta.dag_edges["b"] == {"data": "a"}


class TestReservedParamValidation:
    """Fix #1: Reserved job parameter names must be rejected at decoration time."""

    def setup_method(self):
        reset_registries()

    def test_reserved_name_job_name_raises(self):
        with pytest.raises(ValueError, match="reserved for internal runtime use"):

            @job(params={"__job_name__": "bad"})
            def bad_job():
                @task
                def noop():
                    pass

                noop()

    def test_reserved_name_task_key_raises(self):
        with pytest.raises(ValueError, match="reserved for internal runtime use"):

            @job(params={"__task_key__": "bad"})
            def bad_job2():
                @task
                def noop():
                    pass

                noop()

    def test_reserved_name_run_id_raises(self):
        with pytest.raises(ValueError, match="reserved for internal runtime use"):

            @job(params={"__run_id__": "bad"})
            def bad_job3():
                @task
                def noop():
                    pass

                noop()

    def test_reserved_prefix_upstream_raises(self):
        with pytest.raises(ValueError, match="reserved for internal runtime use"):

            @job(params={"__upstream__data": "bad"})
            def bad_job4():
                @task
                def noop():
                    pass

                noop()

    def test_safe_param_names_accepted(self):
        @job(params={"url": "http://x", "env": "prod"})
        def good_job():
            @task
            def noop():
                pass

            noop()

        assert _JOB_REGISTRY["good_job"].params == {"url": "http://x", "env": "prod"}


class TestDuplicateTaskInvocation:
    """Fix #4: Calling the same @task twice inside one @job body must raise."""

    def setup_method(self):
        reset_registries()

    def test_duplicate_invocation_raises(self):
        with pytest.raises(DuplicateResourceError, match="called more than once"):

            @job
            def dup_call_job():
                @task
                def step():
                    pass

                step()
                step()  # second call – should raise

    def test_second_job_can_reuse_task_function_name(self):
        """Same task function name in a *different* job is allowed."""

        @job
        def job_one():
            @task
            def extract():
                pass

            extract()

        reset_registries()

        @job
        def job_two():
            @task
            def extract():
                pass

            extract()

        assert "job_two.extract" in _TASK_REGISTRY


class TestDuplicateStandaloneTask:
    """Fix #5: Duplicate standalone @task names must raise DuplicateResourceError."""

    def setup_method(self):
        reset_registries()

    def test_duplicate_standalone_task_raises(self):
        @task
        def my_task():
            return 1

        with pytest.raises(DuplicateResourceError, match="Duplicate task 'my_task'"):

            @task
            def my_task():  # noqa: F811 – intentional duplicate
                return 2


class TestDependsOn:
    """Control-flow-only dependencies via @task(depends_on=...)."""

    def setup_method(self):
        reset_registries()

    def test_single_depends_on(self):
        """A single TaskProxy in depends_on creates a DAG edge without edge_map."""

        @job
        def dep_job():
            @task
            def setup():
                pass

            s = setup()

            @task(depends_on=s)
            def work():
                pass

            work()

        meta = _JOB_REGISTRY["dep_job"]
        assert meta.dag["work"] == ["setup"]
        # No IoManager edge – only control-flow
        assert meta.dag_edges.get("work", {}) == {}

    def test_list_depends_on(self):
        """A list of TaskProxies in depends_on creates multiple edges."""

        @job
        def multi_dep_job():
            @task
            def init_a():
                pass

            @task
            def init_b():
                pass

            a = init_a()
            b = init_b()

            @task(depends_on=[a, b])
            def work():
                pass

            work()

        meta = _JOB_REGISTRY["multi_dep_job"]
        assert set(meta.dag["work"]) == {"init_a", "init_b"}
        assert meta.dag_edges.get("work", {}) == {}

    def test_depends_on_with_data_deps(self):
        """depends_on and data dependencies (TaskProxy args) coexist."""

        @job
        def mixed_dep_job():
            @task
            def init():
                pass

            @task
            def produce():
                pass

            i = init()
            p = produce()

            @task(depends_on=i)
            def consume(data):
                pass

            consume(p)

        meta = _JOB_REGISTRY["mixed_dep_job"]
        assert set(meta.dag["consume"]) == {"init", "produce"}
        # Only 'data' -> 'produce' is an IoManager edge
        assert meta.dag_edges["consume"] == {"data": "produce"}

    def test_depends_on_deduplication(self):
        """Same upstream in both depends_on and args is deduplicated."""

        @job
        def dedup_job():
            @task
            def upstream():
                pass

            u = upstream()

            @task(depends_on=u)
            def downstream(data):
                pass

            downstream(u)

        meta = _JOB_REGISTRY["dedup_job"]
        assert meta.dag["downstream"] == ["upstream"]  # single entry, not duplicated

    def test_depends_on_invalid_type_raises(self):
        """Passing a non-TaskProxy to depends_on raises TypeError."""
        with pytest.raises(TypeError, match="expects TaskProxy"):

            @job
            def bad_dep_job():
                @task(depends_on="setup")  # type: ignore[arg-type]
                def work():
                    pass

                work()

    def test_depends_on_uncalled_task_preserves_deps(self):
        """A task with depends_on that is never called still has deps in DAG."""

        @job
        def uncalled_job():
            @task
            def setup():
                pass

            s = setup()

            @task(depends_on=s)
            def work():
                pass

            # setup() is called, work() is NOT called

        meta = _JOB_REGISTRY["uncalled_job"]
        assert meta.dag["work"] == ["setup"]

    def test_depends_on_codegen(self):
        """depends_on produces TaskDependency in generated resources."""
        from databricks_bundle_decorators.codegen import generate_resources

        @job
        def codegen_dep_job():
            @task
            def gate():
                pass

            g = gate()

            @task(depends_on=g)
            def after_gate():
                pass

            after_gate()

        resources = generate_resources(package_name="test_pkg")
        tasks = {t.task_key: t for t in resources["codegen_dep_job"].tasks}
        after = tasks["after_gate"]
        assert any(d.task_key == "gate" for d in after.depends_on)
        # No __upstream__ params for control-flow-only deps
        named_params = after.python_wheel_task.named_parameters
        upstream_keys = [k for k in named_params if k.startswith("__upstream__")]
        assert upstream_keys == []


class TestForEachTask:
    """Tests for the @for_each_task decorator."""

    def setup_method(self):
        reset_registries()

    def test_dynamic_inputs_from_upstream_func_ref(self):
        """for_each_task wired with task_value() using function reference."""

        @job
        def fe_job():
            @task
            def get_items():
                pass

            @for_each_task(inputs=task_value(get_items, "items"))
            def process(inputs: str):
                pass

        meta = _JOB_REGISTRY["fe_job"]
        assert meta.dag["process"] == ["get_items"]
        assert "process" in meta.for_each_tasks
        fe = meta.for_each_tasks["process"]
        assert fe.inputs_task_key == "get_items"
        assert fe.inputs_value_key == "items"
        assert fe.static_inputs is None

    def test_dynamic_inputs_from_upstream_task_proxy(self):
        """for_each_task wired with task_value() using a TaskProxy."""

        @job
        def fe_proxy_job():
            @task
            def get_items():
                pass

            items = get_items()

            @for_each_task(inputs=task_value(items, "countries"))
            def process(inputs: str):
                pass

        meta = _JOB_REGISTRY["fe_proxy_job"]
        assert meta.dag["process"] == ["get_items"]
        fe = meta.for_each_tasks["process"]
        assert fe.inputs_task_key == "get_items"
        assert fe.inputs_value_key == "countries"
        assert fe.static_inputs is None

    def test_custom_value_key(self):
        """task_value() stores the user-specified key name."""

        @job
        def custom_key_job():
            @task
            def discover():
                pass

            @for_each_task(inputs=task_value(discover, "regions"))
            def process(inputs: str):
                pass

        fe = _JOB_REGISTRY["custom_key_job"].for_each_tasks["process"]
        assert fe.inputs_task_key == "discover"
        assert fe.inputs_value_key == "regions"

    def test_static_inputs(self):
        """for_each_task with a plain list of static inputs."""

        @job
        def static_fe_job():
            @for_each_task(inputs=["us-east-1", "eu-west-1"])
            def ingest(inputs: str):
                pass

        meta = _JOB_REGISTRY["static_fe_job"]
        assert meta.dag["ingest"] == []
        fe = meta.for_each_tasks["ingest"]
        assert fe.inputs_task_key is None
        assert fe.static_inputs == ["us-east-1", "eu-west-1"]

    def test_static_inputs_no_call_needed(self):
        """Static for_each_task appears in DAG without being called."""

        @job
        def no_call_job():
            @for_each_task(inputs=["a", "b", "c"])
            def work(inputs: str):
                pass

        meta = _JOB_REGISTRY["no_call_job"]
        assert "work" in meta.dag
        assert "work" in meta.for_each_tasks

    def test_concurrency(self):
        """concurrency is stored in ForEachMeta."""

        @job
        def conc_job():
            @for_each_task(inputs=["a", "b"], concurrency=10)
            def work(inputs: str):
                pass

        fe = _JOB_REGISTRY["conc_job"].for_each_tasks["work"]
        assert fe.concurrency == 10

    def test_with_data_deps(self):
        """for_each_task with inputs in decorator and data deps via call."""

        @job
        def data_fe_job():
            @task
            def get_items():
                pass

            @task
            def get_data():
                pass

            d = get_data()

            @for_each_task(inputs=task_value(get_items, "items"))
            def process(inputs: str, data):
                pass

            process(data=d)

        meta = _JOB_REGISTRY["data_fe_job"]
        assert set(meta.dag["process"]) == {"get_items", "get_data"}
        assert meta.dag_edges["process"] == {"data": "get_data"}
        fe = meta.for_each_tasks["process"]
        assert fe.inputs_task_key == "get_items"

    def test_missing_inputs_param_raises(self):
        """Function must have a parameter named 'inputs'."""
        with pytest.raises(ValueError, match="parameter named 'inputs'"):

            @job
            def bad_param_job():
                @for_each_task(inputs=["a"])
                def work(item: str):
                    pass

    def test_invalid_inputs_type_raises(self):
        """Passing a non-list, non-TaskValueRef as inputs raises TypeError."""
        with pytest.raises(TypeError, match="expects a TaskValueRef"):

            @job
            def bad_type_job():
                @for_each_task(inputs="not_a_list_or_ref")  # type: ignore[arg-type]
                def work(inputs: str):
                    pass

    def test_outside_job_raises(self):
        """@for_each_task outside a @job body raises RuntimeError."""
        with pytest.raises(RuntimeError, match="inside a @job body"):

            @for_each_task(inputs=["a"])
            def work(inputs: str):
                pass

    def test_duplicate_call_raises(self):
        """Calling a for_each_task twice in a @job body raises."""
        with pytest.raises(DuplicateResourceError, match="called more than once"):

            @job
            def dup_fe_job():
                @for_each_task(inputs=["a", "b"])
                def work(inputs: str):
                    pass

                work()
                work()

    def test_with_depends_on_func_ref(self):
        """for_each_task with control-flow depends_on using function ref."""

        @job
        def fe_depends_job():
            @task
            def setup():
                pass

            @for_each_task(inputs=["a", "b"], depends_on=setup)
            def work(inputs: str):
                pass

        meta = _JOB_REGISTRY["fe_depends_job"]
        assert "setup" in meta.dag["work"]

    def test_with_depends_on_task_proxy(self):
        """for_each_task with control-flow depends_on using TaskProxy."""

        @job
        def fe_depends_proxy_job():
            @task
            def setup():
                pass

            s = setup()

            @for_each_task(inputs=["a", "b"], depends_on=s)
            def work(inputs: str):
                pass

        meta = _JOB_REGISTRY["fe_depends_proxy_job"]
        assert "setup" in meta.dag["work"]

    def test_positional_arg_call_for_data_deps(self):
        """for_each_task call with positional args wires data deps."""

        @job
        def pos_fe_job():
            @task
            def get_items():
                pass

            @task
            def get_data():
                pass

            d = get_data()

            @for_each_task(inputs=task_value(get_items, "items"))
            def process(inputs: str, data):
                pass

            process(d)

        meta = _JOB_REGISTRY["pos_fe_job"]
        fe = meta.for_each_tasks["process"]
        assert fe.inputs_task_key == "get_items"
        assert meta.dag_edges["process"] == {"data": "get_data"}

    def test_registered_as_task(self):
        """for_each_task is registered in _TASK_REGISTRY with qualified key."""

        @job
        def reg_fe_job():
            @for_each_task(inputs=["a"])
            def work(inputs: str):
                pass

        assert "reg_fe_job.work" in _TASK_REGISTRY

    def test_non_json_static_inputs_raises(self):
        """Static inputs that aren't JSON-serialisable raise TypeError."""
        with pytest.raises(TypeError, match="JSON-serialisable"):

            @job
            def bad_json_job():
                @for_each_task(inputs=[object()])
                def work(inputs):
                    pass
