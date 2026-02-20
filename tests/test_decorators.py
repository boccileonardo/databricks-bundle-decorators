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
from databricks_bundle_decorators.decorators import job, job_cluster, task


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
