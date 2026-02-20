"""Tests for codegen helpers."""

from databricks_bundle_decorators.codegen import generate_resources
from databricks_bundle_decorators.decorators import job, job_cluster, task
from databricks_bundle_decorators.registry import reset_registries


class TestGenerateResources:
    def setup_method(self):
        reset_registries()

    def test_sdk_config_forwarded_to_job(self):
        """Job-level sdk_config fields appear on the generated Job."""

        test_cluster = job_cluster(
            name="test_cluster", spark_version="13.2.x-scala2.12", num_workers=1
        )

        @job(
            tags={"env": "test"},
            cluster=test_cluster,
            max_concurrent_runs=3,
            description="A test job",
        )
        def my_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        job_obj = resources["my_job"]
        assert job_obj.tags == {"env": "test"}
        assert job_obj.max_concurrent_runs == 3
        assert job_obj.description == "A test job"

    def test_sdk_config_forwarded_to_task(self):
        """Task-level sdk_config fields appear on the generated Task."""

        @job
        def my_job():
            @task(max_retries=2, timeout_seconds=600)
            def my_task():
                pass

            my_task()

        resources = generate_resources(package_name="test_pkg")
        tasks = resources["my_job"].tasks
        assert len(tasks) == 1
        assert tasks[0].max_retries == 2
        assert tasks[0].timeout_seconds == 600

    def test_default_libraries_dist_whl(self):
        """When libraries is not set, tasks get the default dist/*.whl."""

        @job
        def my_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        task_obj = resources["my_job"].tasks[0]
        assert len(task_obj.libraries) == 1
        assert task_obj.libraries[0].whl == "dist/*.whl"

    def test_libraries_empty_for_docker(self):
        """Setting libraries=[] removes all task libraries (Docker deployment)."""

        @job(libraries=[])
        def docker_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        task_obj = resources["docker_job"].tasks[0]
        # No libraries attached — the SDK defaults unset libraries to []
        assert task_obj.libraries == [] or task_obj.libraries is None

    def test_libraries_custom_forwarded(self):
        """Custom Library objects are forwarded to generated tasks."""
        from databricks.bundles.jobs import Library, PythonPyPiLibrary

        custom_lib = Library(pypi=PythonPyPiLibrary(package="requests"))

        @job(libraries=[custom_lib])
        def custom_lib_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        task_obj = resources["custom_lib_job"].tasks[0]
        assert len(task_obj.libraries) == 1
        assert task_obj.libraries[0].pypi.package == "requests"
