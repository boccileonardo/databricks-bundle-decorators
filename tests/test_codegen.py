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
