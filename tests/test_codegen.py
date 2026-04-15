"""Tests for codegen helpers."""

import json

from databricks.bundles.jobs import Library, PythonPyPiLibrary

from databricks_bundle_decorators.backfill import (
    BACKFILL_TAG,
    DailyBackfill,
    HourlyBackfill,
    MonthlyBackfill,
    StaticBackfill,
    WeeklyBackfill,
)
from databricks_bundle_decorators.codegen import generate_resources
from databricks_bundle_decorators.decorators import (
    for_each_task,
    job,
    job_cluster,
    task,
    task_value,
)
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
        # No libraries attached — codegen omits the key, SDK defaults to None
        assert not task_obj.libraries

    def test_libraries_custom_forwarded(self):
        """Custom Library objects are forwarded to generated tasks."""
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

    def test_default_libraries_override(self):
        """Passing default_libraries overrides the hardcoded dist/*.whl."""

        @job
        def my_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(
            package_name="test_pkg",
            default_libraries=[Library(whl="artifacts/*.whl")],
        )
        task_obj = resources["my_job"].tasks[0]
        assert len(task_obj.libraries) == 1
        assert task_obj.libraries[0].whl == "artifacts/*.whl"

    def test_default_libraries_empty_suppresses(self):
        """Passing default_libraries=[] suppresses the default wheel."""

        @job
        def my_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(
            package_name="test_pkg",
            default_libraries=[],
        )
        task_obj = resources["my_job"].tasks[0]
        assert not task_obj.libraries

    def test_job_libraries_override_default_libraries(self):
        """Job-level libraries take precedence over default_libraries."""
        custom_lib = Library(pypi=PythonPyPiLibrary(package="pandas"))

        @job(libraries=[custom_lib])
        def my_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(
            package_name="test_pkg",
            default_libraries=[Library(whl="artifacts/*.whl")],
        )
        task_obj = resources["my_job"].tasks[0]
        assert len(task_obj.libraries) == 1
        assert task_obj.libraries[0].pypi.package == "pandas"


class TestForEachCodegen:
    """Tests for for_each_task codegen output."""

    def setup_method(self):
        reset_registries()

    def test_dynamic_inputs_codegen(self):
        """for_each_task with upstream inputs produces ForEachTask wrapper."""

        @job
        def fe_codegen_job():
            @task
            def get_items():
                pass

            @for_each_task(inputs=task_value(get_items, "items"), concurrency=5)
            def process(inputs: str):
                pass

        resources = generate_resources(package_name="test_pkg")
        tasks = {t.task_key: t for t in resources["fe_codegen_job"].tasks}

        # Outer task has for_each_task, not python_wheel_task
        outer = tasks["process"]
        assert outer.for_each_task is not None
        assert outer.python_wheel_task is None

        # ForEachTask fields — uses the custom key "items"
        fe = outer.for_each_task
        assert fe.inputs == "{{tasks.get_items.values.items}}"
        assert fe.concurrency == 5

        # Inner task has python_wheel_task
        inner = fe.task
        assert inner.task_key == "process_inner"
        assert inner.python_wheel_task is not None
        named = inner.python_wheel_task.named_parameters
        assert named["__for_each_input__"] == "{{input}}"
        assert "__for_each_param__" not in named
        assert named["__task_key__"] == "process"

    def test_static_inputs_codegen(self):
        """for_each_task with static list produces JSON inputs."""

        @job
        def static_codegen_job():
            @for_each_task(inputs=["us-east-1", "eu-west-1"])
            def ingest(inputs: str):
                pass

        resources = generate_resources(package_name="test_pkg")
        tasks = {t.task_key: t for t in resources["static_codegen_job"].tasks}

        outer = tasks["ingest"]
        assert outer.for_each_task is not None
        assert outer.for_each_task.inputs == '["us-east-1", "eu-west-1"]'

    def test_for_each_with_data_deps_codegen(self):
        """for_each_task data deps produce __upstream__ params on inner task."""

        @job
        def data_codegen_job():
            @task
            def get_items():
                pass

            @task
            def get_data():
                pass

            d = get_data()

            @for_each_task(inputs=task_value(get_items, "result"))
            def process(inputs: str, data):
                pass

            process(data=d)

        resources = generate_resources(package_name="test_pkg")
        tasks = {t.task_key: t for t in resources["data_codegen_job"].tasks}

        outer = tasks["process"]
        inner = outer.for_each_task.task
        named = inner.python_wheel_task.named_parameters
        assert named["__upstream__data"] == "get_data"

        # Outer task depends on both get_items and get_data
        dep_keys = {d.task_key for d in outer.depends_on}
        assert dep_keys == {"get_items", "get_data"}

    def test_for_each_sdk_config_on_inner_task(self):
        """SDK config (max_retries etc.) goes on the inner task."""

        @job
        def sdk_fe_job():
            @for_each_task(inputs=["a", "b"], max_retries=3, timeout_seconds=600)
            def work(inputs: str):
                pass

        resources = generate_resources(package_name="test_pkg")
        tasks = {t.task_key: t for t in resources["sdk_fe_job"].tasks}

        inner = tasks["work"].for_each_task.task
        assert inner.max_retries == 3
        assert inner.timeout_seconds == 600

    def test_for_each_outer_has_no_cluster(self):
        """Outer for-each wrapper must never carry compute."""
        test_cluster = job_cluster(
            name="shared", spark_version="15.0.x-scala2.12", num_workers=2
        )

        @job(cluster=test_cluster)
        def fe_cluster_job():
            @for_each_task(inputs=["a", "b"])
            def work(inputs: str):
                pass

        resources = generate_resources(package_name="test_pkg")
        outer = resources["fe_cluster_job"].tasks[0]

        assert outer.for_each_task is not None
        assert outer.job_cluster_key is None

    def test_for_each_inner_inherits_job_cluster(self):
        """Inner task inherits job_cluster_key when no explicit compute."""
        test_cluster = job_cluster(
            name="shared", spark_version="15.0.x-scala2.12", num_workers=2
        )

        @job(cluster=test_cluster)
        def fe_inherit_job():
            @for_each_task(inputs=["x"])
            def work(inputs: str):
                pass

        resources = generate_resources(package_name="test_pkg")
        inner = resources["fe_inherit_job"].tasks[0].for_each_task.task

        assert inner.job_cluster_key == "shared"


class TestBackfillTagCodegen:
    """Tests for backfill definition tag injection."""

    def setup_method(self):
        reset_registries()

    def test_daily_backfill_tag(self):
        """DailyBackfill produces a tag with type and start_date."""

        @job(backfill=DailyBackfill(start_date="2024-01-01"))
        def daily_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tag = json.loads(resources["daily_job"].tags[BACKFILL_TAG])
        assert tag == {"type": "daily", "start_date": "2024-01-01"}

    def test_daily_backfill_tag_with_end_and_tz(self):
        """DailyBackfill with end_date and non-UTC tz includes all fields."""

        @job(
            backfill=DailyBackfill(
                start_date="2024-01-01", end_date="2024-12-31", tz="US/Eastern"
            )
        )
        def daily_full_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tag = json.loads(resources["daily_full_job"].tags[BACKFILL_TAG])
        assert tag == {
            "type": "daily",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "tz": "US/Eastern",
        }

    def test_weekly_backfill_tag(self):
        """WeeklyBackfill produces a tag with type weekly."""

        @job(backfill=WeeklyBackfill(start_date="2024-W01"))
        def weekly_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tag = json.loads(resources["weekly_job"].tags[BACKFILL_TAG])
        assert tag == {"type": "weekly", "start_date": "2024-W01"}

    def test_monthly_backfill_tag(self):
        """MonthlyBackfill produces a tag with type monthly."""

        @job(backfill=MonthlyBackfill(start_date="2024-01-01"))
        def monthly_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tag = json.loads(resources["monthly_job"].tags[BACKFILL_TAG])
        assert tag == {"type": "monthly", "start_date": "2024-01-01"}

    def test_hourly_backfill_tag(self):
        """HourlyBackfill produces a tag with type hourly."""

        @job(backfill=HourlyBackfill(start_date="2024-01-01T00", tz="America/New_York"))
        def hourly_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tag = json.loads(resources["hourly_job"].tags[BACKFILL_TAG])
        assert tag == {
            "type": "hourly",
            "start_date": "2024-01-01T00",
            "tz": "America/New_York",
        }

    def test_static_backfill_tag(self):
        """StaticBackfill produces a tag with keys list."""

        @job(backfill=StaticBackfill(keys=["us", "eu", "jp"]))
        def static_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tag = json.loads(resources["static_job"].tags[BACKFILL_TAG])
        assert tag == {"type": "static", "keys": ["us", "eu", "jp"]}

    def test_backfill_tag_merges_with_user_tags(self):
        """Backfill tag is merged alongside user-provided tags."""

        @job(
            backfill=DailyBackfill(start_date="2024-01-01"),
            tags={"team": "data", "env": "prod"},
        )
        def tagged_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tags = resources["tagged_job"].tags
        assert tags["team"] == "data"
        assert tags["env"] == "prod"
        assert BACKFILL_TAG in tags
        tag = json.loads(tags[BACKFILL_TAG])
        assert tag["type"] == "daily"

    def test_no_backfill_no_tag(self):
        """Jobs without backfill don't get a backfill tag."""

        @job
        def plain_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tags = resources["plain_job"].tags
        assert tags is None or BACKFILL_TAG not in tags

    def test_daily_backfill_utc_omits_tz(self):
        """DailyBackfill with default tz='UTC' omits tz from tag."""

        @job(backfill=DailyBackfill(start_date="2024-06-01", tz="UTC"))
        def utc_job():
            @task
            def noop():
                pass

            noop()

        resources = generate_resources(package_name="test_pkg")
        tag = json.loads(resources["utc_job"].tags[BACKFILL_TAG])
        assert "tz" not in tag

    def test_app_resource_key_adds_permissions(self):
        """When app_resource_key is set, each Job gets a permission entry."""

        @job
        def my_job():
            @task
            def step():
                pass

            step()

        resources = generate_resources(
            package_name="test_pkg",
            app_resource_key="my_app_observability",
        )
        perms = resources["my_job"].permissions
        assert len(perms) == 1
        perm = perms[0]
        assert perm.level.value == "CAN_VIEW"
        assert (
            perm.service_principal_name.value
            == "${resources.apps.my_app_observability.service_principal_client_id}"
        )

    def test_app_resource_key_custom_permission(self):
        """app_permission overrides the default CAN_VIEW level."""

        @job
        def perm_job():
            @task
            def step():
                pass

            step()

        resources = generate_resources(
            package_name="test_pkg",
            app_resource_key="my_app",
            app_permission="CAN_MANAGE_RUN",
        )
        assert resources["perm_job"].permissions[0].level.value == "CAN_MANAGE_RUN"

    def test_no_app_resource_key_no_permissions(self):
        """Without app_resource_key, permissions list is empty."""

        @job
        def plain_job():
            @task
            def step():
                pass

            step()

        resources = generate_resources(package_name="test_pkg")
        assert resources["plain_job"].permissions == []
