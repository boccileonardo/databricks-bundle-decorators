"""TypedDict definitions mirroring Databricks SDK model fields.

These TypedDicts expose the full set of valid parameters for ``@job``
and ``@task`` decorators, giving users IDE autocomplete and type-checking
for every Databricks-native field.

The fields listed here are the **SDK-native** fields that get forwarded
directly to ``databricks.bundles.jobs.Job`` and
``databricks.bundles.jobs.Task`` constructors in codegen.

The only explicit parameters on the decorators with transformation
logic are:

- ``@job(params=...)`` — converts ``dict[str, str]`` into a list of
  ``JobParameterDefinition`` objects and wires forwarding to each task.
- ``@job(cluster=...)`` — resolves a ``@job_cluster`` name into a
  ``JobCluster`` entry with the cluster spec.

All other fields (including ``tags``, ``schedule``, etc.) pass through
unmodified to the SDK constructors.

.. note::

   When the databricks-bundles SDK adds new fields, add them here to
   expose them to users.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from databricks.bundles.jobs import (
        AutoScale,
        AwsAttributes,
        AzureAttributes,
        ClusterLogConf,
        ClusterSpec,
        Compute,
        Continuous,
        CronSchedule,
        DataSecurityMode,
        DockerImage,
        GcpAttributes,
        GitSource,
        InitScriptInfo,
        JobEmailNotifications,
        JobNotificationSettings,
        JobRunAs,
        JobsHealthRules,
        Kind,
        Lifecycle,
        NodeTypeFlexibility,
        PerformanceTarget,
        QueueSettings,
        RunIf,
        RuntimeEngine,
        TaskEmailNotifications,
        TaskNotificationSettings,
        TriggerSettings,
        WebhookNotifications,
        WorkloadType,
    )


class JobConfig(TypedDict, total=False):
    """SDK-native fields forwarded to ``databricks.bundles.jobs.Job``.

    Pass any of these as keyword arguments to ``@job(...)`` alongside
    the pydabs-managed parameters (``params``, ``cluster``).

    Example::

        from databricks.bundles.jobs import CronSchedule

        @job(
            tags={"team": "data"},
            schedule=CronSchedule(
                quartz_cron_expression="0 0 * * * ?",
                timezone_id="UTC",
            ),
            max_concurrent_runs=1,
        )
        def my_job():
            ...
    """

    budget_policy_id: str
    continuous: Continuous
    description: str
    email_notifications: JobEmailNotifications
    git_source: GitSource
    health: JobsHealthRules
    lifecycle: Lifecycle
    max_concurrent_runs: int
    notification_settings: JobNotificationSettings
    performance_target: PerformanceTarget
    queue: QueueSettings
    run_as: JobRunAs
    schedule: CronSchedule
    tags: dict[str, str]
    timeout_seconds: int
    trigger: TriggerSettings
    usage_policy_id: str
    webhook_notifications: WebhookNotifications


class TaskConfig(TypedDict, total=False):
    """SDK-native fields forwarded to ``databricks.bundles.jobs.Task``.

    Pass any of these as keyword arguments to ``@task(...)`` alongside
    the pydabs convenience parameter (``io_manager``).

    Example::

        @task(
            max_retries=2,
            timeout_seconds=1800,
            retry_on_timeout=True,
        )
        def my_task():
            ...
    """

    compute: Compute
    description: str
    disable_auto_optimization: bool
    disabled: bool
    email_notifications: TaskEmailNotifications
    environment_key: str
    existing_cluster_id: str
    health: JobsHealthRules
    max_retries: int
    min_retry_interval_millis: int
    new_cluster: ClusterSpec
    notification_settings: TaskNotificationSettings
    retry_on_timeout: bool
    run_if: RunIf
    timeout_seconds: int
    webhook_notifications: WebhookNotifications


class ClusterConfig(TypedDict, total=False):
    """SDK-native fields forwarded to ``databricks.bundles.jobs.ClusterSpec``.

    Pass any of these as keyword arguments to ``@job_cluster(...)``
    alongside the pydabs convenience parameter (``name``).

    Example::

        @job_cluster(
            name="small_cluster",
            spark_version="13.2.x-scala2.12",
            node_type_id="Standard_DS3_v2",
            num_workers=2,
        )
        def small_cluster():
            ...
    """

    apply_policy_default_values: bool
    autoscale: AutoScale
    autotermination_minutes: int
    aws_attributes: AwsAttributes
    azure_attributes: AzureAttributes
    cluster_log_conf: ClusterLogConf
    cluster_name: str
    custom_tags: dict[str, str]
    data_security_mode: DataSecurityMode
    docker_image: DockerImage
    driver_instance_pool_id: str
    driver_node_type_flexibility: NodeTypeFlexibility
    driver_node_type_id: str
    enable_elastic_disk: bool
    enable_local_disk_encryption: bool
    gcp_attributes: GcpAttributes
    init_scripts: list[InitScriptInfo]
    instance_pool_id: str
    is_single_node: bool
    kind: Kind
    node_type_id: str
    num_workers: int
    policy_id: str
    remote_disk_throughput: int
    runtime_engine: RuntimeEngine
    single_user_name: str
    spark_conf: dict[str, str]
    spark_env_vars: dict[str, str]
    spark_version: str
    ssh_public_keys: list[str]
    total_initial_remote_disk_size: int
    use_ml_runtime: bool
    worker_node_type_flexibility: NodeTypeFlexibility
    workload_type: WorkloadType
