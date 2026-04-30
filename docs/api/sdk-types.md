# SDK Types

The decorator keyword arguments accept SDK-native dataclasses from
`databricks.bundles.jobs`. Below is a complete reference of which classes
are used by each config TypedDict, and where to import them.

## Importing SDK Classes

All classes listed below are imported from a single module:

```python
from databricks.bundles.jobs import CronSchedule, AutoScale, ClusterLogConf  # etc.
```

## Classes Used by `JobConfig` (for `@job(...)`)

| Field | Type | Description |
|-------|------|-------------|
| `continuous` | `Continuous` | Continuous trigger configuration |
| `email_notifications` | `JobEmailNotifications` | Email notification settings for the job |
| `git_source` | `GitSource` | Git repository source configuration |
| `health` | `JobsHealthRules` | Health rules for the job |
| `lifecycle` | `Lifecycle` | Lifecycle management settings |
| `notification_settings` | `JobNotificationSettings` | General notification settings |
| `performance_target` | `PerformanceTarget` | Performance target level |
| `queue` | `QueueSettings` | Queue settings for concurrent runs |
| `run_as` | `JobRunAs` | Service principal or user to run the job as |
| `schedule` | `CronSchedule` | Cron-based schedule |
| `trigger` | `TriggerSettings` | File-arrival or table-update triggers |
| `webhook_notifications` | `WebhookNotifications` | Webhook notification destinations |

Primitive fields (`budget_policy_id`, `description`, `max_concurrent_runs`,
`tags`, `timeout_seconds`, `usage_policy_id`) accept plain Python types
(`str`, `int`, `dict[str, str]`) — no SDK import needed.

## Classes Used by `TaskConfig` (for `@task(...)`)

| Field | Type | Description |
|-------|------|-------------|
| `compute` | `Compute` | Compute configuration override |
| `email_notifications` | `TaskEmailNotifications` | Email notification settings for the task |
| `health` | `JobsHealthRules` | Health rules for the task |
| `new_cluster` | `ClusterSpec` | Inline new cluster spec (prefer `@job_cluster` instead) |
| `notification_settings` | `TaskNotificationSettings` | General notification settings |
| `run_if` | `RunIf` | Conditional execution enum |
| `webhook_notifications` | `WebhookNotifications` | Webhook notification destinations |

Primitive fields (`description`, `disable_auto_optimization`, `disabled`,
`environment_key`, `existing_cluster_id`, `max_retries`,
`min_retry_interval_millis`, `retry_on_timeout`, `timeout_seconds`)
accept plain Python types — no SDK import needed.

## Classes Used by `ClusterConfig` (for `job_cluster(...)`)

| Field | Type | Description |
|-------|------|-------------|
| `autoscale` | `AutoScale` | Autoscaling worker range |
| `aws_attributes` | `AwsAttributes` | AWS-specific cluster attributes |
| `azure_attributes` | `AzureAttributes` | Azure-specific cluster attributes |
| `cluster_log_conf` | `ClusterLogConf` | Cluster log delivery configuration |
| `data_security_mode` | `DataSecurityMode` | Data security mode enum |
| `docker_image` | `DockerImage` | Custom Docker container image |
| `driver_node_type_flexibility` | `NodeTypeFlexibility` | Driver node type flexibility |
| `gcp_attributes` | `GcpAttributes` | GCP-specific cluster attributes |
| `init_scripts` | `list[InitScriptInfo]` | Init script configurations |
| `kind` | `Kind` | Cluster kind enum |
| `runtime_engine` | `RuntimeEngine` | Runtime engine enum |
| `worker_node_type_flexibility` | `NodeTypeFlexibility` | Worker node type flexibility |
| `workload_type` | `WorkloadType` | Workload type configuration |

Primitive fields (`apply_policy_default_values`, `autotermination_minutes`,
`cluster_name`, `custom_tags`, `driver_instance_pool_id`,
`driver_node_type_id`, `enable_elastic_disk`,
`enable_local_disk_encryption`, `init_scripts`, `instance_pool_id`,
`is_single_node`, `node_type_id`, `num_workers`, `policy_id`,
`remote_disk_throughput`, `single_user_name`, `spark_conf`,
`spark_env_vars`, `spark_version`, `ssh_public_keys`,
`total_initial_remote_disk_size`, `use_ml_runtime`) accept plain Python
types — no SDK import needed.

## Nested Classes (used inside the above)

These classes are commonly needed when constructing the top-level objects:

| Class | Used inside | Purpose |
|-------|-------------|---------|
| `DbfsStorageInfo` | `ClusterLogConf` | DBFS log destination |
| `S3StorageInfo` | `ClusterLogConf` | S3 log destination |
| `GcsStorageInfo` | `ClusterLogConf` | GCS log destination |
| `VolumesStorageInfo` | `ClusterLogConf` | Unity Catalog Volumes log destination |
| `DockerBasicAuth` | `DockerImage` | Docker registry credentials |
| `JobsHealthRule` | `JobsHealthRules` | Individual health rule |
| `Webhook` | `WebhookNotifications` | Individual webhook destination |
| `SparseCheckout` | `GitSource` | Sparse checkout configuration |
| `FileArrivalTriggerConfiguration` | `TriggerSettings` | File arrival trigger |
| `TableUpdateTriggerConfiguration` | `TriggerSettings` | Table update trigger |
| `PeriodicTriggerConfiguration` | `TriggerSettings` | Periodic trigger |
| `Library` | `@job(libraries=...)` | Library dependency (whl, jar, pypi) |
| `PythonPyPiLibrary` | `Library` | PyPI package specification |
| `MavenLibrary` | `Library` | Maven artifact specification |
| `JobParameterDefinition` | (internal) | Job parameter — prefer `@job(params={"key": "default"})` |

## Complete Import Example

```python
from databricks.bundles.jobs import (
    # Job-level
    CronSchedule,
    JobEmailNotifications,
    JobRunAs,
    QueueSettings,
    TriggerSettings,
    FileArrivalTriggerConfiguration,
    WebhookNotifications,
    Webhook,
    # Cluster-level
    AutoScale,
    ClusterLogConf,
    DbfsStorageInfo,
    DockerImage,
    DockerBasicAuth,
    InitScriptInfo,
    Library,
)
```

---

For the underlying TypedDict source definitions, see [Internals > SDK Types](../internals/sdk-types.md).
