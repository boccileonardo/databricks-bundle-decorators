# Mixed Bundle

Decorator-defined jobs coexisting with traditional YAML-defined jobs in the same bundle.

## Decorator job

```python
from databricks_bundle_decorators import job, job_cluster, task

cluster = job_cluster(name="cluster", spark_version="16.4.x-scala2.12", node_type_id="Standard_DS3_v2", num_workers=2)

@job(cluster=cluster, max_concurrent_runs=1)
def new_enrichment_job():
    @task(max_retries=3, timeout_seconds=3600)
    def enrich():
        print("Enriching data")

    @task
    def validate(enriched_data):
        print("Running quality checks")

    e = enrich()
    validate(enriched_data=e)
```

## YAML job (in `dbr_resources/legacy_ingest.yaml`)

```yaml
resources:
  jobs:
    legacy_ingest_job:
      name: legacy_ingest_job
      tasks:
        - task_key: ingest_raw
          spark_python_task:
            python_file: src/etl/ingest.py
          new_cluster:
            spark_version: "16.4.x-scala2.12"
            node_type_id: Standard_DS3_v2
            num_workers: 2
```

## `databricks.yaml`

Both sources are referenced together:

```yaml
include:
  - dbr_resources/*.yaml       # traditional YAML jobs

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'  # decorator-defined jobs
```

The Databricks CLI merges resources from all sources. Migrate at your own pace — move one YAML job to a decorator, delete the YAML file, repeat.
