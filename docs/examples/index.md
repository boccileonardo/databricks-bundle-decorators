# Examples

Concise examples of the patterns supported by `databricks-bundle-decorators`.

| Example | Pattern |
|---------|---------|
| [Basic ETL](basic-etl.md) | `@task`, `@job`, `job_cluster()`, `SparkDeltaIoManager`, `params` |
| [For-Each](for-each.md) | `@for_each_task` with dynamic and static inputs |
| [Docker Deployment](docker.md) | `docker_image` cluster, `libraries=[]` |
| [Mixed Bundle](mixed-bundle.md) | Decorator jobs alongside YAML-defined jobs |
| [Bundle Configuration](bundle-config.md) | `databricks.yaml` and `resources/__init__.py` |
