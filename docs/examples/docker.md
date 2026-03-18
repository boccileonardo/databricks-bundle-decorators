# Docker Deployment

Run tasks inside a custom Docker container with pre-installed dependencies.

```python
from databricks_bundle_decorators import job, job_cluster, task

docker_cluster = job_cluster(
    name="docker_cluster",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_DS3_v2",
    num_workers=2,
    docker_image={"url": "my-registry.io/my-pipeline:latest"},
)

@job(cluster=docker_cluster, libraries=[])
def docker_pipeline():
    @task
    def extract():
        print("Running inside custom Docker image")

    @task
    def transform(raw_data):
        print("All heavy deps are pre-installed")

    e = extract()
    transform(raw_data=e)
```

`libraries=[]` tells the framework to skip attaching `dist/*.whl` — the package is already installed in the container.

| `libraries` value | Behaviour |
|-------------------|-----------|
| `None` (default) | Attach `dist/*.whl` — standard wheel deployment |
| `[]` | No libraries — package pre-installed in Docker image |
| `[Library(...)]` | Custom libraries — e.g. PyPI packages, Maven JARs |

See [Docker Deployment](../guides/docker-deployment.md) for the full guide including Dockerfile and `databricks.yaml` examples.
