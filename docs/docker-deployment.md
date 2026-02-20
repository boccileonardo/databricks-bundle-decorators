# Docker Deployment

By default, `databricks-bundle-decorators` deploys tasks as
`python_wheel_task` jobs that install a `.whl` artifact at task startup.
An alternative is to **pre-install** your package (and all dependencies)
into a custom Docker image. This eliminates wheel install time
and gives you full control over the runtime environment.

## Quick start

```bash
uv init my-pipeline && cd my-pipeline
uv add databricks-bundle-decorators
uv run dbxdec init --docker
```

The `--docker` flag generates a pipeline example with `libraries=[]`
and a `databricks.yaml` without the `artifacts` section, since the
package is pre-installed in the Docker image rather than uploaded as a
wheel.

## How it works

The key difference from the standard wheel deployment is the `libraries`
parameter on `@job`:

```python
from databricks_bundle_decorators import job, job_cluster, task

docker_cluster = job_cluster(
    name="docker_cluster",
    spark_version="16.4.x-scala2.12",
    node_type_id="Standard_DS3_v2",
    num_workers=2,
    docker_image={
        "url": "my-registry.io/my-pipeline:latest",
    },
)

@job(
    cluster=docker_cluster,
    libraries=[],  # package is pre-installed in the image
)
def my_pipeline():
    @task
    def extract():
        ...

    @task
    def transform(data):
        ...

    raw = extract()
    transform(raw)
```

### `libraries=[]`

When `libraries` is not specified (the default), every generated task
includes `Library(whl="dist/*.whl")` — Databricks installs the wheel
before running the task.  Setting `libraries=[]` tells the framework to
**skip** library installation entirely because the package is already
available in the container.

### `libraries` parameter reference

| Value | Behavior |
|-------|----------|
| `None` (default) | Attach `dist/*.whl` — standard wheel deployment |
| `[]` | No libraries — package pre-installed in Docker image |
| `[Library(...)]` | Custom libraries — e.g. PyPI packages, Maven JARs |

## Dockerfile example

Your Docker image must have:

1. The Databricks runtime base image
2. Your pipeline package installed (with `databricks-bundle-decorators`)
3. The `dbxdec-run` entry point available on `$PATH`

```dockerfile
FROM databricksruntime/standard:16.4.x-scala2.12

# Install your pipeline package (includes databricks-bundle-decorators)
COPY dist/*.whl /tmp/
RUN pip install /tmp/*.whl && rm /tmp/*.whl
```

Build and push:

```bash
uv build --wheel
docker build -t my-registry.io/my-pipeline:latest .
docker push my-registry.io/my-pipeline:latest
```

## `databricks.yaml` for Docker

With Docker deployment you typically don't need the `artifacts` section
since no wheel is uploaded during `databricks bundle deploy`:

```yaml
bundle:
  name: my-pipeline

# No artifacts section needed — package is in the Docker image.

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'

targets:
  dev:
    mode: development
    workspace:
      host: https://<your-workspace>.azuredatabricks.net/
```

## Mixing Docker and wheel tasks

You can mix deployment strategies across different jobs in the same
project. Each `@job` has its own `libraries` setting:

```python
# This job uses the standard wheel deployment
@job(cluster=wheel_cluster)
def standard_job():
    ...

# This job uses Docker
@job(cluster=docker_cluster, libraries=[])
def docker_job():
    ...
```

## Important notes

- **Entry point discovery**: The `dbxdec-run` console script must be on
  `$PATH` inside the container. This happens automatically when you
  `pip install` the package that depends on `databricks-bundle-decorators`.

- **`resources/__init__.py` still runs locally**: The `load_resources()`
  entry point is invoked by `databricks bundle deploy` on your local
  machine (not inside the Docker image). Make sure your local
  environment has the package installed for codegen to work.

- **Image registry access**: Your Databricks workspace must have network
  access to pull from your container registry. Configure authentication
  via workspace-level Docker credentials.
