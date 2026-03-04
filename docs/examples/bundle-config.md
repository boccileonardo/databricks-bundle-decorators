# Bundle Configuration

The `databricks.yaml` and `resources/__init__.py` files that tie everything together.

## `databricks.yaml`

```yaml
bundle:
  name: my-pipeline

artifacts:
  my_pipeline:
    type: whl
    build: |-
      uv sync --no-dev
      uv build --wheel
    path: .

include:
  - dbr_resources/*.yaml
  - dbr_resources/**/*.yaml

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'

targets:
  dev:
    mode: development
    presets:
      trigger_pause_status: PAUSED
    workspace:
      host: https://<your-dev-workspace>.azuredatabricks.net/

  prod:
    mode: production
    workspace:
      host: https://<your-prod-workspace>.azuredatabricks.net/
      root_path: /Workspace/Users/<your-user>/.bundle/${bundle.name}/${bundle.target}
```

## `resources/__init__.py`

```python
from databricks.bundles.core import Bundle, Resources


def load_resources(bundle: Bundle) -> Resources:
    from databricks_bundle_decorators.discovery import discover_pipelines
    from databricks_bundle_decorators.codegen import generate_resources

    discover_pipelines()

    resources = Resources()
    for resource_key, job in generate_resources().items():
        resources.add_resource(resource_key, job)
    return resources
```

`load_resources` is the entry point called by `databricks bundle deploy`. It discovers all pipeline modules (via the `databricks_bundle_decorators.pipelines` entry-point group), populates the registries, and generates `Job` resources.
