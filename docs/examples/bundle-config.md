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
    from databricks_bundle_decorators import discover_pipelines, generate_resources

    discover_pipelines()

    resources = Resources()
    for resource_key, job in generate_resources().items():
        resources.add_resource(resource_key, job)
    return resources
```

`load_resources` is the entry point called by `databricks bundle deploy`. It discovers all pipeline modules (via the `databricks_bundle_decorators.pipelines` entry-point group), populates the registries, and generates `Job` resources.

### Customising the default wheel path

By default every task gets `Library(whl="dist/*.whl")`.  If your build
outputs wheels to a different location, pass `default_libraries`:

```python
from databricks.bundles.jobs import Library

for resource_key, job in generate_resources(
    default_libraries=[Library(whl="artifacts/dist/*.whl")],
).items():
    resources.add_resource(resource_key, job)
```

Jobs that set their own `libraries` (e.g. `@job(libraries=[])` for
Docker) are **not** affected — `default_libraries` only applies when the
job does not specify libraries explicitly.

| `default_libraries` value | Behaviour |
|---|---|
| `None` (default) | Attach `dist/*.whl` — standard wheel deployment |
| `[Library(...)]` | Custom default libraries for all jobs |
| `[]` | No libraries by default |
