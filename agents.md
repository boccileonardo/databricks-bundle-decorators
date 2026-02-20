# Agent Instructions for databricks-bundle-decorators

## Project Overview

**databricks-bundle-decorators** is a Python library that lets users define Databricks jobs and tasks using `@task`, `@job`, and `job_cluster()` decorators. The decorators compile into Databricks Asset Bundle resources at deploy time and handle task dispatch at runtime. Think of it as an Airflow TaskFlow-inspired pattern, but for Databricks.

The package name is `databricks-bundle-decorators`. The Python import name is `databricks_bundle_decorators`. The CLI commands are `dbxdec` (scaffolding) and `dbxdec-run` (runtime dispatch).

## CRITICAL TOOLS

- **Package manager:** UV only. Use `uv add`, `uv run`, `uv sync`. Never use `pip`, `venv`, `python -m`, or `pip install`.

- Before considering a change completed, must run:

```bash
# Pre-commit multiple check
uv run prek run --all-files

# Type checking
uv run ty check

# Tests
uv run pytest
```

## Architecture — Key Concepts

### Two Phases

1. **Deploy time** (`databricks bundle deploy`): Python files are imported, decorators register metadata into global registries (`_TASK_REGISTRY`, `_JOB_REGISTRY`, `_CLUSTER_REGISTRY` in `registry.py`). `codegen.py` reads the registries and produces `databricks.bundles.jobs.Job` dataclass instances. No task business logic runs.

2. **Runtime** (on Databricks cluster): The `dbxdec-run` entry point (`runtime.py`) is invoked per-task. It discovers pipelines via entry points, looks up the task in the registry, calls `IoManager.read()` for upstream data, executes the task function, and calls `IoManager.write()` for the return value.

### DAG Construction (TaskFlow Pattern)

Inside a `@job` body, `@task` calls return `TaskProxy` objects (not real data). When a proxy is passed as an argument to another `@task` call, the framework records the dependency edge. The DAG is built by normal Python execution — no AST parsing.

```python
@job
def my_job():
    @task
    def a(): ...
    @task
    def b(data): ...

    x = a()       # Returns TaskProxy("a")
    b(x)          # Records: b depends on a, param "data" maps to task "a"
```

### Global Mutable State

The framework stores all discovered metadata in module-level dictionaries ("registries") that live for the lifetime of the Python process:

| Registry | Location | Purpose |
|---|---|---|
| `_TASK_REGISTRY` | `registry.py` | Maps `"job.task_key"` → `TaskMeta` (function, io_manager, sdk config) |
| `_JOB_REGISTRY` | `registry.py` | Maps `"job_name"` → `JobMeta` (DAG, params, cluster) |
| `_CLUSTER_REGISTRY` | `registry.py` | Maps `"cluster_name"` → `ClusterMeta` (spec) |
| `_current_job_*` dicts | `decorators.py` | Temporary scratch space used while a `@job` body executes |
| `_local_task_values` | `task_values.py` | Local fallback for task values when not on Databricks |

This design is intentional — decorators run at import time in a single-threaded process, so globals are safe and simple. The consequence for **testing** is that registries accumulate across tests unless cleared. **Always call `reset_registries()` in `setup_method`** and clear `_local_task_values` when testing task values.

### IoManager

The **producer** declares the IoManager via `@task(io_manager=...)`. The framework calls `write()` after the producer runs and `read()` before the consumer runs. Consumers don't need to know about storage — they receive plain Python objects as function arguments.

### Public API

Everything user-facing is re-exported from `__init__.py`:

- Decorators: `task`, `job`, `job_cluster`
- Data: `IoManager`, `OutputContext`, `InputContext`
- Task values: `set_task_value`, `get_task_value`
- Parameters: `params`
- Types: `JobConfig`, `TaskConfig`, `ClusterConfig`
- Errors: `DuplicateResourceError`
- Discovery: `discover_pipelines`

When adding new public symbols, add them to both the imports and `__all__` in `__init__.py`.

## Testing Conventions

- Test files mirror source files: `test_decorators.py` tests `decorators.py`, etc.
- Use `reset_registries()` in `setup_method` to clear global state between tests.
- Use `_MemoryIo` (in-memory IoManager) for testing data flow without external storage.
- Use `pytest` fixtures: `tmp_path` for filesystem tests, `monkeypatch` for env/argv, `capsys` for output capture.
- The `examples/` directory is excluded from type checking (`[tool.ty] src.exclude = ["examples"]`).
- Tests must not depend on Databricks, PySpark, or network access.

## Coding Guidelines

- Use `` in every source file.
- Type hints on all function signatures. Use `Any` only when truly unavoidable.
- Docstrings in Google/NumPy style with RST cross-references (`` :class:`...` ``, `` :func:`...` ``).
- Internal/private names start with `_` (e.g., `_TASK_REGISTRY`, `_current_job_name`).
- SDK pass-through fields use `**kwargs: Unpack[TypedDict]` pattern for IDE autocomplete.
- No dependencies beyond `databricks-bundles` in production. Dev deps are in `[dependency-groups] dev`.

## Common Tasks

### Adding a new SDK field to decorators

1. Add the field to the relevant `TypedDict` in `sdk_types.py` (`JobConfig`, `TaskConfig`, or `ClusterConfig`).
2. No changes needed in `decorators.py` or `codegen.py` — fields pass through via `**kwargs`.
3. Add a test in `test_decorators.py` or `test_codegen.py` verifying the field appears on the generated object.

### Adding a new CLI subcommand

1. Add the handler function in `cli.py` (e.g., `_cmd_<name>`).
2. Register the subparser in `main()`.
3. Add tests in `test_cli.py`.

### Adding a new public API symbol

1. Define it in the appropriate source module.
2. Import and re-export in `__init__.py`.
3. Add to `__all__` in `__init__.py`.

## Gotchas

- **Registry cleanup in tests:** Forgetting `reset_registries()` causes test pollution — tasks/jobs from previous tests leak into the registry.
- **`@job` body execution:** The `@job` decorator immediately calls `fn()` at decoration time. If the body has side effects beyond `@task` calls, they'll run at import time.
- **`examples/` is not tested:** The example pipeline uses `polars` and `requests` which are not project dependencies. It exists for documentation purposes only.
