# API Reference

Public, user-facing API. For framework internals (codegen, runtime, registry), see [Internals](../internals/index.md).

| Module | Description |
|--------|-------------|
| [Decorators](decorators.md) | `@task`, `@job`, `job_cluster()` |
| [IoManager](io-manager.md) | `IoManager` ABC, `OutputContext`, `InputContext` |
| [Built-in IoManagers](io-managers.md) | `PolarsParquetIoManager` |
| [Task Values](task-values.md) | `set_task_value`, `get_task_value` |
| [Parameters](parameters.md) | `params` dict |
| [SDK Types](sdk-types.md) | `JobConfig`, `TaskConfig`, `ClusterConfig` TypedDicts |
