# API Reference

Public, user-facing API. For framework internals (codegen, runtime, registry), see [Internals](../internals/index.md).

| Page | Description |
|------|-------------|
| [Tasks](tasks.md) | `@task`, `@for_each_task`, `task_value()` |
| [Jobs](jobs.md) | `@job`, `job_cluster()`, `params` |
| [Task Values](task-values.md) | `set_task_value`, `get_task_value` |
| [Built-in IoManagers](io-managers/index.md) | Polars (Parquet, Delta, JSON, CSV), Spark (Delta, Parquet), Unity Catalog (Tables, Volumes) |
| [Custom IoManagers](custom-io-manager.md) | `IoManager` ABC, `OutputContext`, `InputContext` |
| [Backfill Definitions](backfill.md) | `BackfillDef`, `DailyBackfill`, `WeeklyBackfill`, `MonthlyBackfill`, `HourlyBackfill`, `StaticBackfill`, `get_run_logical_date`, `all_partitions` |
