# Internals

Implementation details for contributors and advanced users. These modules are not part of the public API and may change without notice.

| Module | Description |
|--------|-------------|
| [Registry](registry.md) | Global registries (`_TASK_REGISTRY`, `_JOB_REGISTRY`, `_CLUSTER_REGISTRY`), metadata dataclasses |
| [Codegen](codegen.md) | Converts registries into `databricks.bundles.jobs` resources at deploy time |
| [Runtime](runtime.md) | `dbxdec-run` entry point — task dispatch, IoManager wiring, parameter injection |
| [Discovery](discovery.md) | Entry-point-based pipeline discovery |
