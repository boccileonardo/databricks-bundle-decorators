"""Spark Delta IoManagers for classic and serverless compute.

Reads and writes PySpark DataFrames as Delta tables.

- `SparkDeltaIoManager` – for **classic compute**; supports
  credential injection via ``spark.conf.set()``.
- `SparkServerlessDeltaIoManager` – for **serverless compute**;
  relies on Unity Catalog or environment-based auth (no
  ``spark.conf.set()``).

Requires PySpark, which is pre-installed on Databricks clusters.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
    _format_logical_date,
    _needs_logical_date_col,
    _spark_apply_partition_filter,
    _spark_extract_partition_values,
)


class _SparkDeltaBase(IoManager):
    """Private base class with shared Delta read/write logic."""

    _spark: Any  # SparkSession, set in setup()

    def __init__(
        self,
        base_path: str,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        mode: str = "error",
        *,
        auto_filter: bool = True,
    ) -> None:
        self.base_path = base_path.rstrip("/")
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._mode = mode
        self.auto_filter = auto_filter

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame or execute a DeltaMergeBuilder.

        - If *obj* is a ``DeltaMergeBuilder`` (from ``delta.tables``),
          calls ``.execute()`` and returns immediately.
        - Otherwise builds a DataFrameWriter with the configured
          ``mode``, ``partition_by``, and ``write_options``.

        When ``partition_by`` includes ``"logical_date"``, the column
        is injected automatically from the context.
        """
        # Handle merge builders first.
        _merge_cls: type | None = None
        try:
            from delta.tables import DeltaMergeBuilder

            _merge_cls = DeltaMergeBuilder
        except ImportError:
            pass

        if _merge_cls is not None and isinstance(obj, _merge_cls):
            obj.execute()
            return

        partition_by = context.partition_by

        # Inject logical_date column if it's a partition column
        if _needs_logical_date_col(partition_by):
            from pyspark.sql import functions as F

            ld_str = _format_logical_date(context.logical_date)
            obj = obj.withColumn("logical_date", F.lit(ld_str))

        uri = self._uri(context.task_key)
        writer = obj.write.format("delta").mode(self._mode)
        if partition_by:
            # Extract partition values from data before writing
            self._last_partition_values = _spark_extract_partition_values(
                obj, partition_by
            )
            writer = writer.partitionBy(*partition_by)
        for k, v in self._write_options.items():
            writer = writer.option(k, v)
        writer.save(uri)

    def read(self, context: InputContext) -> Any:
        """Read a Delta table as a PySpark DataFrame.

        When ``partition_by`` includes ``"logical_date"``, reads are
        filtered to the current partition unless the upstream
        dependency uses `all_partitions()` or the consuming
        task uses ``@task(all_partitions=True)``.
        """
        uri = self._uri(context.upstream_task_key)
        reader = self._spark.read.format("delta")
        for k, v in self._read_options.items():
            reader = reader.option(k, v)
        result = reader.load(uri)

        if context.partition_filter and not context.all_partitions:
            result = _spark_apply_partition_filter(result, context.partition_filter)
        elif (
            _needs_logical_date_col(context.partition_by) and not context.all_partitions
        ):
            from pyspark.sql import functions as F

            result = result.filter(
                F.col("logical_date") == _format_logical_date(context.logical_date)
            )

        return result


class SparkDeltaIoManager(_SparkDeltaBase):
    """Persist PySpark DataFrames as Delta tables on classic compute.

    Credentials are injected into the Spark session via
    ``spark.conf.set()`` during `setup`, following the same
    dict-or-callable pattern as the Polars IoManagers'
    ``storage_options``.

    Parameters
    ----------
    base_path : str
        Root URI for Delta tables.  Each task creates a sub-directory
        named after its task key (e.g.
        ``abfss://container@account.dfs.core.windows.net/staging``).
    spark_configs : dict[str, str] | Callable[[], dict[str, str]] | None
        Key-value pairs applied via ``spark.conf.set()`` before the
        first read or write.  Can be a plain dict, a **callable** that
        returns a dict (resolved lazily at runtime), or ``None``.

        Use a callable to defer secret lookup to runtime::

            from databricks_bundle_decorators import get_dbutils

            def _configs() -> dict[str, str]:
                dbutils = get_dbutils()
                key = dbutils.secrets.get(scope="kv", key="storage-key")
                return {
                    "fs.azure.account.key.myaccount.dfs.core.windows.net": key,
                }

            io = SparkDeltaIoManager(
                base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
                spark_configs=_configs,
            )

    write_options : dict[str, str] | None
        Extra Spark writer options applied via ``.option(k, v)``.
    read_options : dict[str, str] | None
        Extra Spark reader options applied via ``.option(k, v)``.
    mode : str
        Delta write mode.  One of ``"error"`` (default),
        ``"overwrite"``, ``"append"``, or ``"ignore"``.
        Defaults to ``"error"`` to prevent accidental data loss.

        For **merge** operations, ignore this parameter and return a
        fully-configured ``DeltaMergeBuilder`` from your task instead.
        The IoManager will call ``.execute()`` on it automatically.

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import SparkDeltaIoManager

        io = SparkDeltaIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
            spark_configs={
                "fs.azure.account.key.myaccount.dfs.core.windows.net": "***",
            },
        )

        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)

        Merge example::

            @task(io_manager=io)
            def upsert(new_data):
                from delta.tables import DeltaTable
                spark = SparkSession.getActiveSession()
                dt = DeltaTable.forPath(spark, io._uri("upsert"))
                return (
                    dt.alias("t")
                    .merge(new_data.alias("s"), "t.id = s.id")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                )
    """

    def __init__(
        self,
        base_path: str,
        spark_configs: dict[str, str] | Callable[[], dict[str, str]] | None = None,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        mode: str = "error",
        *,
        auto_filter: bool = True,
    ) -> None:
        super().__init__(
            base_path,
            write_options=write_options,
            read_options=read_options,
            mode=mode,
            auto_filter=auto_filter,
        )
        self._spark_configs = spark_configs

    @property
    def spark_configs(self) -> dict[str, str] | None:
        """Resolve *spark_configs*, calling it first if it is a callable."""
        if callable(self._spark_configs):
            return cast(Callable[[], dict[str, str]], self._spark_configs)()
        return self._spark_configs

    def setup(self) -> None:
        """Obtain the active SparkSession and apply ``spark_configs``."""
        from pyspark.sql import SparkSession

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

        configs = self.spark_configs
        if configs:
            for key, value in configs.items():
                self._spark.conf.set(key, value)


class SparkServerlessDeltaIoManager(_SparkDeltaBase):
    """Persist PySpark DataFrames as Delta tables on serverless compute.

    Serverless compute does **not** support ``spark.conf.set()`` for
    credential injection.  The ``base_path`` **must** be a storage
    location registered as a Unity Catalog **external location** —
    serverless compute can only access paths governed by UC.  Arbitrary
    cloud storage URIs that are not registered as external locations
    will fail at runtime.

    Parameters
    ----------
    base_path : str
        Root URI for Delta tables.  Must be a path governed by a
        Unity Catalog external location (e.g.
        ``abfss://container@account.dfs.core.windows.net/staging``).
    write_options : dict[str, str] | None
        Extra Spark writer options applied via ``.option(k, v)``.
    read_options : dict[str, str] | None
        Extra Spark reader options applied via ``.option(k, v)``.
    mode : str
        Delta write mode (``"error"``, ``"overwrite"``, ``"append"``,
        etc.).  Defaults to ``"error"`` to prevent accidental data
        loss.  For merge operations, return a ``DeltaMergeBuilder``
        from your task instead.

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import (
            SparkServerlessDeltaIoManager,
        )

        io = SparkServerlessDeltaIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
        )

        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)
    """

    def setup(self) -> None:
        """Obtain the active SparkSession (no config injection)."""
        from pyspark.sql import SparkSession

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)
