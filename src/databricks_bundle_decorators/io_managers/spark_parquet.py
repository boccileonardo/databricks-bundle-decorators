"""Spark Parquet IoManagers for classic and serverless compute.

Reads and writes PySpark DataFrames as Parquet files.

- `SparkParquetIoManager` - for **classic compute**; supports
  credential injection via ``spark.conf.set()``.
- `SparkServerlessParquetIoManager` - for **serverless compute**;
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
    RetryConfig,
    _needs_backfill_key_col,
    _resolve_backfill_key,
    _should_inject_backfill_key,
    _spark_apply_partition_filter,
    _spark_extract_partition_values,
)


class _SparkParquetBase(IoManager):
    """Private base class with shared Parquet read/write logic."""

    _spark: Any  # SparkSession, set in setup()

    def __init__(
        self,
        base_path: str | Callable[[], str],
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        *,
        auto_filter: bool = True,
        retry: RetryConfig | None = None,
    ) -> None:
        self._base_path = base_path
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self.auto_filter = auto_filter
        self.retry = retry

    @property
    def base_path(self) -> str:
        """Resolve *base_path*, calling it first if it is a callable."""
        if isinstance(self._base_path, str):
            return self._base_path.rstrip("/")
        return self._base_path().rstrip("/")

    def _uri(self, key: str) -> str:
        return f"{self.base_path}/{key}"

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame as Parquet.

        Uses ``overwrite`` mode so task outputs are idempotent.
        Applies ``partition_by`` and ``write_options`` when set.

        When ``partition_by`` includes ``"backfill_key"``, the column
        is injected automatically from the context.
        """
        partition_by = context.partition_by

        # Inject backfill_key column if it's a partition column
        if _should_inject_backfill_key(
            partition_by, has_backfill_key_col="backfill_key" in obj.columns
        ):
            from pyspark.sql import functions as F  # noqa: N812, PLC0415

            bk = _resolve_backfill_key(context.backfill_key)
            obj = obj.withColumn("backfill_key", F.lit(bk))

        uri = self._uri(context.task_key)
        writer = obj.write.format("parquet").mode("overwrite")
        if partition_by:
            # Extract partition values from data before writing
            self._last_partition_values = _spark_extract_partition_values(
                obj, partition_by
            )
            writer = writer.partitionBy(*partition_by)
            # Only overwrite partitions present in the data
            writer = writer.option("partitionOverwriteMode", "dynamic")
        for k, v in self._write_options.items():
            writer = writer.option(k, v)
        writer.save(uri)

    def read(self, context: InputContext) -> Any:
        """Read Parquet files as a PySpark DataFrame.

        When ``partition_by`` includes ``"backfill_key"``, reads are
        filtered to the current partition unless the upstream
        dependency uses `all_partitions()` or the consuming
        task uses ``@task(all_partitions=True)``.
        """
        uri = self._uri(context.upstream_task_key)
        reader = self._spark.read.format("parquet")
        for k, v in self._read_options.items():
            reader = reader.option(k, v)
        result = reader.load(uri)

        if context.partition_filter and not context.all_partitions:
            result = _spark_apply_partition_filter(result, context.partition_filter)
        elif (
            self.auto_filter
            and _needs_backfill_key_col(context.partition_by)
            and not context.all_partitions
        ):
            from pyspark.sql import functions as F  # noqa: N812, PLC0415

            result = result.filter(
                F.col("backfill_key") == _resolve_backfill_key(context.backfill_key)
            )

        return result


class SparkParquetIoManager(_SparkParquetBase):
    """Persist PySpark DataFrames as Parquet on classic compute.

    Credentials are injected into the Spark session via
    ``spark.conf.set()`` during `setup`, following the same
    dict-or-callable pattern as the Polars IoManagers'
    ``storage_options``.

    Parameters
    ----------
    base_path : str | Callable[[], str]
        Root URI for Parquet files.  Each task creates a sub-directory
        named after its task key (e.g.
        ``abfss://container@account.dfs.core.windows.net/staging``).

        Can also be a **callable** that returns a string, resolved lazily
        at runtime.  Use this for multi-environment deployments where the
        path depends on job parameters::

            from databricks_bundle_decorators import params

            io = SparkParquetIoManager(
                base_path=lambda: f"abfss://lake@{params['env']}account.dfs.core.windows.net/data",
            )
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


            io = SparkParquetIoManager(
                base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
                spark_configs=_configs,
            )

    write_options : dict[str, str] | None
        Extra Spark writer options applied via ``.option(k, v)``.
    read_options : dict[str, str] | None
        Extra Spark reader options applied via ``.option(k, v)``.
    retry : `RetryConfig` | None
        Optional retry configuration for write operations.  When set,
        failed writes are retried with exponential backoff (powered by
        `tenacity`).  Useful for handling transient write conflicts
        during concurrent backfill runs.  Defaults to ``None``
        (no retries).

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import SparkParquetIoManager

        io = SparkParquetIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
            spark_configs={
                "fs.azure.account.key.myaccount.dfs.core.windows.net": "***",
            },
        )


        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)
    """

    def __init__(
        self,
        base_path: str | Callable[[], str],
        spark_configs: dict[str, str] | Callable[[], dict[str, str]] | None = None,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        *,
        auto_filter: bool = True,
        retry: RetryConfig | None = None,
    ) -> None:
        super().__init__(
            base_path,
            write_options=write_options,
            read_options=read_options,
            auto_filter=auto_filter,
            retry=retry,
        )
        self._spark_configs = spark_configs

    @property
    def spark_configs(self) -> dict[str, str] | None:
        """Resolve *spark_configs*, calling it first if it is a callable."""
        if callable(self._spark_configs):
            return cast("Callable[[], dict[str, str]]", self._spark_configs)()
        return self._spark_configs

    def setup(self) -> None:
        """Obtain the active SparkSession and apply ``spark_configs``."""
        from pyspark.sql import SparkSession  # noqa: PLC0415

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

        configs = self.spark_configs
        if configs:
            for key, value in configs.items():
                self._spark.conf.set(key, value)


class SparkServerlessParquetIoManager(_SparkParquetBase):
    """Persist PySpark DataFrames as Parquet on serverless compute.

    Serverless compute does **not** support ``spark.conf.set()`` for
    credential injection.  The ``base_path`` **must** be a storage
    location registered as a Unity Catalog **external location** —
    serverless compute can only access paths governed by UC.  Arbitrary
    cloud storage URIs that are not registered as external locations
    will fail at runtime.

    Parameters
    ----------
    base_path : str | Callable[[], str]
        Root URI for Parquet files.  Must be a path governed by a
        Unity Catalog external location (e.g.
        ``abfss://container@account.dfs.core.windows.net/staging``).

        Can also be a **callable** that returns a string, resolved lazily
        at runtime.
    write_options : dict[str, str] | None
        Extra Spark writer options applied via ``.option(k, v)``.
    read_options : dict[str, str] | None
        Extra Spark reader options applied via ``.option(k, v)``.
    retry : `RetryConfig` | None
        Optional retry configuration for write operations.  When set,
        failed writes are retried with exponential backoff (powered by
        `tenacity`).  Useful for handling transient write conflicts
        during concurrent backfill runs.  Defaults to ``None``
        (no retries).

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import (
            SparkServerlessParquetIoManager,
        )

        io = SparkServerlessParquetIoManager(
            base_path="abfss://lake@myaccount.dfs.core.windows.net/staging",
        )


        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)
    """

    def setup(self) -> None:
        """Obtain the active SparkSession (no config injection)."""
        from pyspark.sql import SparkSession  # noqa: PLC0415

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)
