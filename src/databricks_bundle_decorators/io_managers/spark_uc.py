"""Spark Unity Catalog IoManagers.

Reads and writes PySpark DataFrames via Unity Catalog, supporting both
**managed tables** (three-level namespace) and **volume paths**.

These IoManagers work on both classic and serverless compute because
Unity Catalog authentication is handled by the workspace — no
``spark.conf.set()`` is needed.

- `SparkUCTableIoManager` - managed / external Delta tables
  (``catalog.schema.task_key``)
- `SparkUCVolumeDeltaIoManager` - Delta tables stored in UC Volumes
  (``/Volumes/catalog/schema/volume/task_key``)
- `SparkUCVolumeParquetIoManager` - Parquet files stored in UC Volumes
  (``/Volumes/catalog/schema/volume/task_key``)

Requires PySpark, which is pre-installed on Databricks clusters.
"""

from __future__ import annotations

import logging
from typing import Any

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
    RetryConfig,
    _build_replace_where,
    _needs_backfill_key_col,
    _resolve_backfill_key,
    _should_inject_backfill_key,
    _spark_apply_partition_filter,
    _spark_extract_partition_values,
    _validate_delta_mode,
)

_logger = logging.getLogger(__name__)


class SparkUCTableIoManager(IoManager):
    """Persist PySpark DataFrames as Unity Catalog managed Delta tables.

    Uses ``saveAsTable`` / ``spark.table()`` with the three-level
    namespace ``catalog.schema.task_key``.

    Unity Catalog manages access control and storage location, so no
    credential configuration is required.  Works on both classic and
    serverless compute.

    Parameters
    ----------
    catalog : str
        Unity Catalog catalog name (e.g. ``"main"``).
    schema : str
        Unity Catalog schema (database) name (e.g. ``"staging"``).
    write_options : dict[str, str] | None
        Extra Spark writer options applied via ``.option(k, v)``.
    read_options : dict[str, str] | None
        Extra Spark reader options applied via ``.option(k, v)``.
    mode : str
        Delta write mode (``"error"``, ``"overwrite"``, ``"append"``,
        etc.).  Defaults to ``"error"`` to prevent accidental data
        loss.  For merge operations, return a `DeltaMerge`
        from your task instead.
    retry : `RetryConfig` | None
        Optional retry configuration for write operations.  When set,
        failed writes are retried with exponential backoff (powered by
        `tenacity`).  Useful for handling transient Delta commit
        conflicts during concurrent backfill runs on unpartitioned
        tables.  Defaults to ``None`` (no retries).

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")


        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)


        @task
        def transform(df):  # spark.table("main.staging.extract")
            df.show()
    """

    _spark: Any  # SparkSession, set in setup()

    def __init__(
        self,
        catalog: str,
        schema: str,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        mode: str = "error",
        *,
        auto_filter: bool = True,
        retry: RetryConfig | None = None,
    ) -> None:
        _validate_delta_mode(mode, type(self).__name__)
        self.catalog = catalog
        self.schema = schema
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._mode = mode
        self.auto_filter = auto_filter
        self.retry = retry

    def _table_name(self, key: str) -> str:
        return f"{self.catalog}.{self.schema}.{key}"

    def setup(self) -> None:
        """Obtain the active SparkSession."""
        from pyspark.sql import SparkSession  # noqa: PLC0415

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame or `DeltaMerge`.

        - If *obj* is a `DeltaMerge`, builds and executes a merge.
        - Otherwise writes via ``saveAsTable`` with the configured
          ``mode``, ``partition_by``, and ``write_options``.

        When ``partition_by`` includes ``"backfill_key"``, the column
        is injected automatically from the context.
        """
        from databricks_bundle_decorators.merge import DeltaMerge  # noqa: PLC0415

        if isinstance(obj, DeltaMerge):
            table = self._table_name(context.task_key)
            _logger.info(
                "Merging into %s (predicate=%r, actions=%s)",
                table,
                obj.predicate,
                obj._describe_actions(),
            )
            builder = obj._build_spark_merger(table)
            if builder is None:
                obj._initial_spark_write(
                    table,
                    partition_by=context.partition_by,
                    write_options=dict(self._write_options),
                )
            else:
                builder.execute()
            if context.partition_by:
                self._last_partition_values = _spark_extract_partition_values(
                    obj.source, context.partition_by
                )
            else:
                self._last_partition_values = {}
            return

        partition_by = context.partition_by

        # Inject backfill_key column if it's a partition column
        if _should_inject_backfill_key(
            partition_by, has_backfill_key_col="backfill_key" in obj.columns
        ):
            from pyspark.sql import functions as F  # noqa: N812, PLC0415

            bk = _resolve_backfill_key(context.backfill_key)
            obj = obj.withColumn("backfill_key", F.lit(bk))

        table = self._table_name(context.task_key)
        _logger.info(
            "Writing to %s (mode=%s, partition_by=%s)", table, self._mode, partition_by
        )
        writer = obj.write.format("delta").mode(self._mode)
        if partition_by:
            # Extract partition values from data before writing
            self._last_partition_values = _spark_extract_partition_values(
                obj, partition_by
            )
            writer = writer.partitionBy(*partition_by)
            # Scope overwrite to affected partitions only (skip on first write
            # because saveAsTable + replaceWhere requires an existing table)
            if self._mode == "overwrite" and self._spark.catalog.tableExists(table):
                writer = writer.option(
                    "replaceWhere",
                    _build_replace_where(self._last_partition_values),
                )
        for k, v in self._write_options.items():
            writer = writer.option(k, v)
        writer.saveAsTable(table)

    def read(self, context: InputContext) -> Any:
        """Read a Unity Catalog managed table as a PySpark DataFrame.

        When ``partition_by`` includes ``"backfill_key"``, reads are
        filtered to the current partition unless the upstream
        dependency uses `all_partitions()` or the consuming
        task uses ``@task(all_partitions=True)``.
        """
        table = self._table_name(context.upstream_task_key)
        _logger.info(
            "Reading from %s (partition_filter=%s)", table, context.partition_filter
        )
        result = self._spark.table(table)

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


class SparkUCVolumeDeltaIoManager(IoManager):
    """Persist PySpark DataFrames as Delta tables in UC Volumes.

    Writes to ``/Volumes/<catalog>/<schema>/<volume>/<task_key>``
    using the standard Delta format.

    Parameters
    ----------
    catalog : str
        Unity Catalog catalog name.
    schema : str
        Unity Catalog schema (database) name.
    volume : str
        Unity Catalog volume name.
    write_options : dict[str, str] | None
        Extra Spark writer options applied via ``.option(k, v)``.
    read_options : dict[str, str] | None
        Extra Spark reader options applied via ``.option(k, v)``.
    mode : str
        Delta write mode (``"error"``, ``"overwrite"``, ``"append"``,
        etc.).  Defaults to ``"error"`` to prevent accidental data
        loss.  For merge operations, return a `DeltaMerge`
        from your task instead.
    retry : `RetryConfig` | None
        Optional retry configuration for write operations.  When set,
        failed writes are retried with exponential backoff (powered by
        `tenacity`).  Useful for handling transient Delta commit
        conflicts during concurrent backfill runs on unpartitioned
        tables.  Defaults to ``None`` (no retries).

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeDeltaIoManager,
        )

        io = SparkUCVolumeDeltaIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )


        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)
    """

    _spark: Any  # SparkSession, set in setup()

    def __init__(
        self,
        catalog: str,
        schema: str,
        volume: str,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        mode: str = "error",
        *,
        auto_filter: bool = True,
        retry: RetryConfig | None = None,
    ) -> None:
        _validate_delta_mode(mode, type(self).__name__)
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._mode = mode
        self.auto_filter = auto_filter
        self.retry = retry

    def _uri(self, key: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{key}"

    def setup(self) -> None:
        """Obtain the active SparkSession."""
        from pyspark.sql import SparkSession  # noqa: PLC0415

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame or `DeltaMerge`.

        - If *obj* is a `DeltaMerge`, builds and executes a merge.
        - Otherwise writes via ``save()`` with the configured
          ``mode``, ``partition_by``, and ``write_options``.

        When ``partition_by`` includes ``"backfill_key"``, the column
        is injected automatically from the context.
        """
        from databricks_bundle_decorators.merge import DeltaMerge  # noqa: PLC0415

        if isinstance(obj, DeltaMerge):
            uri = self._uri(context.task_key)
            _logger.info(
                "Merging into %s (predicate=%r, actions=%s)",
                uri,
                obj.predicate,
                obj._describe_actions(),
            )
            builder = obj._build_spark_merger(uri)
            if builder is None:
                obj._initial_spark_write(
                    uri,
                    partition_by=context.partition_by,
                    write_options=dict(self._write_options),
                )
            else:
                builder.execute()
            if context.partition_by:
                self._last_partition_values = _spark_extract_partition_values(
                    obj.source, context.partition_by
                )
            else:
                self._last_partition_values = {}
            return

        partition_by = context.partition_by

        # Inject backfill_key column if it's a partition column
        if _should_inject_backfill_key(
            partition_by, has_backfill_key_col="backfill_key" in obj.columns
        ):
            from pyspark.sql import functions as F  # noqa: N812, PLC0415

            bk = _resolve_backfill_key(context.backfill_key)
            obj = obj.withColumn("backfill_key", F.lit(bk))

        uri = self._uri(context.task_key)
        _logger.info(
            "Writing to %s (mode=%s, partition_by=%s)", uri, self._mode, partition_by
        )
        writer = obj.write.format("delta").mode(self._mode)
        if partition_by:
            # Extract partition values from data before writing
            self._last_partition_values = _spark_extract_partition_values(
                obj, partition_by
            )
            writer = writer.partitionBy(*partition_by)
            # Scope overwrite to affected partitions only
            if self._mode == "overwrite":
                writer = writer.option(
                    "replaceWhere",
                    _build_replace_where(self._last_partition_values),
                )
        for k, v in self._write_options.items():
            writer = writer.option(k, v)
        writer.save(uri)

    def read(self, context: InputContext) -> Any:
        """Read Delta from a UC Volume path as a PySpark DataFrame.

        When ``partition_by`` includes ``"backfill_key"``, reads are
        filtered to the current partition unless the upstream
        dependency uses `all_partitions()` or the consuming
        task uses ``@task(all_partitions=True)``.
        """
        uri = self._uri(context.upstream_task_key)
        _logger.info(
            "Reading from %s (partition_filter=%s)", uri, context.partition_filter
        )
        reader = self._spark.read.format("delta")
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


class SparkUCVolumeParquetIoManager(IoManager):
    """Persist PySpark DataFrames as Parquet in UC Volumes.

    Writes to ``/Volumes/<catalog>/<schema>/<volume>/<task_key>.parquet``
    using the Parquet format.

    Parameters
    ----------
    catalog : str
        Unity Catalog catalog name.
    schema : str
        Unity Catalog schema (database) name.
    volume : str
        Unity Catalog volume name.
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
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main",
            schema="staging",
            volume="raw_data",
        )


        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)
    """

    _spark: Any  # SparkSession, set in setup()

    def __init__(
        self,
        catalog: str,
        schema: str,
        volume: str,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        *,
        auto_filter: bool = True,
        retry: RetryConfig | None = None,
    ) -> None:
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self.auto_filter = auto_filter
        self.retry = retry

    def _uri(self, key: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{key}"

    def setup(self) -> None:
        """Obtain the active SparkSession."""
        from pyspark.sql import SparkSession  # noqa: PLC0415

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame as Parquet to a UC Volume path.

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
        _logger.info("Writing to %s (partition_by=%s)", uri, partition_by)
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
        """Read Parquet from a UC Volume path as a PySpark DataFrame.

        When ``partition_by`` includes ``"backfill_key"``, reads are
        filtered to the current partition unless the upstream
        dependency uses `all_partitions()` or the consuming
        task uses ``@task(all_partitions=True)``.
        """
        uri = self._uri(context.upstream_task_key)
        _logger.info(
            "Reading from %s (partition_filter=%s)", uri, context.partition_filter
        )
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
