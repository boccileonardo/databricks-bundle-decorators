"""Spark Unity Catalog IoManagers.

Reads and writes PySpark DataFrames via Unity Catalog, supporting both
**managed tables** (three-level namespace) and **volume paths**.

These IoManagers work on both classic and serverless compute because
Unity Catalog authentication is handled by the workspace — no
``spark.conf.set()`` is needed.

- `SparkUCTableIoManager` – managed / external Delta tables
  (``catalog.schema.task_key``)
- `SparkUCVolumeDeltaIoManager` – Delta tables stored in UC Volumes
  (``/Volumes/catalog/schema/volume/task_key``)
- `SparkUCVolumeParquetIoManager` – Parquet files stored in UC Volumes
  (``/Volumes/catalog/schema/volume/task_key.parquet``)

Requires PySpark, which is pre-installed on Databricks clusters.
"""

from __future__ import annotations

from typing import Any

from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
)


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
    partition_by : str | list[str] | None
        Column(s) to partition by when writing.  Forwarded to
        Spark's ``partitionBy()``.
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

        from databricks_bundle_decorators.io_managers import SparkUCTableIoManager

        io = SparkUCTableIoManager(catalog="main", schema="staging")

        @task(io_manager=io)
        def extract():
            spark = SparkSession.getActiveSession()
            return spark.range(10)

        @task
        def transform(df):   # spark.table("main.staging.extract")
            df.show()
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        partition_by: str | list[str] | None = None,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        mode: str = "error",
    ) -> None:
        self.catalog = catalog
        self.schema = schema
        self._partition_by = partition_by
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._mode = mode

    def _table_name(self, key: str) -> str:
        return f"{self.catalog}.{self.schema}.{key}"

    def setup(self) -> None:
        """Obtain the active SparkSession."""
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame or execute a DeltaMergeBuilder.

        - If *obj* is a ``DeltaMergeBuilder``, calls ``.execute()``.
        - Otherwise writes via ``saveAsTable`` with the configured
          ``mode``, ``partition_by``, and ``write_options``.
        """
        _merge_cls: type | None = None
        try:
            from delta.tables import DeltaMergeBuilder  # type: ignore[import-untyped]

            _merge_cls = DeltaMergeBuilder
        except ImportError:
            pass

        if _merge_cls is not None and isinstance(obj, _merge_cls):
            obj.execute()
            return

        table = self._table_name(context.task_key)
        writer = obj.write.format("delta").mode(self._mode)
        if self._partition_by:
            cols = (
                [self._partition_by]
                if isinstance(self._partition_by, str)
                else self._partition_by
            )
            writer = writer.partitionBy(*cols)
        for k, v in self._write_options.items():
            writer = writer.option(k, v)
        writer.saveAsTable(table)

    def read(self, context: InputContext) -> Any:
        """Read a Unity Catalog managed table as a PySpark DataFrame."""
        table = self._table_name(context.upstream_task_key)
        return self._spark.table(table)


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
    partition_by : str | list[str] | None
        Column(s) to partition by when writing.  Forwarded to
        Spark's ``partitionBy()``.
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
            SparkUCVolumeDeltaIoManager,
        )

        io = SparkUCVolumeDeltaIoManager(
            catalog="main", schema="staging", volume="raw_data",
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
        partition_by: str | list[str] | None = None,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
        mode: str = "error",
    ) -> None:
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self._partition_by = partition_by
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        self._mode = mode

    def _uri(self, key: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{key}"

    def setup(self) -> None:
        """Obtain the active SparkSession."""
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame or execute a DeltaMergeBuilder.

        - If *obj* is a ``DeltaMergeBuilder``, calls ``.execute()``.
        - Otherwise writes via ``save()`` with the configured
          ``mode``, ``partition_by``, and ``write_options``.
        """
        _merge_cls: type | None = None
        try:
            from delta.tables import DeltaMergeBuilder  # type: ignore[import-untyped]

            _merge_cls = DeltaMergeBuilder
        except ImportError:
            pass

        if _merge_cls is not None and isinstance(obj, _merge_cls):
            obj.execute()
            return

        uri = self._uri(context.task_key)
        writer = obj.write.format("delta").mode(self._mode)
        if self._partition_by:
            cols = (
                [self._partition_by]
                if isinstance(self._partition_by, str)
                else self._partition_by
            )
            writer = writer.partitionBy(*cols)
        for k, v in self._write_options.items():
            writer = writer.option(k, v)
        writer.save(uri)

    def read(self, context: InputContext) -> Any:
        """Read Delta from a UC Volume path as a PySpark DataFrame."""
        uri = self._uri(context.upstream_task_key)
        reader = self._spark.read.format("delta")
        for k, v in self._read_options.items():
            reader = reader.option(k, v)
        return reader.load(uri)


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
    partition_by : str | list[str] | None
        Column(s) to partition by when writing.  Forwarded to
        Spark's ``partitionBy()``.
    write_options : dict[str, str] | None
        Extra Spark writer options applied via ``.option(k, v)``.
    read_options : dict[str, str] | None
        Extra Spark reader options applied via ``.option(k, v)``.

    Example
    -------
    ::

        from databricks_bundle_decorators.io_managers import (
            SparkUCVolumeParquetIoManager,
        )

        io = SparkUCVolumeParquetIoManager(
            catalog="main", schema="staging", volume="raw_data",
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
        partition_by: str | list[str] | None = None,
        write_options: dict[str, str] | None = None,
        read_options: dict[str, str] | None = None,
    ) -> None:
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self._partition_by = partition_by
        self._write_options = write_options or {}
        self._read_options = read_options or {}

    def _uri(self, key: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{key}.parquet"

    def setup(self) -> None:
        """Obtain the active SparkSession."""
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        self._spark = SparkSession.getActiveSession()
        if self._spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

    def write(self, context: OutputContext, obj: Any) -> None:
        """Write a PySpark DataFrame as Parquet to a UC Volume path."""
        uri = self._uri(context.task_key)
        writer = obj.write.format("parquet").mode("overwrite")
        if self._partition_by:
            cols = (
                [self._partition_by]
                if isinstance(self._partition_by, str)
                else self._partition_by
            )
            writer = writer.partitionBy(*cols)
        for k, v in self._write_options.items():
            writer = writer.option(k, v)
        writer.save(uri)

    def read(self, context: InputContext) -> Any:
        """Read Parquet from a UC Volume path as a PySpark DataFrame."""
        uri = self._uri(context.upstream_task_key)
        reader = self._spark.read.format("parquet")
        for k, v in self._read_options.items():
            reader = reader.option(k, v)
        return reader.load(uri)
