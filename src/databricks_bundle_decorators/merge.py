"""Declarative merge definition for Delta upserts.

Users return a `DeltaMerge` from their task function.  The IoManager
uses it to build a fresh merge builder on each write attempt.
For Polars IoManagers this builds a ``deltalake.TableMerger``; for
Spark IoManagers it builds a ``delta.tables.DeltaMergeBuilder``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class _MatchedUpdate:
    updates: dict[str, str]
    predicate: str | None = None


@dataclass(frozen=True)
class _MatchedUpdateAll:
    predicate: str | None = None
    except_cols: list[str] | None = None


@dataclass(frozen=True)
class _MatchedDelete:
    predicate: str | None = None


@dataclass(frozen=True)
class _NotMatchedInsert:
    updates: dict[str, str]
    predicate: str | None = None


@dataclass(frozen=True)
class _NotMatchedInsertAll:
    predicate: str | None = None
    except_cols: list[str] | None = None


@dataclass(frozen=True)
class _NotMatchedBySourceUpdate:
    updates: dict[str, str]
    predicate: str | None = None


@dataclass(frozen=True)
class _NotMatchedBySourceDelete:
    predicate: str | None = None


_MergeAction = (
    _MatchedUpdate
    | _MatchedUpdateAll
    | _MatchedDelete
    | _NotMatchedInsert
    | _NotMatchedInsertAll
    | _NotMatchedBySourceUpdate
    | _NotMatchedBySourceDelete
)


@dataclass
class DeltaMerge:
    """Declarative merge definition for Delta tables.

    Return this from a ``@task`` function to perform a merge/upsert.
    Works with both Polars and Spark IoManagers — the framework detects
    the source type and uses the appropriate Delta library.

    The builder API mirrors ``deltalake.TableMerger`` (snake_case):

    Example
    -------
    ::

        from databricks_bundle_decorators import DeltaMerge


        @task(io_manager=io)
        def upsert(upstream: pl.LazyFrame) -> DeltaMerge:
            df = upstream.collect()
            return (
                DeltaMerge(source=df, predicate="s.id = t.id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
            )

    Parameters
    ----------
    source : Any
        The source data for the merge.  Can be a ``polars.DataFrame``,
        ``polars.LazyFrame``, PyArrow table, or PySpark ``DataFrame``.
    predicate : str
        SQL-like predicate for the merge condition
        (e.g. ``"s.id = t.id"``).
    source_alias : str
        Alias for the source table in the predicate.  Defaults to
        ``"s"``.
    target_alias : str
        Alias for the target table in the predicate.  Defaults to
        ``"t"``.
    """

    source: Any
    predicate: str
    source_alias: str = "s"
    target_alias: str = "t"
    _actions: list[_MergeAction] = field(default_factory=list, init=False, repr=False)

    def when_matched_update_all(
        self,
        predicate: str | None = None,
        except_cols: list[str] | None = None,
    ) -> DeltaMerge:
        """Update all columns when the predicate matches."""
        self._actions.append(
            _MatchedUpdateAll(predicate=predicate, except_cols=except_cols)
        )
        return self

    def when_matched_update(
        self,
        updates: dict[str, str],
        predicate: str | None = None,
    ) -> DeltaMerge:
        """Update specific columns when the predicate matches."""
        self._actions.append(_MatchedUpdate(updates=updates, predicate=predicate))
        return self

    def when_matched_delete(self, predicate: str | None = None) -> DeltaMerge:
        """Delete matched rows."""
        self._actions.append(_MatchedDelete(predicate=predicate))
        return self

    def when_not_matched_insert_all(
        self,
        predicate: str | None = None,
        except_cols: list[str] | None = None,
    ) -> DeltaMerge:
        """Insert all columns for rows that don't match."""
        self._actions.append(
            _NotMatchedInsertAll(predicate=predicate, except_cols=except_cols)
        )
        return self

    def when_not_matched_insert(
        self,
        updates: dict[str, str],
        predicate: str | None = None,
    ) -> DeltaMerge:
        """Insert specific columns for rows that don't match."""
        self._actions.append(_NotMatchedInsert(updates=updates, predicate=predicate))
        return self

    def when_not_matched_by_source_update(
        self,
        updates: dict[str, str],
        predicate: str | None = None,
    ) -> DeltaMerge:
        """Update rows in target that have no match in source."""
        self._actions.append(
            _NotMatchedBySourceUpdate(updates=updates, predicate=predicate)
        )
        return self

    def when_not_matched_by_source_delete(
        self, predicate: str | None = None
    ) -> DeltaMerge:
        """Delete rows in target that have no match in source."""
        self._actions.append(_NotMatchedBySourceDelete(predicate=predicate))
        return self

    def _build_merger(
        self, table_uri: str, storage_options: dict[str, str] | None = None
    ) -> Any:
        """Build a fresh ``deltalake.TableMerger`` from this definition.

        Called by the IoManager on each write/retry attempt.

        Returns ``None`` if the target table does not exist yet — the
        caller should fall back to a regular write in that case.
        """
        from deltalake import DeltaTable  # noqa: PLC0415

        if not DeltaTable.is_deltatable(table_uri, storage_options=storage_options):
            return None

        dt = DeltaTable(table_uri, storage_options=storage_options)

        import polars as pl  # noqa: PLC0415

        source = self.source
        if isinstance(source, pl.DataFrame):
            source = source.to_arrow()
        elif isinstance(source, pl.LazyFrame):
            source = source.collect().to_arrow()

        merger = dt.merge(
            source=source,
            predicate=self.predicate,
            source_alias=self.source_alias,
            target_alias=self.target_alias,
        )

        for action in self._actions:
            match action:
                case _MatchedUpdateAll(predicate=p, except_cols=ec):
                    merger = merger.when_matched_update_all(predicate=p, except_cols=ec)
                case _MatchedUpdate(updates=u, predicate=p):
                    merger = merger.when_matched_update(updates=u, predicate=p)
                case _MatchedDelete(predicate=p):
                    merger = merger.when_matched_delete(predicate=p)
                case _NotMatchedInsertAll(predicate=p, except_cols=ec):
                    merger = merger.when_not_matched_insert_all(
                        predicate=p, except_cols=ec
                    )
                case _NotMatchedInsert(updates=u, predicate=p):
                    merger = merger.when_not_matched_insert(updates=u, predicate=p)
                case _NotMatchedBySourceUpdate(updates=u, predicate=p):
                    merger = merger.when_not_matched_by_source_update(
                        updates=u, predicate=p
                    )
                case _NotMatchedBySourceDelete(predicate=p):
                    merger = merger.when_not_matched_by_source_delete(predicate=p)

        return merger

    def _is_spark_source(self) -> bool:
        """Check if source is a PySpark DataFrame without importing pyspark."""
        return type(self.source).__module__.startswith("pyspark.")

    def _initial_write(
        self, table_uri: str, storage_options: dict[str, str] | None = None
    ) -> None:
        """Write source data directly when the target table doesn't exist yet.

        Called by the IoManager on first run before any Delta table has
        been created at the target path.
        """
        import polars as pl  # noqa: PLC0415

        source = self.source
        if isinstance(source, pl.LazyFrame):
            source.sink_delta(table_uri, storage_options=storage_options)
        elif isinstance(source, pl.DataFrame):
            source.write_delta(table_uri, storage_options=storage_options)
        else:
            # PyArrow or other — convert to polars first
            df = pl.DataFrame(pl.from_arrow(source))
            df.write_delta(table_uri, storage_options=storage_options)

    def _build_spark_merger(self, table_identifier: str) -> Any:
        """Build a fresh ``delta.tables.DeltaMergeBuilder`` for Spark.

        *table_identifier* can be a file path (for ``SparkDeltaIoManager``)
        or a fully-qualified table name (for UC IoManagers).

        Returns ``None`` if the target table does not exist yet.
        """
        from delta.tables import DeltaTable  # noqa: PLC0415
        from pyspark.sql import SparkSession  # noqa: PLC0415

        spark = SparkSession.getActiveSession()
        if spark is None:
            msg = "No active SparkSession found."
            raise RuntimeError(msg)

        # Determine if it's a path or a table name
        is_path = "/" in table_identifier or table_identifier.startswith(
            ("dbfs:", "s3:", "gs:", "abfss:", "file:")
        )

        if is_path:
            if not DeltaTable.isDeltaTable(spark, table_identifier):
                return None
            dt = DeltaTable.forPath(spark, table_identifier)
        else:
            if not spark.catalog.tableExists(table_identifier):
                return None
            dt = DeltaTable.forName(spark, table_identifier)

        source = self.source
        builder = dt.alias(self.target_alias).merge(
            source.alias(self.source_alias), self.predicate
        )

        for action in self._actions:
            match action:
                case _MatchedUpdateAll(predicate=p):
                    builder = builder.whenMatchedUpdateAll(condition=p)
                case _MatchedUpdate(updates=u, predicate=p):
                    builder = builder.whenMatchedUpdate(condition=p, set=u)
                case _MatchedDelete(predicate=p):
                    builder = builder.whenMatchedDelete(condition=p)
                case _NotMatchedInsertAll(predicate=p):
                    builder = builder.whenNotMatchedInsertAll(condition=p)
                case _NotMatchedInsert(updates=u, predicate=p):
                    builder = builder.whenNotMatchedInsert(condition=p, values=u)
                case _NotMatchedBySourceUpdate(updates=u, predicate=p):
                    builder = builder.whenNotMatchedBySourceUpdate(condition=p, set=u)
                case _NotMatchedBySourceDelete(predicate=p):
                    builder = builder.whenNotMatchedBySourceDelete(condition=p)

        return builder

    def _initial_spark_write(self, table_identifier: str) -> None:
        """Write source Spark DataFrame directly when the target doesn't exist.

        Called by the Spark IoManager on first run.
        """
        is_path = "/" in table_identifier or table_identifier.startswith(
            ("dbfs:", "s3:", "gs:", "abfss:", "file:")
        )

        writer = self.source.write.format("delta").mode("error")
        if is_path:
            writer.save(table_identifier)
        else:
            writer.saveAsTable(table_identifier)
