"""Runtime context – job parameters and Databricks utilities.

At runtime the entry point populates the global ``params`` dict from CLI
arguments (parsed via ``argparse``).  Task code imports and reads it::

    from databricks_bundle_decorators import params

    @task
    def my_task():
        url = params["url"]

The :func:`get_dbutils` helper provides a portable way to obtain the
``dbutils`` object regardless of execution environment (Databricks
notebook, job cluster, or Databricks Connect).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession  # type: ignore[import-untyped]  # optional dependency


class _Params(dict[str, Any]):
    """Dict subclass that holds job parameters.

    An instance lives at module level and is populated by the runtime
    runner before the task function is invoked.
    """


# Global params instance – importable by user code.
params: _Params = _Params()


def _populate_params(values: dict[str, Any]) -> None:
    """Replace the contents of the global *params* dict."""
    params.clear()
    params.update(values)


def get_dbutils(spark: SparkSession | None = None) -> Any:
    """Obtain the Databricks ``dbutils`` object for the current environment.

    Tries multiple strategies in order:

    1. **pyspark.dbutils** – works on Databricks Runtime and with
       Databricks Connect.  If *spark* is ``None``, the active
       ``SparkSession`` is used automatically.
    2. **IPython namespace** – fallback for interactive notebook
       environments where ``dbutils`` is injected into the user namespace.

    Parameters
    ----------
    spark:
        An optional :class:`~pyspark.sql.SparkSession`.  When omitted the
        function tries to retrieve the active session via
        ``SparkSession.getActiveSession()``.

    Returns
    -------
    Any
        The ``dbutils`` object (typically
        ``dbruntime.dbutils.DBUtils``).

    Raises
    ------
    RuntimeError
        If ``dbutils`` cannot be obtained through any strategy.

    Example
    -------
    ::

        from databricks_bundle_decorators import get_dbutils

        dbutils = get_dbutils()
        dbutils.fs.ls("/mnt/data")
    """
    # Strategy 1: pyspark.dbutils (Databricks Runtime & Databricks Connect)
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]

        if spark is None:
            from pyspark.sql import SparkSession as _SS  # type: ignore[import-untyped]

            spark = _SS.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except (ImportError, Exception):
        pass

    # Strategy 2: IPython notebook namespace (dbutils injected by Databricks)
    try:
        import IPython  # type: ignore[import-untyped]

        ip = IPython.get_ipython()
        if ip is not None and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"]
    except (ImportError, AttributeError, KeyError):
        pass

    raise RuntimeError(
        "dbutils is not available. Either pass a SparkSession connected via "
        "Databricks Connect or run this code on a Databricks cluster."
    )
