"""Runtime context – provides job parameters to task functions.

At runtime the entry point populates the global ``params`` dict from CLI
arguments (parsed via ``argparse``).  Task code imports and reads it::

    from databricks_bundle_decorators import params

    @task
    def my_task():
        url = params["url"]
"""

from typing import Any


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
