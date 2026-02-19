This is a testing repository for databricks-bundle-decorators (Databricks python asset bundles with decorators).
The goal is to have a working example uisng python asset bundle definitions with support for custom decorators that achieve an interface like Dagster or Prefect to manage task dependencies.
Reference https://docs.databricks.com/aws/en/dev-tools/bundles/python/ for pydabs docs.

```python
import polars as pl
import requests

@cluster(
    name="small_cluster",
)
def small_cluster():
    return {
        "spark_version": "13.2.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
    }

@task
def upstream():
    r = requests.get(params["url"])
    return pl.DataFrame(r.json())

@task
def downstream(df):
    print(df.head(10))
    

@job(
    tags={"source": "github", "type": "api"},
    schedule="0 * * * *"
    params={"url": "https://api.github.com/events"}
    cluster="small_cluster"
)
def job():
    df = upstream()
    downstream(df)
```

You use UV package manager (uv add, uv run, uv sync). You do not use direct venv.