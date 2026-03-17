"""Shared fixtures for Spark-based IoManager tests."""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    """Session-scoped local SparkSession with Delta support."""
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("databricks-bundle-decorators-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "spark-warehouse"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
