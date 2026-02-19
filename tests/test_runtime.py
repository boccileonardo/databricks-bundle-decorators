"""Tests for the runtime task runner."""

from __future__ import annotations

from typing import Any

from databricks_bundle_decorators.context import params
from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
)
from databricks_bundle_decorators.registry import (
    TaskMeta,
    _TASK_REGISTRY,
    reset_registries,
)
from databricks_bundle_decorators.runtime import run_task


class _MemoryIo(IoManager):
    """In-memory IoManager for testing."""

    storage: dict[str, Any] = {}

    def store(self, context: OutputContext, obj: Any) -> None:
        self.storage[context.task_key] = obj

    def load(self, context: InputContext) -> Any:
        return self.storage.get(context.upstream_task_key)


class TestRunTask:
    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}

    def test_simple_task_no_io(self):
        call_log: list[str] = []

        def my_task():
            call_log.append("executed")

        _TASK_REGISTRY["my_task"] = TaskMeta(fn=my_task, task_key="my_task")

        run_task("my_task", {"__job_name__": "j", "__task_key__": "my_task"})

        assert call_log == ["executed"]

    def test_task_receives_params(self):
        captured: dict[str, str] = {}

        def my_task():
            captured.update(params)

        _TASK_REGISTRY["my_task"] = TaskMeta(fn=my_task, task_key="my_task")

        run_task(
            "my_task",
            {"__job_name__": "j", "__task_key__": "my_task", "url": "http://x"},
        )

        assert captured["url"] == "http://x"

    def test_io_manager_store_and_load(self):
        io = _MemoryIo()

        def producer():
            return {"data": [1, 2, 3]}

        def consumer(df):
            return df

        _TASK_REGISTRY["producer"] = TaskMeta(
            fn=producer, task_key="producer", io_manager=io
        )
        _TASK_REGISTRY["consumer"] = TaskMeta(fn=consumer, task_key="consumer")

        # Run the producer
        run_task("producer", {"__job_name__": "j", "__task_key__": "producer"})
        assert io.storage["producer"] == {"data": [1, 2, 3]}

        # Run the consumer with upstream reference
        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__df": "producer",
            },
        )
