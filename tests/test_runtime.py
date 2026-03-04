"""Tests for the runtime task runner."""

import pytest
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
from databricks_bundle_decorators.task_values import (
    _local_task_values,
    get_task_value,
    set_task_value,
)


class _MemoryIo(IoManager):
    """In-memory IoManager for testing."""

    storage: dict[str, Any] = {}

    def write(self, context: OutputContext, obj: Any) -> None:
        self.storage[context.task_key] = obj

    def read(self, context: InputContext) -> Any:
        return self.storage.get(context.upstream_task_key)


class TestRunTask:
    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_simple_task_no_io(self):
        call_log: list[str] = []

        def my_task():
            call_log.append("executed")

        _TASK_REGISTRY["j.my_task"] = TaskMeta(fn=my_task, task_key="my_task")

        run_task("my_task", {"__job_name__": "j", "__task_key__": "my_task"})

        assert call_log == ["executed"]

    def test_task_receives_params(self):
        captured: dict[str, str] = {}

        def my_task():
            captured.update(params)

        _TASK_REGISTRY["j.my_task"] = TaskMeta(fn=my_task, task_key="my_task")

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

        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=producer, task_key="producer", io_manager=io
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=consumer, task_key="consumer")

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

    def test_task_values_cross_task_round_trip(self):
        """set_task_value in producer is retrievable via get_task_value."""

        def producer():
            set_task_value("row_count", 42)

        def consumer():
            return get_task_value("producer", "row_count")

        _TASK_REGISTRY["j.producer"] = TaskMeta(fn=producer, task_key="producer")
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=consumer, task_key="consumer")

        run_task("producer", {"__job_name__": "j", "__task_key__": "producer"})
        run_task("consumer", {"__job_name__": "j", "__task_key__": "consumer"})

        assert _local_task_values["producer"]["row_count"] == 42

    def test_io_manager_setup_called_before_write(self):
        """IoManager.setup() is called once before write()."""
        call_log: list[str] = []

        class _SetupIo(IoManager):
            def setup(self) -> None:
                call_log.append("setup")

            def write(self, context: OutputContext, obj: Any) -> None:
                call_log.append("write")

            def read(self, context: InputContext) -> Any:
                return None

        io = _SetupIo()
        _TASK_REGISTRY["j.t"] = TaskMeta(fn=lambda: "data", task_key="t", io_manager=io)

        run_task("t", {"__job_name__": "j", "__task_key__": "t"})

        assert call_log == ["setup", "write"]

    def test_io_manager_setup_called_before_read(self):
        """IoManager.setup() is called once before read()."""
        call_log: list[str] = []

        class _SetupIo(IoManager):
            def setup(self) -> None:
                call_log.append("setup")

            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                call_log.append("read")
                return "upstream_data"

        io = _SetupIo()
        _TASK_REGISTRY["j.upstream"] = TaskMeta(
            fn=lambda: None, task_key="upstream", io_manager=io
        )
        _TASK_REGISTRY["j.downstream"] = TaskMeta(fn=lambda x: x, task_key="downstream")

        run_task(
            "downstream",
            {
                "__job_name__": "j",
                "__task_key__": "downstream",
                "__upstream__x": "upstream",
            },
        )

        assert call_log == ["setup", "read"]

    def test_io_manager_setup_called_only_once(self):
        """setup() is idempotent — called at most once per IoManager instance."""
        setup_count = 0

        class _CountingIo(IoManager):
            def setup(self) -> None:
                nonlocal setup_count
                setup_count += 1

            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                return "data"

        io = _CountingIo()

        # Producer and consumer share the same IoManager instance.
        _TASK_REGISTRY["j.a"] = TaskMeta(fn=lambda: "val", task_key="a", io_manager=io)
        _TASK_REGISTRY["j.b"] = TaskMeta(fn=lambda x: x, task_key="b")

        # write via producer
        run_task("a", {"__job_name__": "j", "__task_key__": "a"})
        # read via consumer
        run_task(
            "b",
            {
                "__job_name__": "j",
                "__task_key__": "b",
                "__upstream__x": "a",
            },
        )

        assert setup_count == 1


class TestStrictTaskResolution:
    """Runtime always requires qualified key; no short-key fallback."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_qualified_key_is_required(self):
        """Qualified key resolves correctly in strict mode."""
        call_log: list[str] = []

        def my_task():
            call_log.append("ran")

        # Registered under qualified key (as @job would do)
        _TASK_REGISTRY["myjob.my_task"] = TaskMeta(fn=my_task, task_key="my_task")

        run_task("my_task", {"__job_name__": "myjob", "__task_key__": "my_task"})
        assert call_log == ["ran"]

    def test_strict_mode_fails_on_short_key_only(self):
        """A task only in the short-key registry is not found."""

        _TASK_REGISTRY["my_task"] = TaskMeta(fn=lambda: None, task_key="my_task")

        with pytest.raises(RuntimeError, match="not found by qualified key"):
            run_task("my_task", {"__job_name__": "myjob", "__task_key__": "my_task"})


class TestForEachRuntime:
    """Runtime handles __for_each_input__ injection."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_for_each_input_injected_as_string(self):
        """JSON string input is parsed and passed to the task function."""
        captured: dict[str, Any] = {}

        def process(inputs):
            captured["inputs"] = inputs

        _TASK_REGISTRY["j.process"] = TaskMeta(fn=process, task_key="process")

        run_task(
            "process",
            {
                "__job_name__": "j",
                "__task_key__": "process",
                "__for_each_input__": '"hello"',
            },
        )

        assert captured["inputs"] == "hello"

    def test_for_each_input_json_object(self):
        """JSON object input is parsed into a dict."""
        captured: dict[str, Any] = {}

        def process(inputs):
            captured["inputs"] = inputs

        _TASK_REGISTRY["j.process"] = TaskMeta(fn=process, task_key="process")

        run_task(
            "process",
            {
                "__job_name__": "j",
                "__task_key__": "process",
                "__for_each_input__": '{"file": "a.csv", "id": 1}',
            },
        )

        assert captured["inputs"] == {"file": "a.csv", "id": 1}

    def test_for_each_input_plain_string_fallback(self):
        """Non-JSON input is passed as a plain string."""
        captured: dict[str, Any] = {}

        def process(inputs):
            captured["inputs"] = inputs

        _TASK_REGISTRY["j.process"] = TaskMeta(fn=process, task_key="process")

        run_task(
            "process",
            {
                "__job_name__": "j",
                "__task_key__": "process",
                "__for_each_input__": "plain_value",
            },
        )

        assert captured["inputs"] == "plain_value"

    def test_for_each_with_io_manager_data(self):
        """for_each task receives both the input element and IoManager data."""
        io = _MemoryIo()
        io.storage["get_data"] = {"rows": [1, 2, 3]}
        captured: dict[str, Any] = {}

        def process(inputs, data):
            captured["inputs"] = inputs
            captured["data"] = data

        _TASK_REGISTRY["j.process"] = TaskMeta(fn=process, task_key="process")
        _TASK_REGISTRY["j.get_data"] = TaskMeta(
            fn=lambda: None, task_key="get_data", io_manager=io
        )

        run_task(
            "process",
            {
                "__job_name__": "j",
                "__task_key__": "process",
                "__for_each_input__": '"file_a.csv"',
                "__upstream__data": "get_data",
            },
        )

        assert captured["inputs"] == "file_a.csv"
        assert captured["data"] == {"rows": [1, 2, 3]}
