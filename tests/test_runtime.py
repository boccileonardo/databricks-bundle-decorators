"""Tests for the runtime task runner."""

import logging
from typing import Any

import pytest

from databricks_bundle_decorators.backfill import StaticBackfill
from databricks_bundle_decorators.context import params
from databricks_bundle_decorators.io_manager import (
    InputContext,
    IoManager,
    OutputContext,
    RetryConfig,
)
from databricks_bundle_decorators.io_managers import PolarsDeltaIoManager
from databricks_bundle_decorators.registry import (
    _JOB_REGISTRY,
    _TASK_REGISTRY,
    JobMeta,
    TaskMeta,
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


class TestLogicalDateRuntime:
    """Runtime wires backfill_key into IoManager contexts."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_backfill_key_passed_to_output_context(self):
        """OutputContext receives backfill_key as a string."""
        captured_ctx: list[OutputContext] = []

        class _CapturingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                captured_ctx.append(context)

            def read(self, context: InputContext) -> Any:
                return None

        io = _CapturingIo()
        _TASK_REGISTRY["j.t"] = TaskMeta(
            fn=lambda: "data",
            task_key="t",
            io_manager=io,
        )

        run_task(
            "t",
            {
                "__job_name__": "j",
                "__task_key__": "t",
                "backfill_key": "2024-01-15T00:00:00+00:00",
            },
        )

        assert len(captured_ctx) == 1
        assert captured_ctx[0].backfill_key == "2024-01-15T00:00:00+00:00"

    def test_empty_backfill_key_defaults_to_none(self):
        """Empty string backfill_key defaults to None."""
        captured_ctx: list[OutputContext] = []

        class _CapturingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                captured_ctx.append(context)

            def read(self, context: InputContext) -> Any:
                return None

        io = _CapturingIo()
        _TASK_REGISTRY["j.t"] = TaskMeta(fn=lambda: "data", task_key="t", io_manager=io)

        run_task(
            "t",
            {
                "__job_name__": "j",
                "__task_key__": "t",
                "backfill_key": "",
            },
        )

        assert captured_ctx[0].backfill_key is None

    def test_missing_backfill_key_defaults_to_none(self):
        """When backfill_key is not in params, backfill_key is None."""
        captured_ctx: list[OutputContext] = []

        class _CapturingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                captured_ctx.append(context)

            def read(self, context: InputContext) -> Any:
                return None

        io = _CapturingIo()
        _TASK_REGISTRY["j.t"] = TaskMeta(fn=lambda: "data", task_key="t", io_manager=io)

        run_task(
            "t",
            {
                "__job_name__": "j",
                "__task_key__": "t",
            },
        )

        assert captured_ctx[0].backfill_key is None

    def test_backfill_key_passed_to_input_context(self):
        """InputContext receives backfill_key as a string."""
        captured_ctx: list[InputContext] = []

        class _CapturingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                captured_ctx.append(context)
                return "upstream_data"

        io = _CapturingIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: None,
            task_key="producer",
            io_manager=io,
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=lambda x: x, task_key="consumer")

        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__x": "producer",
                "backfill_key": "2024-06-01T12:00:00+00:00",
            },
        )

        assert len(captured_ctx) == 1
        assert captured_ctx[0].backfill_key == "2024-06-01T12:00:00+00:00"


class TestAllPartitionsRuntime:
    """Runtime handles __all_partitions__ flags from codegen."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_all_partitions_flag_passed_to_input_context(self):
        """__all_partitions__<param>=true sets all_partitions on InputContext."""
        captured_ctx: list[InputContext] = []

        class _CapturingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                captured_ctx.append(context)
                return "all_data"

        io = _CapturingIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: None,
            task_key="producer",
            io_manager=io,
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=lambda x: x, task_key="consumer")

        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__x": "producer",
                "__all_partitions__x": "true",
                "backfill_key": "2024-01-15T00:00:00+00:00",
            },
        )

        assert len(captured_ctx) == 1
        assert captured_ctx[0].all_partitions is True

    def test_no_all_partitions_flag_defaults_false(self):
        """Without __all_partitions__ flag, all_partitions is False."""
        captured_ctx: list[InputContext] = []

        class _CapturingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                captured_ctx.append(context)
                return "data"

        io = _CapturingIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: None,
            task_key="producer",
            io_manager=io,
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=lambda x: x, task_key="consumer")

        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__x": "producer",
                "backfill_key": "2024-01-15T00:00:00+00:00",
            },
        )

        assert len(captured_ctx) == 1
        assert captured_ctx[0].all_partitions is False


class TestAutoFilterRuntime:
    """Runtime pushes partition values via task values and populates partition_filter."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_auto_filter_pushes_partition_values(self):
        """Write with auto_filter=True pushes __partition_values__ task value."""

        class _PartitionedIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                return "data"

            def _extract_partition_values(
                self, context: OutputContext
            ) -> dict[str, list[str]]:
                return {"event_date": ["2024-01-15"]}

        io = _PartitionedIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: "data",
            task_key="producer",
            io_manager=io,
            partition_by=["event_date"],
        )

        run_task(
            "producer",
            {"__job_name__": "j", "__task_key__": "producer"},
        )

        assert get_task_value("producer", "__partition_values__") == {
            "event_date": ["2024-01-15"],
        }

    def test_auto_filter_false_skips_extraction(self):
        """Write with auto_filter=False does not push __partition_values__."""

        class _NoFilterIo(IoManager):
            auto_filter = False

            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                return "data"

        io = _NoFilterIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: "data",
            task_key="producer",
            io_manager=io,
            partition_by=["event_date"],
        )

        run_task(
            "producer",
            {"__job_name__": "j", "__task_key__": "producer"},
        )

        assert get_task_value("producer", "__partition_values__") is None

    def test_partition_filter_populated_on_read(self):
        """InputContext gets partition_filter from upstream task values."""
        captured_ctx: list[InputContext] = []

        class _PartitionedIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                captured_ctx.append(context)
                return "data"

            def _extract_partition_values(
                self, context: OutputContext
            ) -> dict[str, list[str]]:
                return {"event_date": ["2024-01-15"]}

        io = _PartitionedIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: "data",
            task_key="producer",
            io_manager=io,
            partition_by=["event_date"],
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=lambda x: x, task_key="consumer")

        # Run producer (pushes __partition_values__)
        run_task(
            "producer",
            {"__job_name__": "j", "__task_key__": "producer"},
        )

        # Run consumer (reads __partition_values__)
        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__x": "producer",
            },
        )

        assert len(captured_ctx) == 1
        assert captured_ctx[0].partition_filter == {
            "event_date": ["2024-01-15"],
        }

    def test_all_partitions_suppresses_partition_filter(self):
        """all_partitions=True skips partition_filter even when available."""
        captured_ctx: list[InputContext] = []

        class _PartitionedIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                captured_ctx.append(context)
                return "data"

            def _extract_partition_values(
                self, context: OutputContext
            ) -> dict[str, list[str]]:
                return {"event_date": ["2024-01-15"]}

        io = _PartitionedIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: "data",
            task_key="producer",
            io_manager=io,
            partition_by=["event_date"],
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=lambda x: x, task_key="consumer")

        run_task(
            "producer",
            {"__job_name__": "j", "__task_key__": "producer"},
        )

        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__x": "producer",
                "__all_partitions__x": "true",
            },
        )

        assert len(captured_ctx) == 1
        assert captured_ctx[0].partition_filter is None

    def test_auto_filter_false_warns_for_all_columns(self, caplog):
        """auto_filter=False emits a warning for all partition columns."""

        class _NoFilterIo(IoManager):
            auto_filter = False

            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                return "data"

        io = _NoFilterIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: "data",
            task_key="producer",
            io_manager=io,
            partition_by=["event_date"],
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=lambda x: x, task_key="consumer")

        run_task(
            "producer",
            {"__job_name__": "j", "__task_key__": "producer"},
        )

        with caplog.at_level(logging.WARNING):
            run_task(
                "consumer",
                {
                    "__job_name__": "j",
                    "__task_key__": "consumer",
                    "__upstream__x": "producer",
                },
            )

        assert "auto_filter=False" in caplog.text
        assert "event_date" in caplog.text

    def test_auto_filter_false_warns_for_backfill_key(self, caplog):
        """auto_filter=False also warns when partition_by is backfill_key."""

        class _NoFilterIo(IoManager):
            auto_filter = False

            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                return "data"

        io = _NoFilterIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: "data",
            task_key="producer",
            io_manager=io,
            partition_by=["backfill_key"],
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=lambda x: x, task_key="consumer")

        run_task(
            "producer",
            {"__job_name__": "j", "__task_key__": "producer"},
        )

        with caplog.at_level(logging.WARNING):
            run_task(
                "consumer",
                {
                    "__job_name__": "j",
                    "__task_key__": "consumer",
                    "__upstream__x": "producer",
                },
            )

        assert "auto_filter=False" in caplog.text
        assert "backfill_key" in caplog.text


class TestPartitionValueExtraction:
    """Verify _extract_partition_values returns cached values from write."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_extract_raises_when_no_write(self):
        """_extract_partition_values raises RuntimeError if write was not called."""

        class _NoWriteIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                return "data"

        io = _NoWriteIo()
        ctx = OutputContext(
            job_name="j", task_key="t", run_id="r", partition_by=["region"]
        )
        with pytest.raises(RuntimeError, match="did not populate"):
            io._extract_partition_values(ctx)

    def test_write_populates_last_partition_values(self):
        """write() populating _last_partition_values feeds _extract_partition_values."""

        class _RealisticIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                self._last_partition_values = {"region": ["us-east"]}

            def read(self, context: InputContext) -> Any:
                return "data"

        io = _RealisticIo()
        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: "data",
            task_key="producer",
            io_manager=io,
            partition_by=["region"],
        )

        run_task(
            "producer",
            {"__job_name__": "j", "__task_key__": "producer"},
        )

        assert get_task_value("producer", "__partition_values__") == {
            "region": ["us-east"],
        }

    def test_sequential_writes_only_return_latest(self):
        """Second write replaces _last_partition_values, not accumulates."""

        class _SequentialIo(IoManager):
            def __init__(self) -> None:
                self._call_count = 0

            def write(self, context: OutputContext, obj: Any) -> None:
                self._call_count += 1
                if self._call_count == 1:
                    self._last_partition_values = {"date": ["2026-03-01"]}
                else:
                    self._last_partition_values = {"date": ["2026-03-02"]}

            def read(self, context: InputContext) -> Any:
                return "data"

        io = _SequentialIo()
        ctx1 = OutputContext(
            job_name="j", task_key="t", run_id="r1", partition_by=["date"]
        )
        ctx2 = OutputContext(
            job_name="j", task_key="t", run_id="r2", partition_by=["date"]
        )

        io.write(ctx1, "data")
        assert io._extract_partition_values(ctx1) == {"date": ["2026-03-01"]}

        io.write(ctx2, "data")
        assert io._extract_partition_values(ctx2) == {"date": ["2026-03-02"]}


class TestStaticBackfillRuntime:
    """Runtime handles StaticBackfill keys that are not ISO-8601 dates."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def _register_static_job(self, keys: list[str]) -> None:
        _JOB_REGISTRY["j"] = JobMeta(
            fn=lambda: None,
            name="j",
            backfill=StaticBackfill(keys=keys),
        )

    def test_non_iso_key_does_not_crash(self):
        """StaticBackfill with non-ISO key (e.g. 'us') must not crash."""
        self._register_static_job(["us", "eu", "jp"])
        call_log: list[str] = []

        def my_task():
            call_log.append("executed")

        _TASK_REGISTRY["j.my_task"] = TaskMeta(fn=my_task, task_key="my_task")

        run_task(
            "my_task",
            {"__job_name__": "j", "__task_key__": "my_task", "backfill_key": "us"},
        )

        assert call_log == ["executed"]

    def test_non_iso_key_backfill_key_is_raw_string(self):
        """For StaticBackfill, backfill_key in IoManager contexts is the raw key."""
        self._register_static_job(["us", "eu"])
        captured_ctx: list[OutputContext] = []

        class _CapturingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                captured_ctx.append(context)

            def read(self, context: InputContext) -> Any:
                return None

        io = _CapturingIo()
        _TASK_REGISTRY["j.t"] = TaskMeta(fn=lambda: "data", task_key="t", io_manager=io)

        run_task(
            "t",
            {"__job_name__": "j", "__task_key__": "t", "backfill_key": "us"},
        )

        assert len(captured_ctx) == 1
        assert captured_ctx[0].backfill_key == "us"


class TestOptionalOutput:
    """Runtime handles tasks that return None (optional outputs)."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_none_output_signals_downstream(self):
        """When a task with io_manager returns None, downstream receives None."""
        io = _MemoryIo()

        def producer():
            return None

        captured: dict[str, Any] = {}

        def consumer(data):
            captured["data"] = data

        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=producer, task_key="producer", io_manager=io
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=consumer, task_key="consumer")

        # Run producer — returns None, should not write but should signal
        run_task("producer", {"__job_name__": "j", "__task_key__": "producer"})

        # Verify no data was written
        assert "producer" not in io.storage

        # Run consumer — should receive None without calling IoManager.read()
        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__data": "producer",
            },
        )

        assert captured["data"] is None

    def test_none_output_skips_io_manager_read(self):
        """IoManager.read() is not called when upstream produced None."""
        read_called = []

        class _TrackingIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                pass

            def read(self, context: InputContext) -> Any:
                read_called.append(True)
                return "should not be returned"

        io = _TrackingIo()

        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=lambda: None, task_key="producer", io_manager=io
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(
            fn=lambda data: data, task_key="consumer"
        )

        run_task("producer", {"__job_name__": "j", "__task_key__": "producer"})
        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__data": "producer",
            },
        )

        assert read_called == []

    def test_non_none_output_still_reads_normally(self):
        """When upstream produces a value, downstream reads via IoManager as usual."""
        io = _MemoryIo()

        def producer():
            return {"rows": [1, 2, 3]}

        captured: dict[str, Any] = {}

        def consumer(data):
            captured["data"] = data

        _TASK_REGISTRY["j.producer"] = TaskMeta(
            fn=producer, task_key="producer", io_manager=io
        )
        _TASK_REGISTRY["j.consumer"] = TaskMeta(fn=consumer, task_key="consumer")

        run_task("producer", {"__job_name__": "j", "__task_key__": "producer"})
        run_task(
            "consumer",
            {
                "__job_name__": "j",
                "__task_key__": "consumer",
                "__upstream__data": "producer",
            },
        )

        assert captured["data"] == {"rows": [1, 2, 3]}


class TestWriteRetry:
    """Tests for IoManager write retry behaviour."""

    def setup_method(self):
        reset_registries()
        _MemoryIo.storage = {}
        _local_task_values.clear()

    def test_retry_succeeds_on_second_attempt(self):
        """Write succeeds after a transient failure."""
        call_count = 0

        class _FlakeyIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                nonlocal call_count
                call_count += 1
                if call_count < 2:
                    raise RuntimeError("CommitFailedError: concurrent write")
                _MemoryIo.storage[context.task_key] = obj

            def read(self, context: InputContext) -> Any:
                return _MemoryIo.storage.get(context.upstream_task_key)

        io = _FlakeyIo()
        io.retry = RetryConfig(max_attempts=3, delay=0.01, backoff_factor=1.0)

        _TASK_REGISTRY["j.t"] = TaskMeta(fn=lambda: "data", task_key="t", io_manager=io)

        run_task("t", {"__job_name__": "j", "__task_key__": "t"})

        assert call_count == 2
        assert _MemoryIo.storage["t"] == "data"

    def test_retry_exhausted_raises(self):
        """Write raises after all attempts are exhausted."""

        class _AlwaysFailIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                raise RuntimeError("CommitFailedError: concurrent write")

            def read(self, context: InputContext) -> Any:
                return None

        io = _AlwaysFailIo()
        io.retry = RetryConfig(max_attempts=3, delay=0.01, backoff_factor=1.0)

        _TASK_REGISTRY["j.t"] = TaskMeta(fn=lambda: "data", task_key="t", io_manager=io)

        with pytest.raises(RuntimeError, match="CommitFailedError"):
            run_task("t", {"__job_name__": "j", "__task_key__": "t"})

    def test_no_retry_when_not_configured(self):
        """Without retry config, failure propagates immediately."""

        class _FailOnceIo(IoManager):
            def write(self, context: OutputContext, obj: Any) -> None:
                raise RuntimeError("write failed")

            def read(self, context: InputContext) -> Any:
                return None

        io = _FailOnceIo()
        # retry is None by default

        _TASK_REGISTRY["j.t"] = TaskMeta(fn=lambda: "data", task_key="t", io_manager=io)

        with pytest.raises(RuntimeError, match="write failed"):
            run_task("t", {"__job_name__": "j", "__task_key__": "t"})

    def test_retry_config_passed_via_constructor(self):
        """Concrete IoManagers accept retry in __init__."""
        cfg = RetryConfig(max_attempts=5, delay=2.0, backoff_factor=3.0)
        io = PolarsDeltaIoManager(base_path="/data/test", retry=cfg)

        assert io.retry is cfg
        assert io.retry.max_attempts == 5
        assert io.retry.delay == 2.0
        assert io.retry.backoff_factor == 3.0
