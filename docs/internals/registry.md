# Registry

## Testing with registries

Decorators store metadata in global, mutable registries that persist for the lifetime of the process. In test suites, state from one test leaks into the next unless explicitly cleared. Always call `reset_registries()` at the start of each test:

```python
from databricks_bundle_decorators.registry import reset_registries

class TestMyPipeline:
    def setup_method(self):
        reset_registries()
```

---

::: databricks_bundle_decorators.registry
