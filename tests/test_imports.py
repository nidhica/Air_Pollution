"""Import every milestone package without starting Spark or reading data."""

import importlib

import pytest

PACKAGES = [
    "ingestion",
    "preprocessing",
    "analytics",
    "analytics.spikes",
    "analytics.spatiotemporal",
    "storage",
    "dashboard",
    "performance",
    "utils",
    "utils.config_loader",
]


@pytest.mark.parametrize("module_name", PACKAGES)
def test_package_imports_without_a_dataset(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert module is not None
