"""Tests for ResourceMonitor in utils/schemas.py"""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

# Stub optional deps that are transitively needed by utils/__init__.py but may
# not be installed in the test environment (e.g., smartsheet_dataframe, pytz).
# We must NOT stub aind_data_schema here so that ResourceMonitor's real v2
# type annotations (ResourceTimestamped, ResourceUsage) are available.
_STUBS = [
    "smartsheet_dataframe",
    "pytz",
    "dask",
    "dask.array",
    "dask.distributed",
    "aind_codeocean_api",
    "aind_codeocean_api.codeocean",
    "aind_codeocean_api.models",
    "aind_codeocean_api.models.data_assets_requests",
    "zarr",
    "boto3",
    "botocore",
    "botocore.exceptions",
    "requests",
]
for _mod in _STUBS:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.schemas import ResourceMonitor  # noqa: E402


class TestResourceMonitorCollectsSamples:
    def test_collects_cpu_and_ram_samples(self):
        from aind_data_schema.core.processing import ResourceUsage

        monitor = ResourceMonitor(interval_seconds=0.05).start()
        time.sleep(0.25)
        monitor.stop()
        usage = monitor.to_resource_usage(cpu_cores=2)
        assert isinstance(usage, ResourceUsage)
        assert len(usage.cpu_usage) > 0
        assert len(usage.ram_usage) > 0

    def test_ram_unit_is_set(self):
        monitor = ResourceMonitor(interval_seconds=0.05).start()
        time.sleep(0.15)
        monitor.stop()
        usage = monitor.to_resource_usage()
        assert usage.ram_unit is not None

    def test_os_and_architecture_populated(self):
        monitor = ResourceMonitor(interval_seconds=0.05).start()
        time.sleep(0.1)
        monitor.stop()
        usage = monitor.to_resource_usage()
        assert usage.os
        assert usage.architecture


class TestResourceMonitorContextManager:
    def test_context_manager_collects_samples(self):
        from aind_data_schema.core.processing import ResourceUsage

        with ResourceMonitor(interval_seconds=0.05) as monitor:
            time.sleep(0.15)
        usage = monitor.to_resource_usage()
        assert isinstance(usage, ResourceUsage)
        assert len(usage.cpu_usage) > 0

    def test_stop_is_idempotent(self):
        monitor = ResourceMonitor(interval_seconds=0.05).start()
        time.sleep(0.1)
        monitor.stop()
        monitor.stop()  # second call must not raise
