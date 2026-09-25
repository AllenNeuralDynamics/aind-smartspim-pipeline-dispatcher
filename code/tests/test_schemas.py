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

import json  # noqa: E402

from utils.schemas import (  # noqa: E402
    ResourceMonitor,
    generate_data_description,
    validate_metadata_v2,
)

# A legacy v1-format data_description.json (platform/modality/label/related_data,
# registry dicts, string fundee, PIDName investigators) — the shape that crashed
# the dispatcher after the v2 upgrade.
V1_DATA_DESCRIPTION = {
    "creation_time": "2026-04-03T10:55:03-07:00",
    "data_level": "raw",
    "data_summary": None,
    "funding_source": [
        {
            "fundee": "Bosiljka Tasic, Jonathan Ting",
            "funder": {
                "abbreviation": "NIMH",
                "name": "National Institute of Mental Health",
                "registry": {"abbreviation": "ROR", "name": "Research Organization Registry"},
                "registry_identifier": "04xeg9z08",
            },
            "grant_number": "U01MH139778",
        }
    ],
    "group": None,
    "institution": {
        "abbreviation": "AIBS",
        "name": "Allen Institute for Brain Science",
        "registry": {"abbreviation": "ROR", "name": "Research Organization Registry"},
        "registry_identifier": "00dcv1019",
    },
    "investigators": [
        {
            "abbreviation": None,
            "name": "Avery Hunker",
            "registry": None,
            "registry_identifier": None,
        },
        {
            "abbreviation": None,
            "name": "Jack Waters",
            "registry": None,
            "registry_identifier": None,
        },
    ],
    "label": None,
    "license": "CC-BY-4.0",
    "modality": [{"abbreviation": "SPIM", "name": "Selective plane illumination microscopy"}],
    "name": "SmartSPIM_837680_2026-04-03_10-55-03",
    "platform": {"abbreviation": "SmartSPIM", "name": "SmartSPIM platform"},
    "project_name": "AIBS WB AAV Toolbox",
    "related_data": [],
    "restrictions": None,
    "schema_version": "1.0.3",
    "subject_id": "837680",
}


def _build_v2_raw_data_description():
    """Build a genuine v2 RAW DataDescription dict for the fast-path test."""
    from aind_data_schema.components.identifiers import Person
    from aind_data_schema.core.data_description import DataDescription, Funding
    from aind_data_schema_models.data_name_patterns import DataLevel
    from aind_data_schema_models.modalities import Modality
    from aind_data_schema_models.organizations import Organization

    raw = DataDescription(
        data_level=DataLevel.RAW,
        name="SmartSPIM_837680_2026-04-03_10-55-03",
        creation_time="2026-04-03T10:55:03-07:00",
        institution=Organization.AIBS,
        funding_source=[Funding(funder=Organization.AI)],
        investigators=[Person(name="Jane Doe")],
        modalities=[Modality.SPIM],
        project_name="AIBS WB AAV Toolbox",
        subject_id="837680",
    )
    return json.loads(raw.model_dump_json())


class TestGenerateDataDescription:
    def test_v1_input_is_reconstructed_to_valid_v2(self, tmp_path):
        from aind_data_schema.core.data_description import DataDescription
        from aind_data_schema_models.modalities import Modality

        raw = tmp_path / "data_description.json"
        raw.write_text(json.dumps(V1_DATA_DESCRIPTION))
        out = tmp_path / "out"
        out.mkdir()

        name = generate_data_description(str(raw), str(out), process_name="stitched")

        assert name.startswith("SmartSPIM_837680_2026-04-03_10-55-03_stitched_")
        result = json.loads((out / "data_description.json").read_text())
        # Output must be valid v2 and carry the derived/SPIM invariants
        validated = DataDescription.model_validate(result)
        assert result["data_level"] == "derived"
        assert validated.modalities == [Modality.SPIM]
        # v1 string fundee -> list of Person
        assert isinstance(result["funding_source"][0]["fundee"], list)
        assert len(result["investigators"]) == 2

    def test_v2_input_takes_fast_path(self, tmp_path):
        from aind_data_schema.core.data_description import DataDescription

        raw = tmp_path / "data_description.json"
        raw.write_text(json.dumps(_build_v2_raw_data_description()))
        out = tmp_path / "out"
        out.mkdir()

        name = generate_data_description(str(raw), str(out), process_name="stitched")

        assert "stitched" in name
        result = json.loads((out / "data_description.json").read_text())
        DataDescription.model_validate(result)  # still valid v2
        assert result["data_level"] == "derived"


class TestValidateMetadataV2:
    def test_valid_v2_returns_true(self, tmp_path):
        f = tmp_path / "data_description.json"
        f.write_text(json.dumps(_build_v2_raw_data_description()))
        assert validate_metadata_v2(f) is True

    def test_invalid_v1_returns_false_without_raising(self, tmp_path):
        f = tmp_path / "data_description.json"
        f.write_text(json.dumps(V1_DATA_DESCRIPTION))
        assert validate_metadata_v2(f) is False

    def test_unknown_filename_is_skipped(self, tmp_path):
        f = tmp_path / "session.json"
        f.write_text(json.dumps({"anything": True}))
        assert validate_metadata_v2(f) is True


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
