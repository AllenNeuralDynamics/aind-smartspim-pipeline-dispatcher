"""Tests for the pipeline_plan log derivation in split_channels."""

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Stub optional deps transitively needed by utils/__init__.py.
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

from modes.split_channels import build_pipeline_plan, handle_split_channels  # noqa: E402

CORE_STAGES = [
    "aind-smartspim-flatfield-estimation",
    "aind-smartspim-destripe",
    "aind-smartspim-stitch",
    "aind-smartspim-fuse",
]


class TestBuildPipelinePlan:
    def test_registration_and_segmentation(self):
        manifest = {
            "pipeline_processing": {
                "registration": {"channels": ["Ex_639_Em_680"]},
                "segmentation": {"channels": ["Ex_488_Em_525"]},
            }
        }
        stages = build_pipeline_plan(manifest)
        assert stages == CORE_STAGES + [
            "aind-smartspim-ccf-registration",
            "aind-smartspim-segmentation",
            "aind-smartspim-classification",
            "aind-smartspim-quantification",
        ]
        assert stages[-1] == "aind-smartspim-quantification"

    def test_registration_only(self):
        manifest = {
            "pipeline_processing": {
                "registration": {"channels": ["Ex_639_Em_680"]},
                "segmentation": {"channels": []},
            }
        }
        stages = build_pipeline_plan(manifest)
        assert stages == CORE_STAGES + ["aind-smartspim-ccf-registration"]
        assert stages[-1] == "aind-smartspim-ccf-registration"

    def test_segmentation_only_has_no_quantification(self):
        manifest = {
            "pipeline_processing": {
                "segmentation": {"channels": ["Ex_488_Em_525"]},
            }
        }
        stages = build_pipeline_plan(manifest)
        assert stages == CORE_STAGES + [
            "aind-smartspim-segmentation",
            "aind-smartspim-classification",
        ]

    def test_legacy_alias_keys(self):
        manifest = {
            "pipeline_processing": {
                "ccf_registration": {"channels": ["Ex_639_Em_680"]},
                "cell_segmentation_channels": {"channels": ["Ex_488_Em_525"]},
            }
        }
        stages = build_pipeline_plan(manifest)
        assert "aind-smartspim-ccf-registration" in stages
        assert "aind-smartspim-quantification" in stages

    def test_missing_sections_yield_core_stages(self):
        assert build_pipeline_plan({}) == CORE_STAGES
        assert build_pipeline_plan({"pipeline_processing": {}}) == CORE_STAGES
        assert build_pipeline_plan({"pipeline_processing": None}) == CORE_STAGES


class TestPipelinePlanRecordEmitted:
    def test_handle_split_channels_emits_pipeline_plan(self, tmp_path):
        dataset = "SmartSPIM_861555_2026-07-14_23-40-43"
        spim = tmp_path / "input" / dataset / "SPIM"
        (spim / "Ex_639_Em_680").mkdir(parents=True)

        data_folder = tmp_path / "data"
        metadata_dir = data_folder / "input_aind_metadata"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "data_description.json").write_text(
            json.dumps({"name": dataset, "investigators": []})
        )
        (data_folder / "processing_manifest.json").write_text(
            json.dumps(
                {
                    "pipeline_processing": {
                        "registration": {"channels": ["Ex_639_Em_680"]},
                        "segmentation": {"channels": []},
                    }
                }
            )
        )
        results_folder = tmp_path / "results"
        results_folder.mkdir()

        records = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test_pipeline_plan")
        logger.setLevel(logging.INFO)
        logger.addHandler(_Capture())

        handle_split_channels(
            data_folder=data_folder,
            results_folder=results_folder,
            input_path=str(tmp_path / "input"),
            logger=logger,
            cloud_mode=False,
        )

        plans = [r for r in records if getattr(r, "event_type", None) == "pipeline_plan"]
        assert len(plans) == 1
        plan = plans[0]
        assert plan.dataset_name == dataset
        assert plan.expected_stages[-1] == "aind-smartspim-ccf-registration"
        assert plan.expected_stage_count == len(plan.expected_stages) == 5
        assert plan.terminal_stage == "aind-smartspim-ccf-registration"
