"""Tests for run_s3_transfer and get_raw_dataset_name."""

import logging
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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

import pytest  # noqa: E402

from utils.io import run_s3_transfer  # noqa: E402
from utils.metadata_compat import get_raw_dataset_name  # noqa: E402


class TestGetRawDatasetName:
    def test_v1_derived_name_is_stripped(self):
        assert (
            get_raw_dataset_name(
                "SmartSPIM_695464_2023-10-28_16-02-45_stitched_2023-10-31_22-14-46"
            )
            == "SmartSPIM_695464_2023-10-28_16-02-45"
        )

    def test_v2_derived_name_is_stripped(self):
        assert (
            get_raw_dataset_name("861555_2026-07-09_04-33-35_stitched_2026-08-19_22-40-17")
            == "861555_2026-07-09_04-33-35"
        )

    def test_raw_names_pass_through(self):
        assert (
            get_raw_dataset_name("SmartSPIM_861555_2026-07-14_23-40-43")
            == "SmartSPIM_861555_2026-07-14_23-40-43"
        )
        assert get_raw_dataset_name("861555_2026-07-09_04-33-35") == "861555_2026-07-09_04-33-35"

    def test_none_and_empty_are_preserved(self):
        assert get_raw_dataset_name(None) is None
        assert get_raw_dataset_name("") == ""


class TestRunS3Transfer:
    def _capture(self, output_lines, raise_error=False):
        """Runs run_s3_transfer with a fake command generator, returns records."""

        def fake_helper(command):
            yield from output_lines
            if raise_error:
                raise subprocess.CalledProcessError(1, command)

        logger = logging.getLogger("test_run_s3_transfer")
        logger.setLevel(logging.DEBUG)
        records = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = _Capture()
        logger.addHandler(handler)
        try:
            with patch("utils.utils.execute_command_helper", side_effect=fake_helper):
                run_s3_transfer("aws s3 cp a b", logger, "test transfer")
        finally:
            logger.removeHandler(handler)
        return records

    def test_progress_lines_are_not_logged(self):
        records = self._capture(
            [
                "Completed 98.4 GiB/~101.1 GiB (155.7 MiB/s) with ~1005 file(s) remaining",
                "upload: ../data/fused/x to s3://bucket/x",
                "copy: s3://a to s3://b",
                "move: s3://a to s3://b",
                "download: s3://a to local",
            ]
        )
        messages = [r.getMessage() for r in records]
        assert not any("Completed" in m or "upload:" in m for m in messages)
        # only the two summary records at INFO
        info_records = [r for r in records if r.levelno == logging.INFO]
        assert [getattr(r, "event_type", None) for r in info_records] == [
            "transfer_start",
            "transfer_end",
        ]
        end = info_records[1]
        assert end.status == "success"
        assert end.duration_seconds >= 0

    def test_non_progress_output_goes_to_debug(self):
        records = self._capture(["warning: skipping weird file"])
        debug_records = [r for r in records if r.levelno == logging.DEBUG]
        assert any("skipping weird file" in r.getMessage() for r in debug_records)

    def test_failure_still_raises_and_logs_failed_end(self):
        with pytest.raises(subprocess.CalledProcessError):
            self._capture(["upload: a to b"], raise_error=True)
        # re-run capturing records to check the end event status
        records = []
        logger = logging.getLogger("test_run_s3_transfer_fail")
        logger.setLevel(logging.DEBUG)

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger.addHandler(_Capture())

        def fake_helper(command):
            yield "upload: a to b"
            raise subprocess.CalledProcessError(1, command)

        with patch("utils.utils.execute_command_helper", side_effect=fake_helper):
            with pytest.raises(subprocess.CalledProcessError):
                run_s3_transfer("aws s3 cp a b", logger, "failing transfer")

        end = [r for r in records if getattr(r, "event_type", None) == "transfer_end"]
        assert len(end) == 1 and end[0].status == "failed"

    def test_extra_fields_propagate(self):
        records = self._capture([])
        start = [r for r in records if getattr(r, "event_type", None) == "transfer_start"]
        assert start and start[0].command == "aws s3 cp a b"