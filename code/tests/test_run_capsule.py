"""Tests for run_capsule helpers: get_version and input/output bucket resolution."""

import argparse
import sys
from unittest.mock import MagicMock, patch

import pytest

# Stub optional deps transitively needed by run_capsule.py / utils/__init__.py
# that may not be installed in the test environment.
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
    "boto3",
    "botocore",
    "botocore.exceptions",
    "requests",
    "zarr",
    "dotenv",
    "log_schema",
]
for _mod in _STUBS:
    sys.modules.setdefault(_mod, MagicMock())

from run_capsule import _resolve_buckets  # noqa: E402
from utils.versioning import get_version  # noqa: E402


def test_get_version_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '__version__ = "1.2.3"'

    with patch("requests.get", return_value=mock_response):
        version = get_version(
            owner="AllenNeuralDynamics",
            repo="aind-smartspim-fuse",
            path="some/path",
        )
    assert version == "1.2.3"


def test_get_version_no_match_in_response():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "no version here"

    with patch("requests.get", return_value=mock_response):
        version = get_version(
            owner="AllenNeuralDynamics",
            repo="aind-smartspim-fuse",
            path="some/path",
        )
    assert version == "Version not found"


def test_get_version_http_failure():
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("requests.get", return_value=mock_response):
        version = get_version(
            owner="AllenNeuralDynamics",
            repo="aind-smartspim-fuse",
            path="some/path",
        )
    assert version is None


class TestResolveBuckets:
    """Precedence for (effective_input, effective_output):
    named flag > positional arg > env var, input falling back to output."""

    ENV_VARS = ("OUTPUT_BUCKET", "OUTPUT_PATH", "INPUT_BUCKET", "INPUT_PATH")

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        for var in self.ENV_VARS:
            monkeypatch.delenv(var, raising=False)

    def _args(self, **kwargs):
        base = dict(
            output_path=None,
            output_path_pos=None,
            input_path=None,
            input_path_pos=None,
        )
        base.update(kwargs)
        return argparse.Namespace(**base)

    def test_input_falls_back_to_output_cloud(self, monkeypatch):
        monkeypatch.setenv("OUTPUT_BUCKET", "out-bucket")
        assert _resolve_buckets(self._args(), cloud_mode=True) == (
            "out-bucket",
            "out-bucket",
        )

    def test_separate_input_bucket_cloud(self, monkeypatch):
        monkeypatch.setenv("OUTPUT_BUCKET", "out-bucket")
        monkeypatch.setenv("INPUT_BUCKET", "raw-bucket")
        assert _resolve_buckets(self._args(), cloud_mode=True) == (
            "raw-bucket",
            "out-bucket",
        )

    def test_local_mode_uses_local_paths(self, monkeypatch):
        monkeypatch.setenv("OUTPUT_PATH", "/scratch/output")
        monkeypatch.setenv("INPUT_PATH", "/scratch/raw")
        # local mode must ignore the bucket env vars entirely
        monkeypatch.setenv("OUTPUT_BUCKET", "ignored")
        monkeypatch.setenv("INPUT_BUCKET", "ignored")
        assert _resolve_buckets(self._args(), cloud_mode=False) == (
            "/scratch/raw",
            "/scratch/output",
        )

    def test_local_input_falls_back_to_output_path(self, monkeypatch):
        monkeypatch.setenv("OUTPUT_PATH", "/scratch/output")
        assert _resolve_buckets(self._args(), cloud_mode=False) == (
            "/scratch/output",
            "/scratch/output",
        )

    def test_flag_beats_positional_beats_env(self, monkeypatch):
        monkeypatch.setenv("INPUT_BUCKET", "env-in")
        monkeypatch.setenv("OUTPUT_BUCKET", "env-out")
        args = self._args(
            input_path="flag-in",
            input_path_pos="pos-in",
            output_path_pos="pos-out",
        )
        assert _resolve_buckets(args, cloud_mode=True) == ("flag-in", "pos-out")

        args = self._args(input_path_pos="pos-in")
        assert _resolve_buckets(args, cloud_mode=True) == ("pos-in", "env-out")

    def test_nothing_set_is_falsy(self):
        effective_input, effective_output = _resolve_buckets(
            self._args(), cloud_mode=True
        )
        assert not effective_input
        assert not effective_output
