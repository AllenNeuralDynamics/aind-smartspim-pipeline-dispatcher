"""Shared pytest fixtures."""

import json
from unittest.mock import patch

import pytest


@pytest.fixture
def tmp_json(tmp_path):
    """Write a sample JSON file and return its path."""
    data = {"key": "value", "number": 42}
    p = tmp_path / "sample.json"
    p.write_text(json.dumps(data))
    return p, data


@pytest.fixture
def sample_raw_data_folder(tmp_path):
    """Create a folder with derivatives/processing_manifest.json."""
    derivatives = tmp_path / "derivatives"
    derivatives.mkdir()
    manifest = {"pipeline_processing": {"stitching": {"s3_path": "s3://bucket/dataset"}}}
    (derivatives / "processing_manifest.json").write_text(json.dumps(manifest))
    return tmp_path, manifest


@pytest.fixture
def mock_boto3_client():
    """Patch boto3.client for the aws module."""
    with patch("boto3.client") as mock_client:
        yield mock_client
