"""Integration-style tests for run_capsule.py top-level helpers."""

from unittest.mock import MagicMock, patch

import pytest

import run_capsule


def test_get_version_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '__version__ = "1.2.3"'

    with patch("requests.get", return_value=mock_response):
        version = run_capsule.get_version(
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
        version = run_capsule.get_version(
            owner="AllenNeuralDynamics",
            repo="aind-smartspim-fuse",
            path="some/path",
        )
    assert version == "Version not found"


def test_get_version_http_failure():
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("requests.get", return_value=mock_response):
        version = run_capsule.get_version(
            owner="AllenNeuralDynamics",
            repo="aind-smartspim-fuse",
            path="some/path",
        )
    assert version is None
