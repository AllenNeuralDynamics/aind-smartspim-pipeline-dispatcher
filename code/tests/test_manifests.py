"""Tests for manifests/builder.py"""

import json

import pytest
from manifests.builder import get_processing_manifest_path


def test_get_processing_manifest_path_in_derivatives(tmp_path):
    derivatives = tmp_path / "derivatives"
    derivatives.mkdir()
    manifest = derivatives / "processing_manifest.json"
    manifest.write_text(json.dumps({}))

    result = get_processing_manifest_path(tmp_path)
    assert result == manifest


def test_get_processing_manifest_path_in_spim_derivatives(tmp_path):
    spim = tmp_path / "SPIM" / "derivatives"
    spim.mkdir(parents=True)
    manifest = spim / "processing_manifest.json"
    manifest.write_text(json.dumps({}))

    result = get_processing_manifest_path(tmp_path)
    assert result == manifest


def test_get_processing_manifest_path_not_found(tmp_path):
    result = get_processing_manifest_path(tmp_path)
    assert result is None


def test_get_processing_manifest_path_nonexistent_folder(tmp_path):
    nonexistent = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError):
        get_processing_manifest_path(nonexistent)
