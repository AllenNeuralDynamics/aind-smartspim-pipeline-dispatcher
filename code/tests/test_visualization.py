"""Tests for utils/visualization.py"""

import pytest
from utils.visualization import volume_orientation, wavelength_to_hex_alternate


def test_wavelength_to_hex_alternate_low():
    result = wavelength_to_hex_alternate(440)
    assert result == "#3300FF"


def test_wavelength_to_hex_alternate_mid():
    result = wavelength_to_hex_alternate(561)
    assert result == "#AAFF00"


def test_wavelength_to_hex_alternate_high():
    result = wavelength_to_hex_alternate(800)
    assert result == "#FFFFFF"


def test_volume_orientation_ras():
    params = {
        "axes": [
            {"dimension": 0, "direction": "R"},
            {"dimension": 1, "direction": "A"},
            {"dimension": 2, "direction": "S"},
        ]
    }
    result = volume_orientation(params)
    assert result == [0.0, 0.0, 0.0, 1.0]


def test_volume_orientation_lps():
    params = {
        "axes": [
            {"dimension": 0, "direction": "L"},
            {"dimension": 1, "direction": "P"},
            {"dimension": 2, "direction": "S"},
        ]
    }
    result = volume_orientation(params)
    assert result == [0.0, 1.0, 0.0, 0.0]


def test_volume_orientation_unknown_raises():
    """ValueError is raised for unrecognised orientations."""
    params = {
        "axes": [
            {"dimension": 0, "direction": "X"},
            {"dimension": 1, "direction": "Y"},
            {"dimension": 2, "direction": "Z"},
        ]
    }
    with pytest.raises(ValueError) as exc_info:
        volume_orientation(params)
    assert "XYZ" in str(exc_info.value)
