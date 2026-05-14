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
    params = {"axes": [{"direction": "R"}, {"direction": "A"}, {"direction": "S"}]}
    result = volume_orientation(params)
    assert result == [0.0, 0.0, 0.0, 1.0]


def test_volume_orientation_lps():
    params = {"axes": [{"direction": "L"}, {"direction": "P"}, {"direction": "S"}]}
    result = volume_orientation(params)
    assert result == [0.0, 1.0, 0.0, 0.0]


def test_volume_orientation_unknown_raises_with_literal_braces():
    """
    BUG #3: ValueError message uses {acquired} without f-string prefix, so the
    variable name appears literally in the message text (not its value).
    """
    params = {"axes": [{"direction": "X"}, {"direction": "Y"}, {"direction": "Z"}]}
    with pytest.raises(ValueError) as exc_info:
        volume_orientation(params)
    assert "{acquired}" in str(exc_info.value)
