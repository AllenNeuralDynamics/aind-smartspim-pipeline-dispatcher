"""Tests for utils/notifications.py"""

import pytest
from utils.notifications import clean_investigator_names


def test_clean_investigator_names_single():
    result = clean_investigator_names(["Alice Smith"])
    assert result == "Alice Smith"


def test_clean_investigator_names_two():
    result = clean_investigator_names(["Alice Smith", "Bob Jones"])
    assert result == "Alice Smith and Bob Jones"


def test_clean_investigator_names_three():
    result = clean_investigator_names(["Alice Smith", "Bob Jones", "Carol Lee"])
    assert result == "Alice Smith, Bob Jones and Carol Lee"


def test_clean_investigator_names_empty_raises():
    with pytest.raises(IndexError):
        clean_investigator_names([])
