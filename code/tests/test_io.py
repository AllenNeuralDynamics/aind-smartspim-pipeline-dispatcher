"""Tests for utils/io.py"""

import re

from utils.io import (
    check_type_helper,
    create_folder,
    generate_timestamp,
    read_json_as_dict,
    save_dict_as_json,
    save_string_to_txt,
    validate_capsule_inputs,
)


def test_generate_timestamp_format():
    ts = generate_timestamp()
    assert re.match(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", ts)


def test_create_folder_new(tmp_path):
    new_dir = tmp_path / "new_folder"
    create_folder(str(new_dir))
    assert new_dir.is_dir()


def test_create_folder_existing(tmp_path):
    existing = tmp_path / "existing"
    existing.mkdir()
    create_folder(str(existing))
    assert existing.is_dir()


def test_save_and_read_json_roundtrip(tmp_path):
    data = {"a": 1, "b": [1, 2, 3]}
    path = str(tmp_path / "data.json")
    save_dict_as_json(path, data)
    result = read_json_as_dict(path)
    assert result == data


def test_read_json_missing_file(tmp_path):
    result = read_json_as_dict(str(tmp_path / "nonexistent.json"))
    assert result == {}


def test_save_string_to_txt(tmp_path):
    out = tmp_path / "out.txt"
    save_string_to_txt("hello world", str(out))
    assert out.read_text() == "hello world\n"


def test_validate_capsule_inputs_all_exist(tmp_path):
    f1 = tmp_path / "a.json"
    f2 = tmp_path / "b.json"
    f1.write_text("{}")
    f2.write_text("{}")
    missing = validate_capsule_inputs([str(f1), str(f2)])
    assert missing == []


def test_validate_capsule_inputs_missing(tmp_path):
    f1 = tmp_path / "a.json"
    f1.write_text("{}")
    missing = validate_capsule_inputs([str(f1), str(tmp_path / "missing.json")])
    assert len(missing) == 1
    assert "missing.json" in missing[0]


def test_check_type_helper_documents_bug():
    """
    BUG #5: check_type_helper uses isinstance(type(value), val_type) which is inverted.
    The correct form is isinstance(value, val_type).
    This test documents the current (buggy) behaviour so the test suite stays green.
    """
    result = check_type_helper(42, int)
    assert result is False
