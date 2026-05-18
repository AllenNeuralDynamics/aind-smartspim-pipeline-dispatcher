"""Tests for utils/aws.py"""

import json
from unittest.mock import MagicMock, patch

import pytest

from utils.aws import get_messenger_credentails, is_s3_path, split_s3_path


def test_is_s3_path_true():
    assert is_s3_path("s3://my-bucket/some/prefix") is True


def test_is_s3_path_false_local():
    assert is_s3_path("/local/path/to/file") is False


def test_is_s3_path_false_http():
    assert is_s3_path("http://example.com") is False


def test_split_s3_path_basic():
    bucket, prefix = split_s3_path("s3://my-bucket/some/prefix/file.json")
    assert bucket == "my-bucket"
    assert prefix == "some/prefix/file.json"


def test_split_s3_path_no_prefix():
    bucket, prefix = split_s3_path("s3://my-bucket/")
    assert bucket == "my-bucket"
    assert prefix == ""


def test_get_messenger_credentails_success(mock_boto3_client):
    secret_value = json.dumps({"email": "user@example.com", "password": "secret"})
    mock_client_instance = MagicMock()
    mock_client_instance.get_secret_value.return_value = {"SecretString": secret_value}
    mock_boto3_client.return_value = mock_client_instance

    result = get_messenger_credentails("my-secret")
    assert result == {"email": "user@example.com", "password": "secret"}


def test_get_messenger_credentails_binary_secret(mock_boto3_client):
    secret_value = json.dumps({"token": "abc"}).encode()
    mock_client_instance = MagicMock()
    mock_client_instance.get_secret_value.return_value = {"SecretBinary": secret_value}
    mock_boto3_client.return_value = mock_client_instance

    result = get_messenger_credentails("my-secret")
    assert result == {"token": "abc"}


def test_get_messenger_credentails_exception(mock_boto3_client):
    mock_client_instance = MagicMock()
    mock_client_instance.get_secret_value.side_effect = Exception("access denied")
    mock_boto3_client.return_value = mock_client_instance

    result = get_messenger_credentails("my-secret")
    assert result is None
