"""
AWS S3 and Secrets Manager utilities.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

import boto3

PathLike = Union[str, Path]


def get_messenger_credentails(secret_id: str) -> Dict:
    """
    Gets messenger credentials from AWS Secrets Manager.

    Note: contains a known dead-code `return secret_dict` after `return None`
    on the exception path (BUGS.md #6). Behaviour preserved intentionally.
    """
    client = boto3.client("secretsmanager", region_name="us-west-2")

    try:
        response = client.get_secret_value(SecretId=secret_id)
        print("response ", response)
        secret_string = response.get("SecretString")

        if secret_string:
            secret_dict = json.loads(secret_string)
            return secret_dict
        else:
            return response.get("SecretBinary")

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

    return secret_dict  # noqa: F821 — unreachable (BUGS.md #6)


def get_messanger_credentails(secret_id):
    """
    Pulls data from AWS Secrets Manager.

    Duplicate of get_messenger_credentails() with a different (misspelled) name.
    See BUGS.md #8 for details.
    """
    client = boto3.client("secretsmanager", region_name="us-west-2")

    try:
        response = client.get_secret_value(SecretId=secret_id)
        secret_string = response.get("SecretString")

        if secret_string:
            secret_dict = json.loads(secret_string)
            return secret_dict
        else:
            return response.get("SecretBinary")

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

    return secret_dict  # noqa: F821 — unreachable (BUGS.md #7)


def list_s3_folders(bucket: str, prefix: str, extension: Optional[str] = None) -> list:
    """
    List top-level 'folders' under a given S3 prefix.
    """
    if not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    folders = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder_name = Path(cp["Prefix"].rstrip("/")).name
            if extension is None or folder_name.endswith(extension):
                folders.append(folder_name)

    return folders


def list_s3_files(bucket: str, prefix: str, extension: str) -> list:
    """
    List files under a given S3 prefix that end with a given extension.
    """
    if not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(extension):
                files.append(key)

    return files


def is_s3_path(path: str) -> bool:
    """
    Returns True if path uses the s3:// scheme.
    """
    parsed = urlparse(str(path))
    return parsed.scheme == "s3"


def split_s3_path(s3_path: str):
    """
    Split an S3 URI into (bucket, prefix).

    Example: "s3://my-bucket/folder/" → ("my-bucket", "folder/")
    """
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix
