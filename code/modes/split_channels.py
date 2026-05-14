"""
Split-channels mode — lists available S3 channels and writes per-channel
preprocessing manifests.
"""

import logging
from pathlib import Path
from typing import Tuple, Union

from utils import utils
from manifests.builder import get_data_config

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def handle_split_channels(
    data_folder: PathLike,
    results_folder: PathLike,
    output_bucket: str,
    logger: logging.Logger,
) -> Tuple[str, list, dict]:
    """
    Handles the split_channels mode: lists S3 channels and writes per-channel
    preprocessing manifest JSON files.

    Returns
    -------
    Tuple[str, list, dict]
        (dataset_name, investigators, email_message_params)
    """
    logger.info("Starting channel splitting...")

    pipeline_config, dataset_name, investigators = get_data_config(
        data_folder=data_folder,
        data_description_path="input_aind_metadata/data_description.json",
    )

    bucket_name = output_bucket
    if not bucket_name:
        logger.warning("OUTPUT_BUCKET not set; skipping split_channels S3 listing.")
    else:
        BASE_PATH = f"s3://{bucket_name}/"
        prefix = f"{dataset_name}/SPIM"
        BASE_PATH = f"{BASE_PATH}{prefix}"
        channels = [
            i
            for i in utils.list_s3_folders(bucket=bucket_name, prefix=prefix)
            if "Ex" in i
        ]

        for ch in channels:
            utils.save_dict_as_json(
                f"{results_folder}/preprocess_{ch}.json",
                {
                    "input_data": BASE_PATH,
                    "channel": ch,
                },
            )

    return dataset_name, investigators or [], {}
