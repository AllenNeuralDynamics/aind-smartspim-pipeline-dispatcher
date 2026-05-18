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
    output_path: str,
    logger: logging.Logger,
    cloud_mode: bool = True,
) -> Tuple[str, list, dict]:
    """
    Handles the split_channels mode: lists available channels and writes
    per-channel preprocessing manifest JSON files.

    In cloud mode, channels are discovered via S3.  In local mode, they are
    read from subdirectories under {output_path}/{dataset_name}/SPIM/.

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

    if not output_path:
        logger.warning("Output path not set; skipping split_channels channel listing.")
    elif cloud_mode:
        bucket_name = output_path
        BASE_PATH = f"s3://{bucket_name}/{dataset_name}/SPIM"
        prefix = f"{dataset_name}/SPIM"
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

    else:
        spim_path = Path(output_path) / dataset_name / "SPIM"
        if not spim_path.exists():
            logger.warning(f"SPIM path does not exist: {spim_path}; skipping split_channels.")
        else:
            channels = [d.name for d in spim_path.iterdir() if d.is_dir() and "Ex" in d.name]
            for ch in channels:
                utils.save_dict_as_json(
                    f"{results_folder}/preprocess_{ch}.json",
                    {
                        "input_data": str(spim_path),
                        "channel": ch,
                    },
                )

    return dataset_name, investigators or [], {}
