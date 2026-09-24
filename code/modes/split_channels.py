"""
Split-channels mode — lists available S3 channels and writes per-channel
preprocessing manifests.
"""

import logging
from pathlib import Path
from typing import List, Tuple, Union

from manifests.builder import get_data_config
from utils import utils

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def _get_section_channels(sections: dict, section_names: List[str]) -> list:
    """
    Returns the channel list of the first manifest section found among
    the given names (legacy manifests use alias keys).
    """
    for section_name in section_names:
        section = sections.get(section_name)
        if isinstance(section, dict):
            return section.get("channels") or []

    return []


def build_pipeline_plan(pipeline_config: dict) -> List[str]:
    """
    Derives the expected pipeline stages from the processing manifest.

    The imaging stages always run; atlas registration, cell segmentation,
    classification and quantification depend on the channels requested in
    the manifest. Quantification requires both registration and
    segmentation channels since it maps detected cells into CCF space.

    Parameters
    ----------
    pipeline_config: dict
        Parsed processing_manifest.json (with or without the
        top-level "pipeline_processing" key).

    Returns
    -------
    List[str]
        Expected stages in pipeline order.
    """
    sections = pipeline_config.get("pipeline_processing", pipeline_config) or {}

    registration_channels = _get_section_channels(sections, ["registration", "ccf_registration"])
    segmentation_channels = _get_section_channels(
        sections, ["segmentation", "cell_segmentation_channels"]
    )

    expected_stages = [
        "aind-smartspim-flatfield-estimation",
        "aind-smartspim-destripe",
        "aind-smartspim-stitch",
        "aind-smartspim-fuse",
    ]

    if len(registration_channels):
        expected_stages.append("aind-smartspim-ccf-registration")

    if len(segmentation_channels):
        expected_stages.append("aind-smartspim-segmentation")
        expected_stages.append("aind-smartspim-classification")

    if len(registration_channels) and len(segmentation_channels):
        expected_stages.append("aind-smartspim-quantification")

    return expected_stages


def handle_split_channels(
    data_folder: PathLike,
    results_folder: PathLike,
    input_path: str,
    logger: logging.Logger,
    cloud_mode: bool = True,
) -> Tuple[str, list, dict]:
    """
    Handles the split_channels mode: lists available channels and writes
    per-channel preprocessing manifest JSON files.

    In cloud mode, channels are discovered from the raw data in
    s3://{input_path}/{dataset_name}/SPIM/.  In local mode, they are
    read from subdirectories under {input_path}/{dataset_name}/SPIM/.

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

    expected_stages = build_pipeline_plan(pipeline_config)
    logger.info(
        "Pipeline execution plan",
        extra={
            "event_type": "pipeline_plan",
            "dataset_name": dataset_name,
            "expected_stages": expected_stages,
            "expected_stage_count": len(expected_stages),
            "terminal_stage": expected_stages[-1],
        },
    )

    if not input_path:
        logger.warning("Input path not set; skipping split_channels channel listing.")
    elif cloud_mode:
        bucket_name = input_path
        BASE_PATH = f"s3://{bucket_name}/{dataset_name}/SPIM"
        prefix = f"{dataset_name}/SPIM"
        channels = [
            i for i in utils.list_s3_folders(bucket=bucket_name, prefix=prefix) if "Ex" in i
        ]

        logger.info(f"[Cloud mode] Base path: {BASE_PATH} - channels: {channels}")

        for ch in channels:
            utils.save_dict_as_json(
                f"{results_folder}/preprocess_{ch}.json",
                {
                    "input_data": BASE_PATH,
                    "channel": ch,
                },
            )

        if not len(channels):
            raise ValueError(f"No channels were identified in {BASE_PATH}/{prefix}")

    else:
        spim_path = Path(input_path) / dataset_name / "SPIM"
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
