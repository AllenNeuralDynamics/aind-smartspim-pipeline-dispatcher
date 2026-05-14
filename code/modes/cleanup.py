"""
Clean-up mode — moves segmentation and quantification results to S3 and
sends a completion alert.
"""

import logging
import os
import re
from glob import glob
from pathlib import Path
from typing import Tuple, Union

from __init__ import __maintainers__, __pipeline_notes__, __pipeline_version__
from utils import utils
from manifests.builder import get_data_config

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def clean_up(
    processing_manifest: dict,
    data_folder: PathLike,
    results_folder: PathLike,
    alert_bot_link: str,
):
    """
    Moves all segmentation and quantification data to the destination S3 bucket.

    Parameters
    ----------
    processing_manifest:
        Full processing manifest dict (must include pipeline_processing.stitching.s3_path).
    data_folder:
        Code Ocean data folder path.
    results_folder:
        Code Ocean results folder path.
    alert_bot_link:
        MS Teams webhook URL for pipeline notifications.
    """
    logger.info(f"Data folder: {os.listdir(data_folder)}")

    cell_folders = glob(f"{data_folder}/cell_*")
    quantification_folders = glob(f"{data_folder}/quant_*")

    logger.info(f"Cell folders: {cell_folders}")
    logger.info(f"Quantification folders: {quantification_folders}")

    proposals_processing = []
    for cell_folder in cell_folders:
        processing_jsons = [
            p
            for p in glob(f"{cell_folder}/proposals/metadata/*processing*.json")
            if "manifest" not in str(p)
        ]
        proposals_processing.append(processing_jsons)

    segmentation_processing = []
    for cell_folder in cell_folders:
        processing_jsons = [
            p
            for p in glob(f"{cell_folder}/metadata/*processing*.json")
            if "manifest" not in str(p)
        ]
        segmentation_processing.append(processing_jsons)

    quantification_processing = []
    for quant_folder in quantification_folders:
        processing_jsons = [
            p
            for p in glob(f"{quant_folder}/metadata/*processing*.json")
            if "manifest" not in str(p)
        ]
        quantification_processing.append(processing_jsons)

    processing_paths = list()
    combined_processing_list = (
        [[f"{data_folder}/input_aind_metadata/processing.json"]]
        + proposals_processing
        + segmentation_processing
        + quantification_processing
    )
    for sub_list in combined_processing_list:
        processing_paths += sub_list

    logger.info(f"Compiling processing paths: {processing_paths}")

    if len(processing_paths) > 1:
        output_filename = utils.compile_processing_jsons(
            processing_paths=processing_paths,
            output_general_processing=results_folder,
            processor_full_name=__maintainers__[0],
            pipeline_version=__pipeline_version__,
            pipeline_notes=__pipeline_notes__,
        )

        logger.info(f"Compiled processing.json in path {output_filename}")

        s3_path = processing_manifest["pipeline_processing"]["stitching"]["s3_path"]
        cell_s3_output = f"{s3_path}/image_cell_segmentation"
        quantification_s3_output = f"{s3_path}/image_cell_quantification"

        regex_channels = r"Ex_(\d{3})_Em_(\d{3})$"

        for out in utils.execute_command_helper(
            f"aws s3 cp {output_filename}/processing.json {s3_path}/processing.json"
        ):
            print(out)

        for cell_folder in cell_folders:
            channel_name = re.search(regex_channels, cell_folder).group()

            for out in utils.execute_command_helper(
                f"aws s3 mv --recursive {cell_folder} {cell_s3_output}/{channel_name}"
            ):
                print(out)

        for quantification_folder in quantification_folders:
            channel_name = re.search(regex_channels, quantification_folder).group()

            for out in utils.execute_command_helper(
                f"aws s3 mv --recursive {quantification_folder} {quantification_s3_output}/{channel_name}"
            ):
                print(out)

        utils.save_string_to_txt(
            f"Results of cell segmentation saved in: {cell_s3_output}",
            f"{results_folder}/output_cell.txt",
        )

        utils.save_string_to_txt(
            f"Results of quantification saved in: {quantification_s3_output}",
            f"{results_folder}/output_quantification.txt",
        )

    else:
        print("No segmentation data to copy!")
        utils.save_dict_as_json(
            filename=f"{results_folder}/processing_manifest_no_cell_detection.json",
            dictionary=processing_manifest,
        )

    alert_bot = utils.AlertBot(url=alert_bot_link)
    alert_bot.send_message(
        f"Finished processing dataset: {processing_manifest['name']}"
    )


def handle_clean(
    data_folder: PathLike,
    results_folder: PathLike,
    alert_bot_link: str,
    logger: logging.Logger,
) -> Tuple[str, list, dict]:
    """
    Handles the clean mode: collects segmentation and quantification results,
    moves them to S3, and sends the completion alert.

    Returns
    -------
    Tuple[str, list, dict]
        (dataset_name, investigators, email_message_params)
    """
    logger.info("Starting cleaning...")
    pipeline_config, dataset_name, investigators = get_data_config(
        data_folder=data_folder,
        data_description_path="input_aind_metadata/data_description.json",
        processing_manifest_path="modified_processing_manifest.json",
    )

    pipeline_config["name"] = dataset_name

    clean_up(
        processing_manifest=pipeline_config,
        data_folder=data_folder,
        results_folder=results_folder,
        alert_bot_link=alert_bot_link,
    )

    return dataset_name, investigators or [], {}
