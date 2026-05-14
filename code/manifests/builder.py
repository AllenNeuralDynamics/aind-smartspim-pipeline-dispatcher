"""
Manifest creation, reading, and conversion helpers.
"""

from pathlib import Path
from typing import Dict, List, Tuple, Union

from utils import utils
from utils.io import read_json_as_dict

PathLike = Union[str, Path]


def get_data_config(
    data_folder: PathLike,
    processing_manifest_path: str = "processing_manifest.json",
    data_description_path: str = "data_description.json",
) -> Tuple:
    """
    Returns the first smartspim dataset found
    in the data folder

    Parameters
    -----------
    data_folder: str
        Path to the folder that contains the data

    processing_manifest_path: str
        Path for the processing manifest

    data_description_path: str
        Path for the data description

    Returns
    -----------
    Tuple[Dict, str, list]
        Dict: Empty dictionary if the path does not exist,
        dictionary with the data otherwise.

        Str: Empty string if the processing manifest
        was not found

        List: Empty list if no investigators in data description
    """
    processing_manifest_path = Path(f"{data_folder}/{processing_manifest_path}")
    data_description_path = Path(f"{data_folder}/{data_description_path}")

    if not processing_manifest_path.exists():
        raise ValueError(
            f"Please, check processing manifest path: {processing_manifest_path}"
        )

    if not data_description_path.exists():
        raise ValueError(
            f"Please, check data description path: {data_description_path}"
        )

    derivatives_dict = read_json_as_dict(str(processing_manifest_path))
    data_description_dict = read_json_as_dict(str(data_description_path))

    smartspim_dataset = data_description_dict.get("name")
    investigators = data_description_dict.get("investigators")

    return derivatives_dict, smartspim_dataset, investigators


def get_processing_manifest_path(raw_data_folder: PathLike) -> Path:
    """
    Locates processing_manifest.json under raw_data_folder.

    Checks two locations used by different SmartSPIM folder-structure versions:
      - <raw_data_folder>/derivatives/processing_manifest.json
      - <raw_data_folder>/SPIM/derivatives/processing_manifest.json

    Returns the first match found, or None if neither exists.

    Raises
    ------
    FileNotFoundError
        If raw_data_folder itself does not exist.
    """
    raw_data_folder = Path(raw_data_folder)

    if not raw_data_folder.exists():
        raise FileNotFoundError(f"Raw data folder does not exist: {raw_data_folder}")

    processing_manifest_path = None

    for path in [
        raw_data_folder.joinpath("derivatives"),
        raw_data_folder.joinpath("SPIM/derivatives"),
    ]:
        curr_proc_man = path.joinpath("processing_manifest.json")
        if curr_proc_man.exists():
            processing_manifest_path = curr_proc_man

    return processing_manifest_path


def get_standard_manifest_config(
    pipeline_processing: Dict, hashmap_stepnames: Dict
) -> Dict:
    """
    Converts a legacy processing manifest format to the current standard.

    Parameters
    ----------
    pipeline_processing:
        Raw pipeline_processing dict (possibly old schema).
    hashmap_stepnames:
        Canonical step-name mapping (e.g. MANIFEST_STEP_NAMES in run_capsule.py).

    Returns
    -------
    Dict
        Standardised pipeline_processing dict.

    Raises
    ------
    ValueError
        If pipeline_processing is empty.
    """
    if not len(pipeline_processing):
        raise ValueError("Please, provide a valid processing manifest.")

    standard_pipeline_processing = {}
    for step_name, values in hashmap_stepnames.items():
        standard_pipeline_processing[step_name] = {}

        for possible_name in values["possible_names"]:
            if possible_name in pipeline_processing:
                config = {}
                if "cell_segmentation_channels" == possible_name:
                    config = {
                        "channels": pipeline_processing[possible_name],
                        "input_scale": "0",
                        "chunksize": "128",
                        "signal_start": "0",
                        "signal_end": "-1",
                    }
                elif "ccf_registration" == possible_name:
                    config = {
                        "channels": pipeline_processing[possible_name],
                        "input_scale": 3,
                    }
                else:
                    config = pipeline_processing[possible_name]

                standard_pipeline_processing[step_name] = config
                break

    return standard_pipeline_processing


def get_omezarr_path(stitched_path: PathLike):
    """
    Finds the folder containing OMEZarr data within stitched_path.

    Checks "processed" and "image_tile_fusing" sub-directories.

    Returns
    -------
    Path or None
        The folder containing OMEZarr/, or None if not found.

    Raises
    ------
    FileNotFoundError
        If stitched_path does not exist.
    """
    stitched_path = Path(stitched_path)

    if not stitched_path.exists():
        raise FileNotFoundError(f"Path {stitched_path} does not exist!")

    possible_omezarr_folders = ["processed", "image_tile_fusing"]

    for pof in possible_omezarr_folders:
        curr_folder = stitched_path.joinpath(pof)
        if curr_folder.joinpath("OMEZarr").exists():
            return curr_folder

    return None


def create_segmentation_manifests(
    processing_manifest: dict, results_folder: PathLike, prefix: str
) -> None:
    """
    Writes per-channel segmentation processing manifest JSON files to results_folder.

    Parameters
    ----------
    processing_manifest:
        Full processing manifest dict (with "pipeline_processing" key).
    results_folder:
        Destination directory for output JSON files.
    prefix:
        Filename prefix, e.g. "segmentation" or "classification".
    """
    pipeline_config = processing_manifest["pipeline_processing"]
    segment_channels = pipeline_config["segmentation"]["channels"]

    if len(segment_channels):
        print(
            f"Preparing segmentation configs for {segment_channels} with prefix: {prefix}"
        )
        background_channel = processing_manifest["pipeline_processing"]["registration"][
            "channels"
        ]

        background_channel = background_channel[0] if len(background_channel) else None

        for channel_to_segment in segment_channels:
            copy_pipeline_config = pipeline_config.copy()

            copy_pipeline_config["segmentation"]["input_data"] = "../data/fused"
            copy_pipeline_config["segmentation"]["channel"] = channel_to_segment

            if background_channel is None:
                copy_pipeline_config["segmentation"][
                    "background_channel"
                ] = channel_to_segment
            else:
                copy_pipeline_config["segmentation"][
                    "background_channel"
                ] = background_channel

            copy_pipeline_config["quantification"] = {}
            copy_pipeline_config["quantification"]["fused_folder"] = "../data/fused"
            copy_pipeline_config["quantification"]["channel"] = channel_to_segment
            copy_pipeline_config["quantification"]["save_path"] = "../results/"

            print(copy_pipeline_config, channel_to_segment)

            utils.save_dict_as_json(
                f"{results_folder}/{prefix}_processing_manifest_{channel_to_segment}.json",
                copy_pipeline_config,
            )

    else:
        utils.save_dict_as_json(
            f"{results_folder}/{prefix}_processing_manifest_empty.json",
            pipeline_config.copy(),
        )
