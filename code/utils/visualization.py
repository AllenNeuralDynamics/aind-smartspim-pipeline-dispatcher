"""
Neuroglancer link generation, dynamic range calculation, and
wavelength/orientation conversion utilities.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import dask.array as da
import numpy as np

PathLike = Union[str, Path]


def wavelength_to_hex(wavelength: int) -> int:
    """
    Converts a wavelength (nm) to a hex colour value.
    """
    color_map = {
        460: 0x690AFE,
        470: 0x3F2EFE,
        480: 0x4B90FE,
        490: 0x59D5F8,
        500: 0x5DF8D6,
        520: 0x5AFEB8,
        540: 0x58FEA1,
        560: 0x51FF1E,
        565: 0xBBFB01,
        575: 0xE9EC02,
        580: 0xF5C503,
        590: 0xF58D02,
        600: 0xF55401,
        620: 0xF52E01,
        640: 0xF51701,
        680: 0xF50901,
        750: 0xF50101,
    }

    for upper_bound, hex_color in color_map.items():
        if wavelength <= upper_bound:
            return hex_color

    return 0xFFFFFF


def wavelength_to_hex_alternate(wavelength: int) -> str:
    """
    Converts a fluorescent wavelength to a hex colour string (e.g. "#FF0000").
    Uses a perceptually distinct palette for common fluorescence channels.
    """
    color_map = {
        450: "#3300FF",
        490: "#00AAFF",
        525: "#00FF44",
        561: "#AAFF00",
        594: "#FF8800",
        640: "#FF0000",
        680: "#CC0044",
        750: "#880000",
    }

    for upper_bound, hex_color in sorted(color_map.items()):
        if wavelength <= upper_bound:
            return hex_color

    return "#FFFFFF"


def volume_orientation(acquisition_params: dict) -> List[float]:
    """
    Maps an acquisition orientation string to a neuroglancer cross-section
    orientation quaternion.

    Parameters
    ----------
    acquisition_params: dict
        Dict containing the key "axes" with a list of axis dicts, each with
        a "direction" entry.

    Returns
    -------
    List[float]
        Four-element orientation quaternion for neuroglancer.

    Raises
    ------
    ValueError
        If the orientation string is not recognised.
    """
    acquired = ["", "", ""]

    for axis in acquisition_params["axes"]:
        acquired[axis["dimension"]] = axis["direction"][0]

    acquired = "".join(acquired)

    if acquired in ["SPR", "SPL"]:
        orientation = [0.5, 0.5, 0.5, -0.5]
    elif acquired == "SAL":
        orientation = [0.5, 0.5, -0.5, 0.5]
    elif acquired == "IAR":
        orientation = [0.5, -0.5, 0.5, 0.5]
    elif acquired == "RAS":
        orientation = [np.cos(np.pi / 4), 0.0, 0.0, np.cos(np.pi / 4)]
    elif acquired == "RPI":
        orientation = [np.cos(np.pi / 4), 0.0, 0.0, -np.cos(np.pi / 4)]
    elif acquired == "LAI":
        orientation = [0.0, np.cos(np.pi / 4), -np.cos(np.pi / 4), 0.0]
    else:
        raise ValueError(
            f"Acquisition orientation: {acquired} has unknown NG parameters"
        )

    return orientation


def calculate_dynamic_range(
    fuse_folder: PathLike, extension: str, percentile: int = 99, level: int = 3
) -> dict:
    """
    Calculates the dynamic range for each channel zarr in fuse_folder.

    Returns a dict mapping channel name → [range_max, window_max].
    """
    dynamic_ranges = {}
    for fused_zarr in Path(fuse_folder).glob(extension):
        channels = re.findall(r"Ex_\d+_Em_\d+", str(fused_zarr))
        if not channels:
            # Not a channel image store; skip.
            continue
        channel = channels[0]
        img = da.from_zarr(fused_zarr, str(level)).squeeze()
        range_max = da.percentile(img.flatten(), [percentile]).compute()[0]
        window_max = int(range_max * 1.5)
        dynamic_ranges[channel] = [int(range_max), window_max]
    return dynamic_ranges


def generate_ng_link(
    input_configs: dict,
    s3_path: PathLike,
    base_url: PathLike,
    json_name: str,
    segmentation: bool,
    ccf: bool,
) -> dict:
    """
    Builds a neuroglancer JSON state dict.
    """
    if segmentation:
        ng_path = f"{s3_path}/image_atlas_alignment/{json_name}"
    elif ccf:
        ng_path = f"{s3_path}/image_atlas_alignment/ccf_visualization/{json_name}"
    else:
        ng_path = f"{s3_path}/{json_name}"

    json_state = {
        "ng_link": f"{base_url}#!{ng_path}",
        "title": input_configs["title"],
        "dimensions": input_configs["dimensions"],
        "crossSectionOrientation": input_configs["crossSectionOrientation"],
        "crossSectionScale": input_configs["crossSectionScale"],
        "projectionScale": 16384,
        "layers": input_configs["layers"],
        "gpuMemoryLimit": 1500000000,
        "selectedLayer": {
            "visible": True,
            "layer": input_configs["layers"][0]["name"],
        },
        "layout": "4panel",
    }

    return json_state


def create_neuroglancer_link(
    config: dict,
    s3_channel_paths: List[str],
    s3_dataset_path: str,
    orientation: Union[Dict, List],
    dynamic_ranges: dict,
    segmentation: bool,
    ccf: bool,
    ccf_annotation_s3: Optional[str] = None,
) -> Tuple:
    """
    Creates the neuroglancer link for the processed dataset.

    Parameters
    -------------
    config: dict
        Image configuration necessary to build the neuroglancer link.
    s3_channel_paths: List[str]
        S3 paths for each of the channels.
    s3_dataset_path: str
        S3 path where the dataset is stored.
    orientation: Union[Dict, List]
        Acquisition orientation obtained from processing manifest as a dict or
        manual input as list.
    dynamic_ranges: dict
        Values for setting dynamic range for each channel.
    segmentation: bool
        If you want to have a transformed CCF segmentation layer added in raw space.
    ccf: bool
        If you want to have a CCF segmentation layer added in CCF space.
    ccf_annotation_s3: Optional[str]
        S3 path to the CCF annotation precomputed volume. Used only when ccf=True.
        If None and ccf=True, the CCF layer source will be None.

    Returns
    -------------
    Tuple[Path, str]
        Path where the neuroglancer config json was generated and the link path.
    """
    s3_channel_paths = sorted(s3_channel_paths)

    dimensions = {
        "z": [config["z_res"] * 10**-6, "m"],
        "y": [config["y_res"] * 10**-6, "m"],
        "x": [config["x_res"] * 10**-6, "m"],
        "t": [0.001, "s"],
    }

    projectionOrientation = [
        0.459884375333786,
        0.6998259425163269,
        -0.031935740262269974,
        0.5456465482711792,
    ]

    colors = []
    for channel_str in s3_channel_paths:
        channel_str = re.findall(r"Ex_\d+_Em_\d+", str(channel_str))[0]
        channel = int(channel_str.split("_")[-1])
        hex_code = wavelength_to_hex_alternate(channel)
        hex_str = (
            '#uicontrol vec3 color color(default="'
            + hex_code
            + '")\n#uicontrol invlerp normalized\nvoid main() {\nemitRGB(color * normalized());\n}'
        )
        colors.append(hex_str)

    layers = []
    for idx in range(len(s3_channel_paths)):
        channel_name = re.findall(r"Ex_\d+_Em_\d+", str(s3_channel_paths[idx]))[0]

        layers.append(
            {
                "source": s3_channel_paths[idx],
                "type": "image",
                "channel": 0,
                "name": channel_name + ".zarr",
                "opacity": 1,
                "blend": "additive",
                "tab": "rendering",
                "shader": colors[idx],
                "shaderControls": {
                    "normalized": {
                        "range": [0, dynamic_ranges[channel_name][0]],
                        "window": [0, dynamic_ranges[channel_name][1]],
                    }
                },
            }
        )

    if segmentation:
        layers.append(
            {
                "source": f"precomputed://{s3_dataset_path}/image_atlas_alignment/ccf_annotation_precomputed",
                "type": "segmentation",
                "tab": "source",
                "name": "CCF_parcellation",
            }
        )

    if ccf:
        layers.append(
            {
                "type": "segmentation",
                "source": ccf_annotation_s3,
                "tab": "segments",
                "segments": [],
                "name": "CCF_parcellation",
            }
        )

    crossSectionScale = 15
    projectionScale = 1024

    if isinstance(orientation, dict):
        crossSectionOrientation = volume_orientation(orientation)
    elif isinstance(orientation, list) and ccf:
        crossSectionOrientation = orientation
        crossSectionScale = 1
        projectionScale = 512
    else:
        crossSectionOrientation = [np.cos(np.pi / 4), 0.0, 0.0, np.cos(np.pi / 4)]

    subject_id = Path(s3_dataset_path).name.split("_")[1]

    input_configs = {
        "title": subject_id,
        "dimensions": dimensions,
        "layers": layers,
        "crossSectionOrientation": crossSectionOrientation,
        "crossSectionScale": crossSectionScale,
        "projectionScale": projectionScale,
        "projectionOrientation": projectionOrientation,
        "toolPalettes": {
            "Shader controls": {"row": 2, "query": "type:shaderControl"},
        },
    }

    ng_json_name = "neuroglancer_config.json"

    from utils import utils

    json_state = utils.generate_ng_link(
        input_configs=input_configs,
        s3_path=s3_dataset_path,
        base_url=config["ng_base_url"],
        json_name=ng_json_name,
        segmentation=segmentation,
        ccf=ccf,
    )

    ng_output_path = f"{config['output_folder']}/{ng_json_name}"

    with open(ng_output_path, "w") as outfile:
        json.dump(json_state, outfile, indent=2)

    return Path(ng_output_path), json_state["ng_link"]
