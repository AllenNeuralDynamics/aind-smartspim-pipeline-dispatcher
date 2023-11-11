""" Main script that works as a dispatcher in code ocean """

import logging
import os
import re
import sys
import time
from glob import glob
from pathlib import Path
from typing import List, Tuple, Union

from aind_codeocean_api.codeocean import CodeOceanClient
from ng_link import NgState
from utils import utils

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler("test.log", "a"),
    ],
)
logging.disable("DEBUG")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PathLike = Union[str, Path]


def wait_for_data_availability(
    co_client,
    data_asset_id: str,
    timeout_seconds: int = 300,
    pause_interval=10,
):
    """
    There is a lag between when a register data request is made and when the
    data is available to be used in a capsule.
    Parameters
    ----------
    data_asset_id : str
    timeout_seconds : int
        Roughly how long the method should check if the data is available.
    pause_interval : int
        How many seconds between when the backend is queried.

    Returns
    -------
    requests.Response

    """
    num_of_checks = 0
    break_flag = False
    time.sleep(pause_interval)
    response = co_client.get_data_asset(data_asset_id)

    if ((pause_interval * num_of_checks) > timeout_seconds) or (
        response.status_code == 200
    ):
        break_flag = True
    while not break_flag:
        time.sleep(pause_interval)
        response = co_client.get_data_asset(data_asset_id)
        num_of_checks += 1
        if ((pause_interval * num_of_checks) > timeout_seconds) or (
            response.status_code == 200
        ):
            break_flag = True
    return response


def make_data_viewable(co_client: CodeOceanClient, response_contents: dict):
    """
    Makes a registered dataset viewable

    Parameters
    ----------
    co_client: CodeOceanClient
        Code ocean client

    response_contents: dict
        Dictionary with the response
        of the created data asset

    """
    data_asset_id = response_contents["id"]
    response_data_available = wait_for_data_availability(co_client, data_asset_id)

    if response_data_available.status_code != 200:
        logger.info(f"Unable to find: {data_asset_id}")
        return

    # Make data asset viewable to everyone
    update_data_perm_response = co_client.update_permissions(
        data_asset_id=data_asset_id, everyone="viewer"
    )
    logger.info(f"Data asset viewable to everyone: {update_data_perm_response}")


def dispatch(processing_manifest: dict, results_folder: PathLike, bucket: str):
    """
    Creates multiple processing manifest jsons using
    the original processing manifest. This is done to
    use the flatten connection and instantiate multiple
    computations to process each channel in parallel.

    Parameters
    ----------
    processing_manifest: dict
        Dictionary with the processing manifest
        metadata

    results_folder: str
        Path pointing to the results folder

    bucket: str
        Bucket name where the data is stored
    """

    logger.info(f"Provided processing manifest: {processing_manifest}")

    codeocean_domain = os.getenv("API_KEY")
    co_token = os.getenv("API_SECRET")
    co_client = CodeOceanClient(domain=codeocean_domain, token=co_token)

    # Getting path in S3
    dataset_to_register = processing_manifest["stitching"]["s3_path"]
    dataset_to_register = dataset_to_register.split("/")[-1]

    smartspim_fused_tags = ["smartspim", "processed"]

    # Register the fused smartspim dataset
    data_asset_reg_response = co_client.register_data_asset(
        asset_name=dataset_to_register,
        mount=dataset_to_register,
        bucket=bucket,
        prefix=dataset_to_register,
        tags=smartspim_fused_tags,
    )

    response_contents = data_asset_reg_response.json()
    logger.info(f"Created data asset in Code Ocean: {response_contents}")

    # Making the created data asset available for everyone
    make_data_viewable(co_client, response_contents)

    # Creating processing manifests for channels to register
    register_channels = processing_manifest["registration"]["channels"]

    for channel_to_register in register_channels:
        copy_processing_manifest = processing_manifest.copy()

        copy_processing_manifest["registration"]["input_data"] = "../data/fused"
        copy_processing_manifest["registration"]["channel"] = channel_to_register

        utils.save_dict_as_json(
            f"{results_folder}/registration_processing_manifest_{channel_to_register}.json",
            copy_processing_manifest,
        )

    # Creating processing manifests for channels to segment and quantify
    segment_channels = processing_manifest["segmentation"]["channels"]

    for channel_to_segment in segment_channels:
        copy_processing_manifest = processing_manifest.copy()

        copy_processing_manifest["segmentation"]["input_data"] = "../data/fused"
        copy_processing_manifest["segmentation"]["channel"] = channel_to_segment

        # Creating quantification parameters
        copy_processing_manifest["quantification"] = {}
        copy_processing_manifest["quantification"]["fused_folder"] = "../data/fused"
        copy_processing_manifest["quantification"]["channel"] = channel_to_segment
        copy_processing_manifest["quantification"]["save_path"] = "../results/"

        utils.save_dict_as_json(
            f"{results_folder}/segmentation_processing_manifest_{channel_to_segment}.json",
            copy_processing_manifest,
        )


def clean_up(
    processing_manifest: dict,
    data_folder: PathLike,
    results_folder: PathLike,
):
    """
    Moves all the data to the aind-open-data bucket in
    AWS.

    Parameters
    ----------
    processing_manifest: dict
        Dictionary with the processing manifest
        metadata

    data_folder: str
        Path pointing to the data folder

    results_folder: str
        Path pointing to the results folder

    bucket: str
        Bucket name
    """
    logger.info(f"Data folder: {os.listdir(data_folder)}")

    # # Variables from processing manifest
    # bucket = "aind-open-data"

    ccf_folders = glob(f"{data_folder}/ccf_*")
    cell_folders = glob(f"{data_folder}/cell_*")
    quantification_folders = glob(f"{data_folder}/quant_*")

    logger.info(f"CCF folders: {ccf_folders}")
    logger.info(f"Cell folders: {cell_folders}")
    logger.info(f"Quantification folders: {quantification_folders}")

    # Defining s3 outputs
    s3_path = processing_manifest["stitching"]["s3_path"]
    ccf_s3_output = f"{s3_path}/image_atlas_alignment"
    cell_s3_output = f"{s3_path}/image_cell_segmentation"
    quantification_s3_output = f"{s3_path}/image_cell_quantification"

    regex_channels = r"Ex_(\d{3})_Em_(\d{3})$"

    # Moving data to the CCF folder
    for ccf_folder in ccf_folders:
        channel_name = re.search(regex_channels, ccf_folder).group()

        for out in utils.execute_command_helper(
            f"aws s3 mv --recursive {ccf_folder} {ccf_s3_output}/{channel_name}"
        ):
            print(out)

    # Moving data to the cell folder
    for cell_folder in cell_folders:
        channel_name = re.search(regex_channels, cell_folder).group()

        for out in utils.execute_command_helper(
            f"aws s3 mv --recursive {cell_folder} {cell_s3_output}/{channel_name}"
        ):
            print(out)

    # Moving data to the quantification folder
    for quantification_folder in quantification_folders:
        channel_name = re.search(regex_channels, quantification_folder).group()

        for out in utils.execute_command_helper(
            f"aws s3 mv --recursive {quantification_folder} {quantification_s3_output}/{channel_name}"
        ):
            print(out)

    utils.save_string_to_txt(
        f"Results of CCF saved in: {ccf_s3_output}",
        f"{results_folder}/output_ccf.txt",
    )

    utils.save_string_to_txt(
        f"Results of cell segmentation saved in: {cell_s3_output}",
        f"{results_folder}/output_cell.txt",
    )

    utils.save_string_to_txt(
        f"Results of quantification saved in: {quantification_s3_output}",
        f"{results_folder}/output_quantification.txt",
    )


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
    Tuple[Dict, str]
        Dict: Empty dictionary if the path does not exist,
        dictionary with the data otherwise.

        Str: Empty string if the processing manifest
        was not found
    """

    # Returning first smartspim dataset found
    # Doing this because of Code Ocean, ideally we would have
    # a single dataset in the pipeline

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

    derivatives_dict = utils.read_json_as_dict(str(processing_manifest_path))
    data_description_dict = utils.read_json_as_dict(str(data_description_path))

    smartspim_dataset = data_description_dict["name"]

    return derivatives_dict, smartspim_dataset


def copy_intermediate_data(
    output_dispatch_metadata: PathLike,
    destripe_files: PathLike,
    stitch_folders: PathLike,
    fuse_folders: PathLike,
    new_dataset_name: str,
    bucket_path: str,
    results_folder: PathLike,
    logger: logging.Logger,
) -> str:
    """
    Copies the destripe, stitch and fusion metadata
    to the destination bucket to make it available
    to scientists as soon as possible.

    Parameters
    ----------
    output_dispatch_metadata: PathLike
        Path where the new metadata (derived)
        for the processed dataset is located

    destripe_files: PathLike
        Metadata files generated in the
        parallel destriping step

    stitch_folders: PathLike
        Stitch folders generated in the
        stitch step.

    fuse_folders: PathLike
        Fuse folders generated in the
        parallel fusion step.

    new_dataset_name: str
        New dataset name where the data will
        be copied following the aind conventions
        e.g., s3://{bucket_path}/{new_dataset_name}

    bucket_path: str
        S3 path where the data will be moved.
        Do not include 's3://' since this is
        automatically added.

    results_folder: PathLike
        Results folder path in Code Ocean

    logger: logging.Logger
        Logging object

    Returns
    -------
    str
        Path where the data was moved.
        e.g., s3://{bucket_path}/{new_dataset_name}
        It includes the "s3://" prefix.
    """

    stitch_processings = []
    fuse_processings = []

    for stitch_folder in stitch_folders:
        processing_jsons = [
            p
            for p in glob(f"{stitch_folder}/metadata/*processing*.json")
            if "manifest" not in str(p)
        ]
        stitch_processings.append(processing_jsons)

    for fuse_folder in fuse_folders:
        processing_jsons = [
            p
            for p in glob(f"{fuse_folder}/metadata/*processing*.json")
            if "manifest" not in str(p)
        ]
        fuse_processings.append(processing_jsons)

    # Flattening list
    processing_paths = list()
    for sub_list in stitch_processings + fuse_processings:
        processing_paths += sub_list

    processing_paths = destripe_files + processing_paths
    logger.info(f"Processing paths: {processing_paths}")

    output_filename = utils.compile_processing_jsons(
        processing_paths=processing_paths,
        output_general_processing=output_dispatch_metadata,
        processor_full_name="Camilo Laiton",
        pipeline_version="1.5.0",
    )

    logger.info(f"Compiled processing.json in path {output_filename}")

    s3_path = f"s3://{bucket_path}/{new_dataset_name}"

    # Copying derived metadata
    output_dispatch_metadata = Path(output_dispatch_metadata)
    for out in utils.execute_command_helper(
        f"aws s3 cp --recursive {s3_path} {output_dispatch_metadata}"
    ):
        logger.info(out)

    # Copying out fused data
    output_fusion = "image_tile_fusing"
    dest_zarr_path = f"{s3_path}/{output_fusion}/OMEZarr"
    dest_metadata_path = f"{s3_path}/{output_fusion}/metadata"

    for fuse_folder in fuse_folders:
        logger.info(f"Copying data from {fuse_folder} to {s3_path}/{output_fusion}")
        fuse_folder = Path(fuse_folder)
        source_zarr = fuse_folder.joinpath("OMEZarr")
        source_metadata = fuse_folder.joinpath("metadata")

        if source_zarr.exists():
            for out in utils.execute_command_helper(
                f"aws s3 cp --recursive {source_zarr} {dest_zarr_path}"
            ):
                logger.info(out)

        else:
            raise ValueError(f"Folder {source_zarr} does not exist!")

        if source_metadata.exists():
            for out in utils.execute_command_helper(
                f"aws s3 cp --recursive {source_metadata} {dest_metadata_path}/{fuse_folder.name}"
            ):
                logger.info(out)

        else:
            raise ValueError(f"Folder {source_metadata} does not exist!")

    # Copying stitch metadata
    for stitch_folder in stitch_folders:
        logger.info(f"Copying data from {stitch_folder} to {dest_metadata_path}")
        stitch_folder = Path(stitch_folder)
        source_metadata = stitch_folder.joinpath("metadata")

        if source_metadata.exists():
            for out in utils.execute_command_helper(
                f"aws s3 cp --recursive {source_metadata} {dest_metadata_path}/{stitch_folder.name}"
            ):
                logger.info(out)

        else:
            raise ValueError(f"Folder {source_metadata} does not exist!")

    utils.save_string_to_txt(
        f"Stitched dataset saved in: {s3_path}",
        f"{results_folder}/output_stitching.txt",
    )

    return s3_path, dest_zarr_path


def create_derived_stitched_metadata(
    data_folder: PathLike, results_folder: PathLike, logger: logging.Logger
) -> Tuple[PathLike, str]:
    """
    Creates the derived metadata following
    AIND conventions.

    Parameters
    ----------
    data_folder: PathLike
        Path to the code ocean data folder

    results_folder: PathLike
        Path to the code ocean results folder

    logger: logging.Logger
        Logging object

    Returns
    -------
    Tuple[PathLike, str]
        The first position of the tuple
        corresponds to the path where the
        metadata was created while the
        second position has the new name
        of the dataset
    """
    logger.info("Generating derived data description")
    raw_metadata_path = data_folder.joinpath("input_aind_metadata")
    output_dispatch_metadata = f"{results_folder}/output_aind_metadata"
    utils.create_folder(output_dispatch_metadata)

    new_dataset_name = utils.generate_data_description(
        raw_data_description_path=raw_metadata_path.joinpath("data_description.json"),
        dest_data_description=output_dispatch_metadata,
        process_name="stitched",
    )

    logger.info("Copying all available raw SmartSPIM metadata")

    # This is the AIND metadata
    found_metadata = utils.copy_available_metadata(
        input_path=raw_metadata_path,
        output_path=output_dispatch_metadata,
        ignore_files=[
            "data_description.json",  # Ignoring data description since we're generating it above
            "processing.json",  # This is generated with all the steps
        ],
    )

    logger.info(f"Copied metadata from {data_folder}: {found_metadata}")
    logger.info(f"Metadata in folder: {os.listdir(output_dispatch_metadata)}")

    return output_dispatch_metadata, new_dataset_name


def create_ng_link(self, config: dict, s3_channel_paths: List[str]) -> str:
    """
    Creates the neuroglancer link for the processed dataset

    Parameters
    -------------

    config: dict
        Image configuration necessary to build the
        neuroglancer link

    s3_channel_paths: List[str]
        S3 paths for each of the channels

    Returns
    -------------
    str:
        Path where the neuroglancer config json
        was generated
    """

    dimensions = {
        "z": {
            "voxel_size": config["z_res"],
            "unit": "microns",
        },
        "y": {
            "voxel_size": config["y_res"],
            "unit": "microns",
        },
        "x": {
            "voxel_size": config["x_res"],
            "unit": "microns",
        },
        "t": {"voxel_size": 0.001, "unit": "seconds"},
    }

    colors = []
    for channel_str in s3_channel_paths:
        channel_str = Path(channel_str).name
        channel: int = int(channel_str.split("_")[-1])
        hex_val: int = utils.wavelength_to_hex(channel)
        hex_str = f"#{str(hex(hex_val))[2:]}"

        colors.append(hex_str)

    # Creating layer per channel
    layers = []
    for idx in range(len(s3_channel_paths)):
        channel_name = Path(s3_channel_paths[idx]).name

        layers.append(
            {
                "source": s3_channel_paths[idx],
                "type": "image",
                # use channel idx when source is the same
                # in zarr to change channel otherwise 0
                "channel": 0,
                "name": channel_name,
                "shader": {
                    "color": colors[idx],
                    "emitter": "RGB",
                    "vec": "vec3",
                },
                "shaderControls": {"normalized": {"range": [0, 200]}},  # Optional
            }
        )

    neuroglancer_link = NgState(
        input_config={"dimensions": dimensions, "layers": layers},
        mount_service="s3",
        bucket_path=config["bucket_path"],
        output_json=config["output_folder"],
        base_url=config["ng_base_url"],
        json_name="neuroglancer_config.json",
    )

    neuroglancer_link.save_state_as_json()

    return f"{config['output_folder']}/neuroglancer_config.json"


def run():
    """
    Run function allows the smartspim pipeline to execute
    in parallel. It receives an input parameter related to
    the capsule mode:

    - "dispatch": This mode dispatches multiple instances of
    the downstream capsules.

    - "clean": This mode cleans up all the results from the
    downstream capsules because our data is being copied to the
    aind-open-data bucket.
    """

    # Absolute paths of common Code Ocean folders
    data_folder = Path(os.path.abspath("../data"))
    results_folder = Path(os.path.abspath("../results"))

    mode = str(sys.argv[1:])
    mode = mode.replace("[", "").replace("]", "").casefold()
    sys.argv = [sys.argv[0]]

    # It is assumed that these files
    # will be in the data folder
    required_input_elements = [
        f"{data_folder}/processing_manifest.json",
        f"{data_folder}/input_aind_metadata/data_description.json",
    ]

    missing_files = utils.validate_capsule_inputs(required_input_elements)

    if len(missing_files):
        raise ValueError(
            f"We miss the following files in the capsule input: {missing_files}"
        )

    pipeline_config, dataset_name = get_data_config(
        data_folder=data_folder,
        data_description_path="input_aind_metadata/data_description.json",
    )

    # Loading .env file, this file must be placed with
    # the code ocean domain and token
    # dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    # load_env_file = load_dotenv(dotenv_path=dotenv_path)
    # logger.info(f"Load env file status: {load_env_file}")

    if "dispatch" in mode:
        # Creating new metadata for stitched dataset
        output_dispatch_metadata, new_dataset_name = create_derived_stitched_metadata(
            data_folder=data_folder, results_folder=results_folder, logger=logger
        )

        # Looking for files
        destripe_files = glob(f"{data_folder}/image_destriping_*")
        stitch_folders = glob(f"{data_folder}/stitched/stitch_*")
        fuse_folders = glob(f"{data_folder}/fused/fusion_*")

        bucket_path = (
            "aind-msma-morphology-data/test_data/SmartSPIM"  # "aind-open-data"
        )

        s3_path, s3_dest_zarr = copy_intermediate_data(
            output_dispatch_metadata=output_dispatch_metadata,
            destripe_files=destripe_files,
            stitch_folders=stitch_folders,
            fuse_folders=fuse_folders,
            new_dataset_name=new_dataset_name,
            bucket_path=bucket_path,
            results_folder=results_folder,
            logger=logger,
        )

        # Getting S3 paths for channels
        s3_paths_for_channels = []
        for fuse_folder in fuse_folders:
            channel_name = f"{Path(fuse_folder).name}".replace("fusion_", "")
            # f"{s3_path}/{output_fusion}/OMEZarr"
            s3_paths_for_channels.append(f"{s3_dest_zarr}/{channel_name}.zarr")

        axes_resolution = pipeline_config["pipeline_processing"]["stitching"][
            "resolution"
        ]
        output_json = create_ng_link(
            config={
                "bucket_path": bucket_path,
                "output_folder": results_folder,
                "ng_base_url": "https://aind-neuroglancer-sauujisjxq-uw.a.run.app",
                "z_res": axes_resolution[2]["resolution"],
                "y_res": axes_resolution[1]["resolution"],
                "x_res": axes_resolution[0]["resolution"],
            },
            s3_channel_paths=s3_paths_for_channels,
        )

        # Copying neuroglancer config out
        for out in utils.execute_command_helper(
            f"aws s3 cp --recursive {output_json} {s3_path}"
        ):
            logger.info(out)

        # Setting the stitching path in pipeline config
        pipeline_config["pipeline_processing"]["stitching"]["s3_path"] = s3_path

        dispatch(
            processing_manifest=pipeline_config,
            results_folder=results_folder,
            bucket=bucket_path,
        )

        utils.save_dict_as_json(
            f"{results_folder}/modified_processing_manifest.json",
            pipeline_config,
        )

    elif "clean" in mode:
        clean_up(
            processing_manifest=pipeline_config,
            data_folder=data_folder,
            results_folder=results_folder,
        )

    else:
        raise NotImplementedError(f"The mode {mode} has not been implemented")


if __name__ == "__main__":
    run()
