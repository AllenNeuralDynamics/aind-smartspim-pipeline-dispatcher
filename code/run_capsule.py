""" Main script that works as a dispatcher in code ocean """

import json
import logging
import os
import sys
import time
from glob import glob
from pathlib import Path
from typing import Union

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.credentials import CodeOceanCredentials
from dotenv import load_dotenv
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


def dispatch(processing_manifest_path: PathLike):
    """
    Creates multiple processing manifest jsons using
    the original processing manifest. This is done to
    use the flatten connection and instantiate multiple
    computations to process each channel in parallel.

    Parameters
    ----------
    processing_manifest_path: PathLike
        Path where the processing manifest json is
        located.
    """

    processing_manifest = utils.read_json_as_dict(processing_manifest_path)

    logger.info(f"Provided processing manifest: {processing_manifest}")

    results_folder = os.path.abspath("../results")
    data_folder = os.path.abspath("../data")

    codeocean_domain = os.getenv("CODEOCEAN_DOMAIN")
    co_token = os.getenv("CODEOCEAN_TOKEN")
    co_client = CodeOceanClient(domain=codeocean_domain, token=co_token)

    # Getting path in S3
    dataset_to_register = processing_manifest["stitching"]["s3_path"]
    dataset_to_register = dataset_to_register.split("/")[-1]

    smartspim_fused_tags = ["smartspim", "processed"]

    # Register the fused smartspim dataset
    data_asset_reg_response = co_client.register_data_asset(
        asset_name=dataset_to_register,
        mount=dataset_to_register,
        bucket="aind-open-data",
        prefix=dataset_to_register,
        tags=smartspim_fused_tags,
    )

    response_contents = data_asset_reg_response.json()
    logger.info(f"Created data asset in Code Ocean: {response_contents}")

    # Making the created data asset available for everyone
    make_data_viewable(co_client, response_contents)

    # Getting channel name
    channels_to_process = processing_manifest["segmentation"]["channels"]

    for channel_to_process in channels_to_process:
        copy_processing_manifest = processing_manifest.copy()

        copy_processing_manifest["registration"]["input_data"] = "../data/fused"
        copy_processing_manifest["registration"]["channel"] = channel_to_process

        copy_processing_manifest["segmentation"]["input_data"] = "../data/fused"
        copy_processing_manifest["segmentation"]["channel"] = channel_to_process

        # Creating quantification parameters
        copy_processing_manifest["quantification"] = {}
        copy_processing_manifest["quantification"]["fused_folder"] = "../data/fused"
        copy_processing_manifest["quantification"]["channel"] = channel_to_process
        copy_processing_manifest["quantification"]["save_path"] = "../results/"

        utils.save_dict_as_json(
            f"{results_folder}/processing_manifest_{channel_to_process}.json",
            copy_processing_manifest,
        )


def clean_up(processing_manifest_path: PathLike):
    """
    Moves all the data to the aind-open-data bucket in
    AWS.

    Parameters
    ----------
    processing_manifest_path: PathLike
        Path where the processing manifest json is
        located.
    """

    if not os.path.exists(processing_manifest_path):
        raise ValueError("Processing manifest path does not exist!")

    pipeline_config = utils.read_json_as_dict(processing_manifest_path)

    # Defining paths
    data_folder = os.path.abspath("../data")
    results_folder = os.path.abspath("../results")

    logger.info(f"Data folder: {os.listdir(data_folder)}")

    # Variables from processing manifest
    bucket = "aind-open-data"

    ccf_folders = glob(f"{data_folder}/ccf_*")
    cell_folders = glob(f"{data_folder}/cell_*")
    quantification_folders = glob(f"{data_folder}/quant_*")

    logger.info(f"CCF folders: {ccf_folders}")
    logger.info(f"Cell folders: {cell_folders}")
    logger.info(f"Quantification folders: {quantification_folders}")

    # Defining s3 outputs
    s3_path = pipeline_config["stitching"]["s3_path"]
    ccf_s3_output = f"{s3_path}/processed/CCF_Atlas_Registration"
    cell_s3_output = f"{s3_path}/processed/Cell_Segmentation"
    quantification_s3_output = f"{s3_path}/processed/Quantification"

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

    mode = str(sys.argv[1:]).casefold()
    sys.argv = [sys.argv[0]]

    processing_manifest_path = os.path.abspath("../data/processing_manifest.json")

    # Loading .env file, this file must be placed with
    # the code ocean domain and token
    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    logger.info(f"Load env file status: {load_env_file}")

    if mode == "dispatch":
        dispatch(processing_manifest_path)

    elif mode == "clean":
        clean_up(processing_manifest_path)

    else:
        raise NotImplementedError(f"The mode {mode} has not been implemented")


if __name__ == "__main__":
    run()
