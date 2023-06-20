""" Main script that works as a dispatcher in code ocean """

import json
import logging
import os
import sys
import time
from glob import glob
from pathlib import Path

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


def run():
    """
    Run function that loads a smartspim configuration,
    creates a data asset in code ocean and creates
    new smartspim configurations for each dataset channel
    """

    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    print(f"Load env file status: {load_env_file}")

    processing_manifest_path = os.path.abspath("../data/processing_manifest.json")
    processing_manifest = utils.read_json_as_dict(processing_manifest_path)

    # parameters = sys.argv[1:]
    # sys.argv = [sys.argv[0]]
    # processing_manifest = json.loads(parameters[0])

    logger.info(f"Provided processing manifest: {processing_manifest}")

    results_folder = os.path.abspath("../results")
    data_folder = os.path.abspath("../data")

    codeocean_domain = os.getenv("CODEOCEAN_DOMAIN")
    co_token = os.getenv("CODEOCEAN_TOKEN")
    co_client = CodeOceanClient(domain=codeocean_domain, token=co_token)

    # Getting path in S3
    dataset_to_register = processing_manifest["stitching"]["s3_path"]
    dataset_to_register = dataset_to_register.split("/")[-1]

    data_asset_reg_response = co_client.register_data_asset(
        asset_name=dataset_to_register,
        mount=dataset_to_register,
        bucket="aind-open-data",
        prefix=dataset_to_register,
        tags=["smartspim", "processed"],
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


if __name__ == "__main__":
    run()
