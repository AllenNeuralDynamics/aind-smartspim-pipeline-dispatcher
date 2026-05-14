"""Main script that works as a dispatcher in code ocean"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from utils import utils
from utils.io import get_yaml_config
from utils.notifications import send_email_alerts
from modes.cleanup import handle_clean
from modes.dispatch import handle_dispatch
from modes.postprocess import handle_postprocess_start, handle_postprocess_stop
from modes.split_channels import handle_split_channels

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

SCRIPT_DIR = Path(os.path.abspath(__file__)).parent

PIPELINE_REPOS = [
    ("aind-smartspim-microscope-to-zarr", "File format conversion"),
    ("aind-smartspim-flatfield-estimation", "Image flat-field correction"),
    ("aind-smartspim-destripe", "Image destriping"),
    ("aind-smartspim-stitch", "Image tile alignment"),
    ("aind-smartspim-fuse", "Image tile fusing"),
    ("aind-smartspim-ccf-registration", "Image atlas alignment"),
    ("aind-smartspim-segmentation", "Image cell segmentation"),
    ("aind-smartspim-classification", "Image cell segmentation"),
    ("aind-smartspim-quantification", "Image cell quantification"),
]

MANIFEST_STEP_NAMES = {
    "stitching": {"possible_names": ["stitching"]},
    "registration": {"possible_names": ["registration", "ccf_registration"]},
    "segmentation": {"possible_names": ["segmentation", "cell_segmentation_channels"]},
}


def run():
    """
    Run function allows the smartspim pipeline to execute
    in parallel. It receives an input parameter related to
    the capsule mode:

    - "dispatch": This mode dispatches multiple instances of
    the downstream capsules.

    - "clean": This mode cleans up all the results from the
    downstream capsules because our data is being copied to the
    destination bucket.
    """

    data_folder = Path(os.path.abspath("../data"))
    results_folder = Path(os.path.abspath("../results"))

    mode = str(sys.argv[1:])
    mode = mode.replace("[", "").replace("]", "").casefold()
    sys.argv = [sys.argv[0]]

    # Load .env file if present (env vars already set take precedence)
    load_dotenv()

    alert_bot_link = os.environ["CUSTOM_KEY"]
    alert_configs = get_yaml_config(SCRIPT_DIR.joinpath("utils/alert_configs.yml"))

    # Configurable values — read from environment, fall back to defaults where safe
    output_bucket = os.getenv("OUTPUT_BUCKET")
    ng_base_url = os.getenv("NG_BASE_URL", "https://neuroglancer-demo.appspot.com/")
    ccf_annotation_s3 = os.getenv("CCF_ANNOTATION_S3")
    source_email = os.getenv("SOURCE_EMAIL")
    co_domain = os.getenv("CODEOCEAN_DOMAIN")

    logger.info(f"Alert bot link: {alert_bot_link}")
    logger.info(f"SES alert configs: {alert_configs}")
    logger.info(f"Capsule mode: {mode}")

    required_input_elements = [
        f"{data_folder}/processing_manifest.json",
        f"{data_folder}/input_aind_metadata/data_description.json",
    ]

    if "clean" in mode:
        required_input_elements = [
            f"{data_folder}/modified_processing_manifest.json",
            f"{data_folder}/input_aind_metadata/data_description.json",
        ]

    if "postprocess-start" in mode:
        required_input_elements = [
            f"{data_folder}/raw_data",
            f"{data_folder}/stitched_data",
        ]

    if "postprocess-stop" in mode:
        required_input_elements = [
            f"{data_folder}/registration",
            f"{data_folder}/classification",
            f"{data_folder}/quantification",
            f"{data_folder}/postprocess_dispatch",
            f"{data_folder}/stitched_data",
        ]

    missing_files = utils.validate_capsule_inputs(required_input_elements)

    if len(missing_files):
        raise ValueError(
            f"We miss the following files in the capsule input: {missing_files}"
        )

    logger.info(f"Data in data folder: {os.listdir(data_folder)}")

    acquisition_json = utils.read_json_as_dict(
        data_folder.joinpath("input_aind_metadata/acquisition.json")
    )

    if not len(acquisition_json):
        raise FileNotFoundError("Please, provide an acquisition.json")

    axes_resolution_xyz = utils.get_resolution(acquisition_config=acquisition_json)

    dataset_name = ""
    investigators = []
    email_message_params = {}

    if "split_channels" in mode:
        dataset_name, investigators, email_message_params = handle_split_channels(
            data_folder=data_folder,
            results_folder=results_folder,
            output_bucket=output_bucket,
            logger=logger,
        )

    elif "dispatch" in mode:
        dataset_name, investigators, email_message_params = handle_dispatch(
            data_folder=data_folder,
            results_folder=results_folder,
            output_bucket=output_bucket,
            ng_base_url=ng_base_url,
            ccf_annotation_s3=ccf_annotation_s3,
            co_domain=co_domain,
            axes_resolution_xyz=axes_resolution_xyz,
            logger=logger,
        )

    elif "clean" in mode:
        dataset_name, investigators, email_message_params = handle_clean(
            data_folder=data_folder,
            results_folder=results_folder,
            alert_bot_link=alert_bot_link,
            logger=logger,
        )

    elif "postprocess-start" in mode:
        dataset_name, investigators, email_message_params = handle_postprocess_start(
            data_folder=data_folder,
            results_folder=results_folder,
            pipeline_repos=PIPELINE_REPOS,
            manifest_step_names=MANIFEST_STEP_NAMES,
            logger=logger,
        )

    elif "postprocess-stop" in mode:
        dataset_name, investigators, email_message_params = handle_postprocess_stop(
            data_folder=data_folder,
            results_folder=results_folder,
            output_bucket=output_bucket,
            ng_base_url=ng_base_url,
            ccf_annotation_s3=ccf_annotation_s3,
            axes_resolution_xyz=axes_resolution_xyz,
            logger=logger,
        )

    else:
        raise NotImplementedError(f"The mode {mode} has not been implemented")

    if investigators:
        send_email_alerts(
            mode=mode,
            alert_configs=alert_configs,
            investigators=investigators,
            dataset_name=dataset_name,
            logger=logger,
            email_message_params=email_message_params,
            source_email=source_email,
        )


if __name__ == "__main__":
    run()
