"""Main script that works as a dispatcher in code ocean"""

import argparse
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from log_schema import setup_logging

from __init__ import __pipeline_name__, __title__, __version__
from utils import utils
from utils.io import get_yaml_config
from utils.notifications import send_email_alerts
from modes.cleanup import handle_clean
from modes.dispatch import handle_dispatch
from modes.postprocess import handle_postprocess_start, handle_postprocess_stop
from modes.split_channels import handle_split_channels

logger = logging.getLogger(__name__)

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


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="run_capsule.py",
        description="SmartSPIM pipeline dispatcher.",
    )
    ap.add_argument(
        "mode",
        help="Pipeline stage: dispatch|clean|split_channels|postprocess-start|postprocess-stop",
    )
    ap.add_argument(
        "cloud_mode_pos",
        nargs="?",
        default=None,
        metavar="CLOUD",
        help="true|false (positional; Nextflow compat). Overridden by --cloud-mode.",
    )
    ap.add_argument(
        "output_path_pos",
        nargs="?",
        default=None,
        metavar="OUTPUT_PATH",
        help="S3 bucket or local path (positional; Nextflow compat). Overridden by --output-path.",
    )
    ap.add_argument("--cloud-mode",        default=None, help="Overrides CLOUD_MODE env var")
    ap.add_argument("--output-path",       default=None, help="Overrides OUTPUT_BUCKET / OUTPUT_PATH env vars")
    ap.add_argument("--data-folder",       default=None, help="Overrides DATA_FOLDER env var")
    ap.add_argument("--results-folder",    default=None, help="Overrides RESULTS_FOLDER env var")
    ap.add_argument("--ng-base-url",       default=None, help="Overrides NG_BASE_URL env var")
    ap.add_argument("--ccf-annotation-s3", default=None, help="Overrides CCF_ANNOTATION_S3 env var")
    ap.add_argument("--co-domain",         default=None, help="Overrides CODEOCEAN_DOMAIN env var")
    return ap.parse_args()


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

    Set CLOUD_MODE=false to run on SLURM or any environment without AWS access.
    Set DATA_FOLDER and RESULTS_FOLDER to override the Code Ocean path defaults.
    """

    args = _parse_args()
    mode = args.mode.casefold()

    # Load .env file if present (env vars already set take precedence)
    load_dotenv(SCRIPT_DIR / ".env")

    process_name = f"{__title__}-{mode}"

    setup_logging(model={
        "pipeline_name": __pipeline_name__,
        "process_name": process_name,
        "software_name": __title__,
        "software_version": __version__,
    })

    start_time = time.monotonic()

    # ── Execution mode: named flag > positional arg > env var > default ───────
    _cloud_raw = (args.cloud_mode or args.cloud_mode_pos or os.getenv("CLOUD_MODE", "true")).strip().lower()
    cloud_mode = _cloud_raw == "true"

    # ── Paths: named flag > env var > Code Ocean default ─────────────────────
    _data_env    = (args.data_folder    or os.getenv("DATA_FOLDER",    "")).strip()
    _results_env = (args.results_folder or os.getenv("RESULTS_FOLDER", "")).strip()
    data_folder    = Path(_data_env)    if _data_env    else Path(os.path.abspath("../data"))
    results_folder = Path(_results_env) if _results_env else Path(os.path.abspath("../results"))

    # ── Output destination: named flag > positional arg > env var ─────────────
    output_bucket    = os.getenv("OUTPUT_BUCKET")
    output_path_env  = os.getenv("OUTPUT_PATH", "").strip()
    _output_explicit = args.output_path or args.output_path_pos
    effective_output = _output_explicit or (output_bucket if cloud_mode else output_path_env)

    # ── Notifications ─────────────────────────────────────────────────────────
    alert_bot_link = os.getenv("ALERT_BOT_LINK")
    if not alert_bot_link:
        logger.warning("ALERT_BOT_LINK not set; Teams alerts will be skipped.")
    alert_configs  = get_yaml_config(SCRIPT_DIR.joinpath("utils/alert_configs.yml"))

    # ── Visualisation / CO: named flag > env var ──────────────────────────────
    ng_base_url       = args.ng_base_url       or os.getenv("NG_BASE_URL", "https://neuroglancer-demo.appspot.com/")
    ccf_annotation_s3 = args.ccf_annotation_s3 or os.getenv("CCF_ANNOTATION_S3")
    source_email      = os.getenv("SOURCE_EMAIL")
    co_domain         = args.co_domain         or os.getenv("CODEOCEAN_DOMAIN")

    logger.info(
        "Dispatcher started",
        extra={
            "event_type": "stage_start",
            "mode": mode,
            "cloud_mode": cloud_mode,
            "data_folder": str(data_folder),
            "results_folder": str(results_folder),
            "effective_output": effective_output,
            "ng_base_url": ng_base_url,
            "ccf_annotation_s3": ccf_annotation_s3,
            "co_domain": co_domain,
            "source_email": source_email,
            "alert_bot_link_set": bool(alert_bot_link),
        },
    )

    dataset_name = ""
    investigators = []
    email_message_params = {}

    try:
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

        if "split_channels" in mode:
            dataset_name, investigators, email_message_params = handle_split_channels(
                data_folder=data_folder,
                results_folder=results_folder,
                output_path=effective_output,
                cloud_mode=cloud_mode,
                logger=logger,
            )

        elif "dispatch" in mode:
            dataset_name, investigators, email_message_params = handle_dispatch(
                data_folder=data_folder,
                results_folder=results_folder,
                output_path=effective_output,
                cloud_mode=cloud_mode,
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
                cloud_mode=cloud_mode,
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
                output_path=effective_output,
                cloud_mode=cloud_mode,
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

    except Exception:
        duration_seconds = round(time.monotonic() - start_time, 3)
        logger.error(
            "Dispatcher failed",
            exc_info=True,
            extra={
                "event_type": "stage_failure",
                "mode": mode,
                "dataset_name": dataset_name,
                "duration_seconds": duration_seconds,
            },
        )
        raise

    duration_seconds = round(time.monotonic() - start_time, 3)
    logger.info(
        "Dispatcher completed",
        extra={
            "event_type": "stage_complete",
            "mode": mode,
            "dataset_name": dataset_name,
            "duration_seconds": duration_seconds,
        },
    )


if __name__ == "__main__":
    run()
