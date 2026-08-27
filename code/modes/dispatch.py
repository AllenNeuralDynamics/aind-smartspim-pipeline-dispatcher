"""
Dispatch mode — registers the stitched data asset in Code Ocean and creates
per-channel segmentation manifests for the flatten connection.
"""

import json
import logging
import os
import re
import time
from glob import glob
from pathlib import Path
from typing import List, Tuple, Union

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.data_assets_requests import (
    CreateDataAssetRequest,
    Source,
    Sources,
)

from __init__ import __pipeline_name__, __pipeline_version__, __url__
from utils import metadata_compat, utils
from manifests.builder import get_data_config

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def wait_for_data_availability(
    co_client,
    data_asset_id: str,
    timeout_seconds: int = 300,
    pause_interval: int = 10,
):
    """
    Polls the Code Ocean API until a registered data asset is available.

    Parameters
    ----------
    co_client:
        Authenticated CodeOceanClient instance.
    data_asset_id:
        ID of the data asset to wait for.
    timeout_seconds:
        Maximum total wait time in seconds.
    pause_interval:
        Seconds to sleep between polls.

    Returns
    -------
    requests.Response
        The last response from the API.
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
    Makes a registered data asset viewable by everyone.

    Parameters
    ----------
    co_client:
        Authenticated CodeOceanClient instance.
    response_contents:
        Dict returned by the create_data_asset API call (must contain "id").
    """
    data_asset_id = response_contents["id"]
    response_data_available = wait_for_data_availability(co_client, data_asset_id)

    if response_data_available.status_code != 200:
        logger.info(f"Unable to find: {data_asset_id}")
        return

    update_data_perm_response = co_client.update_permissions(
        data_asset_id=data_asset_id, everyone="viewer"
    )
    logger.info(
        "Data asset made viewable to everyone",
        extra={
            "data_asset_id": data_asset_id,
            "status_code": getattr(update_data_perm_response, "status_code", None),
        },
    )


def dispatch(
    processing_manifest: dict,
    results_folder: PathLike,
    bucket: str,
    co_domain: str = None,
):
    """
    Registers the stitched SmartSPIM dataset as a Code Ocean data asset and
    writes per-channel segmentation manifest JSON files for the flatten connection.

    Parameters
    ----------
    processing_manifest:
        Full processing manifest dict.
    results_folder:
        Path to the Code Ocean results folder.
    bucket:
        S3 bucket name where the stitched data is stored.
    co_domain:
        Code Ocean API base URL. Reads CODEOCEAN_DOMAIN env var when None.
        Data-asset registration is skipped when neither is set.
    """
    logger.debug(f"Provided processing manifest: {processing_manifest}")

    co_domain = co_domain or os.getenv("CODEOCEAN_DOMAIN")

    dataset_to_register = processing_manifest["pipeline_processing"]["stitching"][
        "s3_path"
    ]

    if co_domain:
        # The SmartSPIM_ prefix is optional: v2 asset names drop it
        pattern = (
            r"(?:SmartSPIM_)?\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
            r"_stitched_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        )
        found_pattern = re.findall(pattern=pattern, string=dataset_to_register)

        if len(found_pattern):
            dataset_to_register = found_pattern[0]

            smartspim_fused_tags = ["smartspim", "processed"]

            co_token = os.getenv("API_SECRET")
            co_client = CodeOceanClient(domain=co_domain, token=co_token)

            aws_source = Sources.AWS(
                bucket=bucket,
                prefix=dataset_to_register,
                keep_on_external_storage=True,
                public=True,
            )
            source = Source(aws=aws_source)

            create_data_asset_request = CreateDataAssetRequest(
                name=dataset_to_register,
                tags=smartspim_fused_tags,
                mount=dataset_to_register,
                source=source,
                custom_metadata=None,
            )

            input_json_data = json.loads(create_data_asset_request.json_string)

            try:
                data_asset_reg_response = co_client.create_data_asset(
                    request=input_json_data
                )

                response_contents = data_asset_reg_response.json()
                logger.info(
                    "Created data asset in Code Ocean",
                    extra={
                        "data_asset_id": response_contents.get("id"),
                        "data_asset_name": dataset_to_register,
                    },
                )

                make_data_viewable(co_client, response_contents)

            except Exception as e:
                logger.warning(f"Error registering data asset in the API call. Error: {e}")

        else:
            logger.warning(
                "Skipping data-asset registration: the stitched dataset name in "
                f"stitching.s3_path did not match the expected pattern: {dataset_to_register}"
            )
    else:
        logger.warning("CODEOCEAN_DOMAIN not set; skipping data-asset registration.")

    pipeline_config = processing_manifest.get("pipeline_processing")

    if pipeline_config:
        logger.info("Creating segmentation and quantification parameters")
        segment_channels = pipeline_config["segmentation"]["channels"]
        background_channel = processing_manifest["pipeline_processing"]["registration"][
            "channels"
        ][0]

        if len(segment_channels):
            logger.info(f"Preparing segmentation configs for channels: {segment_channels}")

            for channel_to_segment in segment_channels:
                copy_pipeline_config = pipeline_config.copy()

                copy_pipeline_config["segmentation"]["input_data"] = "../data/fused"
                copy_pipeline_config["segmentation"]["channel"] = channel_to_segment
                copy_pipeline_config["segmentation"][
                    "background_channel"
                ] = background_channel

                copy_pipeline_config["quantification"] = {}
                copy_pipeline_config["quantification"]["fused_folder"] = "../data/fused"
                copy_pipeline_config["quantification"]["channel"] = channel_to_segment
                copy_pipeline_config["quantification"]["save_path"] = "../results/"

                utils.save_dict_as_json(
                    f"{results_folder}/segmentation_processing_manifest_{channel_to_segment}.json",
                    copy_pipeline_config,
                )

        else:
            utils.save_dict_as_json(
                f"{results_folder}/segmentation_processing_manifest_empty.json",
                pipeline_config.copy(),
            )

            logger.warning("No segmentation channels provided in the processing manifest")
            logger.debug(f"Pipeline config without segmentation channels: {pipeline_config}")

    else:
        raise BaseException("Stopping pipeline, pipeline configuration.")


def copy_intermediate_data(
    output_dispatch_metadata: PathLike,
    flatfield_folder: List[PathLike],
    destripe_files: List[PathLike],
    stitch_folder: List[PathLike],
    fuse_folder: List[PathLike],
    ccf_folders: List[PathLike],
    s3_path: str,
    results_folder: PathLike,
    logger: logging.Logger,
    cloud_mode: bool = True,
):
    """
    Copies the destripe, stitch and fusion metadata
    to the destination bucket to make it available
    to scientists as soon as possible.

    Parameters
    ----------
    output_dispatch_metadata: PathLike
        Path where the new metadata (derived)
        for the processed dataset is located

    destripe_files: List[PathLike]
        Metadata files generated in the
        parallel destriping step

    flatfield_channels: List[PathLike]
        Flatfields applied to the dataset

    stitch_folders: List[PathLike]
        Stitch folders generated in the
        stitch step.

    fuse_folders: List[PathLike]
        Fuse folders generated in the
        parallel fusion step.

    ccf_folders: List[PathLike]
        CCF registration folders generated
        in the pipeline.

    s3_path: str
        Path where we want to copy the data to s3.

    results_folder: PathLike
        Results folder path in Code Ocean

    logger: logging.Logger
        Logging object

    """
    flatfield_processings = [str(flatfield_folder.joinpath("metadata/processing.json"))]
    stitch_processings = [str(stitch_folder.joinpath("metadata/processing.json"))]
    fuse_processings = [str(p) for p in list(fuse_folder.glob("*_processing.json"))]
    ccf_processings = []

    for ccf_folder in ccf_folders:
        processing_jsons = [
            p
            for p in glob(f"{ccf_folder}/metadata/*processing*.json")
            if "manifest" not in str(p)
        ]
        ccf_processings.append(processing_jsons)

    # Flattening list
    processing_paths = list()
    combined_processing_list = ccf_processings
    for sub_list in combined_processing_list:
        processing_paths += sub_list

    processing_paths = (
        flatfield_processings
        + destripe_files
        + stitch_processings
        + fuse_processings
        + processing_paths
    )
    logger.info(f"Compiling {len(processing_paths)} processing.json files")
    logger.debug(f"Processing paths: {processing_paths}")

    try:
        output_filename = utils.compile_processing_jsons(
            processing_paths=processing_paths,
            output_general_processing=output_dispatch_metadata,
            pipeline_name=__pipeline_name__,
            pipeline_version=__pipeline_version__,
            pipeline_url=__url__,
        )

    except Exception:
        logger.error(
            "Error while compiling processing manifests; continuing without a "
            "compiled processing.json",
            exc_info=True,
        )
        output_filename = None

    logger.info(f"Compiled processing.json in path {output_filename}")

    output_dispatch_metadata = Path(output_dispatch_metadata)
    output_fusion = "image_tile_fusing"
    dest_metadata_path = f"{s3_path}/{output_fusion}/metadata"
    dest_zarr_path = f"{s3_path}/{output_fusion}/OMEZarr"
    ccf_s3_output = f"{s3_path}/image_atlas_alignment"
    regex_channels = r"Ex_(\d{3})_Em_(\d{3})|ccf_reverse|ccf_annotation_precomputed"

    if cloud_mode:
        # ── Cloud: push to S3 ────────────────────────────────────────────────
        utils.run_s3_transfer(
            f"aws s3 cp --recursive {output_dispatch_metadata} {s3_path}",
            logger,
            f"derived metadata -> {s3_path}",
        )

        utils.run_s3_transfer(
            f"aws s3 cp --recursive {flatfield_folder} {dest_metadata_path}/flatfield_correction",
            logger,
            f"flatfield correction -> {dest_metadata_path}/flatfield_correction",
        )

        for fused_zarr in fuse_folder.glob("*.zarr"):
            utils.run_s3_transfer(
                f"aws s3 cp --recursive {fused_zarr} {dest_zarr_path}/{fused_zarr.name}",
                logger,
                f"fused OMEZarr {fused_zarr.name} -> {dest_zarr_path}/{fused_zarr.name}",
                extra={"channel": fused_zarr.stem},
            )

        for fused_metadata in list(fuse_folder.glob("*.yaml")) + list(fuse_folder.glob("*.json")):
            utils.run_s3_transfer(
                f"aws s3 cp {fused_metadata} {dest_metadata_path}/fusion/{fused_metadata.name}",
                logger,
                f"fusion metadata {fused_metadata.name} -> {dest_metadata_path}/fusion",
            )

        utils.run_s3_transfer(
            f"aws s3 cp --recursive {stitch_folder} {dest_metadata_path}/stitching",
            logger,
            f"stitching metadata -> {dest_metadata_path}/stitching",
        )

        for ccf_folder in ccf_folders:
            channel_name = re.search(regex_channels, ccf_folder).group()
            utils.run_s3_transfer(
                f"aws s3 mv --recursive {ccf_folder} {ccf_s3_output}/{channel_name}",
                logger,
                f"CCF registration {channel_name} -> {ccf_s3_output}/{channel_name}",
                extra={"channel": channel_name},
            )

    else:
        # ── Local: rearrange on the filesystem ───────────────────────────────
        utils.create_folder(dest_metadata_path)
        utils.create_folder(dest_zarr_path)

        utils.run_s3_transfer(
            f"cp {output_dispatch_metadata}/*.json {s3_path}/",
            logger,
            f"derived metadata -> {s3_path}",
        )

        dest_ff = f"{dest_metadata_path}/flatfield_correction"
        utils.create_folder(dest_ff)
        utils.run_s3_transfer(
            f"cp -r {flatfield_folder}/. {dest_ff}/",
            logger,
            f"flatfield correction -> {dest_ff}",
        )

        for fused_zarr in fuse_folder.glob("*.zarr"):
            dest = f"{dest_zarr_path}/{fused_zarr.name}"
            utils.create_folder(dest)
            utils.run_s3_transfer(
                f"cp -r {fused_zarr}/. {dest}/",
                logger,
                f"fused OMEZarr {fused_zarr.name} -> {dest}",
                extra={"channel": fused_zarr.stem},
            )

        dest_fusion_meta = f"{dest_metadata_path}/fusion"
        utils.create_folder(dest_fusion_meta)
        for fused_metadata in list(fuse_folder.glob("*.yaml")) + list(fuse_folder.glob("*.json")):
            utils.run_s3_transfer(
                f"cp {fused_metadata} {dest_fusion_meta}/{fused_metadata.name}",
                logger,
                f"fusion metadata {fused_metadata.name} -> {dest_fusion_meta}",
            )

        dest_stitch = f"{dest_metadata_path}/stitching"
        utils.create_folder(dest_stitch)
        utils.run_s3_transfer(
            f"cp -r {stitch_folder}/. {dest_stitch}/",
            logger,
            f"stitching metadata -> {dest_stitch}",
        )

        for ccf_folder in ccf_folders:
            channel_name = re.search(regex_channels, ccf_folder).group()
            dest_ccf = f"{ccf_s3_output}/{channel_name}"
            utils.create_folder(dest_ccf)
            utils.run_s3_transfer(
                f"mv {ccf_folder}/* {dest_ccf}/",
                logger,
                f"CCF registration {channel_name} -> {dest_ccf}",
                extra={"channel": channel_name},
            )

    utils.save_string_to_txt(
        f"Stitched dataset saved in: {s3_path}",
        f"{results_folder}/output_stitching.txt",
    )


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
        files_to_copy=[
            "acquisition.json",
            "instrument.json",
            "subject.json",
            "procedures.json",
            "session.json",
        ],
    )

    logger.info(f"Copied metadata from {raw_metadata_path}: {found_metadata}")
    logger.debug(
        f"Metadata in raw folder {raw_metadata_path}: {os.listdir(raw_metadata_path)}"
    )
    logger.debug(
        f"Metadata in folder {output_dispatch_metadata}: {os.listdir(output_dispatch_metadata)}"
    )

    return output_dispatch_metadata, new_dataset_name


def handle_dispatch(
    data_folder: PathLike,
    results_folder: PathLike,
    output_path: str,
    ng_base_url: str,
    ccf_annotation_s3: str,
    co_domain: str,
    axes_resolution_xyz: List,
    logger: logging.Logger,
    cloud_mode: bool = True,
) -> Tuple[str, list, dict]:
    """
    Handles the dispatch mode: registers the stitched dataset, builds
    Neuroglancer links, and fans out per-channel segmentation manifests.

    Returns
    -------
    Tuple[str, list, dict]
        (dataset_name, investigators, email_message_params)
    """
    from utils.visualization import create_neuroglancer_link

    pipeline_config, dataset_name, investigators = get_data_config(
        data_folder=data_folder,
        data_description_path="input_aind_metadata/data_description.json",
    )

    output_dispatch_metadata, new_dataset_name = create_derived_stitched_metadata(
        data_folder=data_folder, results_folder=results_folder, logger=logger
    )

    logger.info(
        f"Derived stitched asset created: {new_dataset_name}",
        extra={
            "event_type": "dataset_resolved",
            "dataset_name": metadata_compat.get_raw_dataset_name(dataset_name),
            "asset_name": new_dataset_name,
        },
    )

    flatfield_folder = data_folder.joinpath("flatfield_estimation")
    destripe_files = [str(p) for p in list(data_folder.glob("image_destriping_*"))]
    stitch_folder = data_folder.joinpath("stitched")
    fuse_folder = data_folder.joinpath("fused")
    ccf_folders = glob(f"{data_folder}/ccf_registration_results/ccf_*")

    # bucket_path is used for Code Ocean registration and NG links (cloud only)
    bucket_path = output_path if cloud_mode else ""
    if not output_path:
        logger.warning("Output path not set; copy and dispatch steps will be skipped.")
    dest_root = (
        f"s3://{output_path}/{new_dataset_name}" if cloud_mode
        else f"{output_path}/{new_dataset_name}"
    )
    dest_zarr_path = f"{dest_root}/image_tile_fusing/OMEZarr"
    dest_reg_path = f"{dest_root}/image_atlas_alignment"

    s3_paths_for_channels = [
        f"{dest_zarr_path}/{fused_zarr.name}"
        for fused_zarr in fuse_folder.glob("*.zarr")
    ]

    channel_dynamic_ranges = utils.calculate_dynamic_range(
        fuse_folder=fuse_folder, extension="*.zarr", percentile=99, level=3
    )
    orientation = pipeline_config["prelim_acquisition"]

    # Getting the subject id from the metadata since v2 asset names
    # do not follow the SmartSPIM_<subject_id>_... convention
    subject_id = utils.read_json_as_dict(
        str(data_folder.joinpath("input_aind_metadata/data_description.json"))
    ).get("subject_id")

    output_json, ng_link_path = create_neuroglancer_link(
        config={
            "bucket_path": bucket_path,
            "output_folder": results_folder,
            "ng_base_url": ng_base_url,
            "z_res": axes_resolution_xyz[2],
            "y_res": axes_resolution_xyz[1],
            "x_res": axes_resolution_xyz[0],
        },
        s3_channel_paths=s3_paths_for_channels,
        s3_dataset_path=dest_root,
        orientation=orientation,
        dynamic_ranges=channel_dynamic_ranges,
        segmentation=False,
        ccf=False,
        ccf_annotation_s3=ccf_annotation_s3,
        subject_id=subject_id,
    )

    email_message_params = {"ng_link_path": ng_link_path}

    qc_evaluators = [
        {
            "name": "Neuroglancer Link Evaluation",
            "description": "Checks that the whole-brain neuroglancer link was created",
            "notes": "",
            "stage": "Processing",
            "qc_metric_values": [
                {
                    "name": "Dataset neuroglancer link",
                    "description": "Qualitative check that the neuroglancer link was created",
                    "value": "",
                    "reference": ng_link_path,
                    "status": "Pending",
                },
            ],
        },
    ]

    utils.create_quality_control_metadata(
        qc_eval_values=qc_evaluators,
        output_path=output_dispatch_metadata,
    )

    if cloud_mode:
        utils.run_s3_transfer(
            f"aws s3 cp {output_json} {dest_root}/{output_json.name}",
            logger,
            f"whole-brain neuroglancer config -> {dest_root}/{output_json.name}",
        )
    else:
        utils.create_folder(dest_root)
        utils.run_s3_transfer(
            f"cp {output_json} {dest_root}/{output_json.name}",
            logger,
            f"whole-brain neuroglancer config -> {dest_root}/{output_json.name}",
        )

    # CCF overlay in raw space
    output_json, ng_link_path = create_neuroglancer_link(
        config={
            "bucket_path": bucket_path,
            "output_folder": results_folder,
            "ng_base_url": ng_base_url,
            "z_res": axes_resolution_xyz[2],
            "y_res": axes_resolution_xyz[1],
            "x_res": axes_resolution_xyz[0],
        },
        s3_channel_paths=s3_paths_for_channels,
        s3_dataset_path=dest_root,
        orientation=orientation,
        dynamic_ranges=channel_dynamic_ranges,
        segmentation=True,
        ccf=False,
        ccf_annotation_s3=ccf_annotation_s3,
        subject_id=subject_id,
    )

    if cloud_mode:
        utils.run_s3_transfer(
            f"aws s3 cp {output_json} {dest_root}/image_atlas_alignment/{output_json.name}",
            logger,
            f"CCF-overlay neuroglancer config -> {dest_root}/image_atlas_alignment/{output_json.name}",
        )
    else:
        utils.create_folder(f"{dest_root}/image_atlas_alignment")
        utils.run_s3_transfer(
            f"cp {output_json} {dest_root}/image_atlas_alignment/{output_json.name}",
            logger,
            f"CCF-overlay neuroglancer config -> {dest_root}/image_atlas_alignment/{output_json.name}",
        )

    # Registered images with CCF overlay
    reg_folder = Path(f"{data_folder}/ccf_registration_results")
    ccf_resolution = 25

    s3_paths_for_reg_channels = [
        f"{dest_reg_path}/{reg_zarr.name[4:]}/OMEZarr/image.zarr"
        for reg_zarr in reg_folder.glob("ccf_Ex_*")
    ]

    logger.info(
        f"Registered channels for the CCF-space neuroglancer link: {s3_paths_for_reg_channels}"
    )

    channel_dynamic_ranges = utils.calculate_dynamic_range(
        fuse_folder=reg_folder, extension="ccf_Ex_*/OMEZarr/image.zarr", percentile=99, level=0
    )

    logger.debug(f"Registered-channel dynamic ranges: {channel_dynamic_ranges}")

    output_json, ng_link_path = create_neuroglancer_link(
        config={
            "bucket_path": bucket_path,
            "output_folder": results_folder,
            "ng_base_url": ng_base_url,
            "z_res": ccf_resolution,
            "y_res": ccf_resolution,
            "x_res": ccf_resolution,
        },
        s3_channel_paths=s3_paths_for_reg_channels,
        s3_dataset_path=dest_root,
        orientation=[0, 1, 0, 0],
        dynamic_ranges=channel_dynamic_ranges,
        segmentation=False,
        ccf=True,
        ccf_annotation_s3=ccf_annotation_s3,
        subject_id=subject_id,
    )

    if cloud_mode:
        utils.run_s3_transfer(
            f"aws s3 cp {output_json} {dest_root}/image_atlas_alignment/ccf_visualization/{output_json.name}",
            logger,
            f"CCF-space neuroglancer config -> {dest_root}/image_atlas_alignment/ccf_visualization/{output_json.name}",
        )
    else:
        utils.create_folder(f"{dest_root}/image_atlas_alignment/ccf_visualization")
        utils.run_s3_transfer(
            f"cp {output_json} {dest_root}/image_atlas_alignment/ccf_visualization/{output_json.name}",
            logger,
            f"CCF-space neuroglancer config -> {dest_root}/image_atlas_alignment/ccf_visualization/{output_json.name}",
        )

    copy_intermediate_data(
        output_dispatch_metadata=output_dispatch_metadata,
        flatfield_folder=flatfield_folder,
        destripe_files=destripe_files,
        stitch_folder=stitch_folder,
        fuse_folder=fuse_folder,
        ccf_folders=ccf_folders,
        s3_path=dest_root,
        results_folder=results_folder,
        logger=logger,
        cloud_mode=cloud_mode,
    )

    data_results = glob(f"{results_folder}/*")
    logger.info(f"Data in {results_folder}: {data_results}")

    pipeline_config["pipeline_processing"]["stitching"]["s3_path"] = dest_root

    dispatch(
        processing_manifest=pipeline_config,
        results_folder=results_folder,
        bucket=bucket_path,
        co_domain=co_domain,
    )

    utils.save_dict_as_json(
        f"{results_folder}/modified_processing_manifest.json",
        pipeline_config,
    )

    return dataset_name, investigators or [], email_message_params
