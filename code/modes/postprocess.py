"""
Post-processing helpers — data copying, processing metadata filtering,
and position removal utilities used in the postprocess-start and
postprocess-stop pipeline modes.
"""

import copy
import logging
import re
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Union

from aind_data_schema.core.processing import Processing, ProcessName
from pydantic import TypeAdapter

from utils import utils
from utils.io import read_json_as_dict
from utils.versioning import (
    check_dataset_latest_version,
    get_dataset_step_versions,
    get_pipeline_versions,
)
from manifests.builder import (
    create_segmentation_manifests,
    get_omezarr_path,
    get_processing_manifest_path,
    get_standard_manifest_config,
)

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def remove_positions(lst: List, positions: List[int]) -> List:
    """
    Returns lst with items at the given positions removed.

    Parameters
    ----------
    lst:
        Source list.
    positions:
        Zero-based indices to remove.

    Raises
    ------
    ValueError
        If any position is out of range.
    """
    len_objs = len(lst)
    max_index = max(positions)
    min_index = min(positions)

    if min_index < 0 or max_index > len_objs:
        msg = (
            "Error in removal of positions"
            f"Positions: {positions} - Len list: {len_objs}"
        )
        raise ValueError(msg)

    return [obj for idx, obj in enumerate(lst) if idx not in set(positions)]


def get_filtered_proc_metadata(
    input_proc_json: Path,
    copy_ccf: bool,
    copy_classification: bool,
    copy_quantification: bool,
) -> Tuple:
    """
    Returns a Processing object with steps removed that will be re-executed,
    along with the list of deleted position indices.

    Parameters
    ----------
    input_proc_json:
        Path to an existing processing.json.
    copy_ccf, copy_classification, copy_quantification:
        Flags indicating which steps will be reprocessed (and must be removed).

    Returns
    -------
    Tuple[Processing, List[int]]
    """
    from utils.io import read_json_as_dict

    if not input_proc_json.exists():
        raise FileNotFoundError(
            f"Processing.json was not provided in {input_proc_json}"
        )

    input_proc_json_data = read_json_as_dict(input_proc_json)
    processing_adapter = TypeAdapter(Processing)
    input_proc_json_data_obj = processing_adapter.validate_python(input_proc_json_data)
    input_proc_json_data_obj_copy = copy.deepcopy(input_proc_json_data_obj)

    if not copy_ccf and not copy_classification and not copy_quantification:
        return input_proc_json_data_obj_copy, []

    delete_pos = []
    data_processes = input_proc_json_data_obj.data_processes
    for i, dt_proc in enumerate(data_processes):
        remove_reg = copy_ccf and (
            "aind-ccf-registration" in dt_proc.code.url
            or ProcessName.IMAGE_ATLAS_ALIGNMENT == dt_proc.process_type
        )

        remove_cell = copy_classification and (
            "aind-SmartSPIM-segmentation" in dt_proc.code.url
            or "aind-smartspim-classification" in dt_proc.code.url
            or ProcessName.IMAGE_CELL_SEGMENTATION == dt_proc.process_type
        )

        remove_quant = copy_quantification and (
            "aind-smartspim-quantification" in dt_proc.code.url
            or ProcessName.IMAGE_CELL_QUANTIFICATION == dt_proc.process_type
        )

        if remove_reg or remove_cell or remove_quant:
            delete_pos.append(i)

    input_proc_json_data_obj_copy.data_processes = remove_positions(
        data_processes, delete_pos
    )
    return input_proc_json_data_obj_copy, delete_pos


def copy_postprocessed_data(
    post_fuse_folder: PathLike,
    output_dispatch_metadata: PathLike,
    ccf_folders: List[PathLike],
    cell_folders: List[PathLike],
    quantification_folders: List[PathLike],
    s3_path: str,
    results_folder: PathLike,
    new_processing_path: str,
    logger: logging.Logger,
    cloud_mode: bool = True,
):
    """
    Copies postprocessed CCF, cell, and quantification results to S3.

    Parameters
    ----------
    post_fuse_folder:
        Local image_tile_fusing folder from the stitched data asset.
    output_dispatch_metadata:
        Local folder containing the derived AIND metadata to upload.
    ccf_folders, cell_folders, quantification_folders:
        Local folders produced by registration, classification, and quantification.
    s3_path:
        Destination S3 path (e.g. s3://bucket/<dataset_name>).
    results_folder:
        Code Ocean results folder.
    new_processing_path:
        Path to the compiled processing.json to upload.
    logger:
        Logger instance.
    """
    if (
        not len(ccf_folders)
        and not len(cell_folders)
        and not len(quantification_folders)
    ):
        msg = (
            f"Avoiding copy. CCF: {ccf_folders} - CELL: {cell_folders}"
            f" - QUANT: {quantification_folders}"
        )
        logger.info(msg)
        return None

    ccf_s3_output = f"{s3_path}/image_atlas_alignment"
    cell_s3_output = f"{s3_path}/image_cell_segmentation"
    quantification_s3_output = f"{s3_path}/image_cell_quantification"
    fusion_s3_output = f"{s3_path}/image_tile_fusing"
    regex_channels = r"Ex_(\d{3})_Em_(\d{3})$"

    output_dispatch_metadata = Path(output_dispatch_metadata)

    if cloud_mode:
        for out in utils.execute_command_helper(
            f"aws s3 cp --recursive {output_dispatch_metadata} {s3_path}"
        ):
            logger.info(out)

        for out in utils.execute_command_helper(
            f"aws s3 cp {new_processing_path} {s3_path}/processing.json"
        ):
            logger.info(out)

        for ccf_folder in ccf_folders:
            channel_name = re.search(regex_channels, str(ccf_folder)).group()
            for out in utils.execute_command_helper(
                f"aws s3 mv --recursive {ccf_folder} {ccf_s3_output}/{channel_name}"
            ):
                logger.info(out)

        for cell_folder in cell_folders:
            channel_name = re.search(regex_channels, str(cell_folder)).group()
            for out in utils.execute_command_helper(
                f"aws s3 mv --recursive {cell_folder} {cell_s3_output}/{channel_name}"
            ):
                print(out)

        for quantification_folder in quantification_folders:
            channel_name = re.search(regex_channels, str(quantification_folder)).group()
            for out in utils.execute_command_helper(
                f"aws s3 mv --recursive {quantification_folder} {quantification_s3_output}/{channel_name}"
            ):
                print(out)

        for out in utils.execute_command_helper(
            f"aws s3 cp --recursive {post_fuse_folder} {fusion_s3_output}"
        ):
            logger.info(out)

    else:
        utils.create_folder(s3_path)
        for out in utils.execute_command_helper(
            f"cp -r {output_dispatch_metadata}/. {s3_path}/"
        ):
            logger.info(out)

        for out in utils.execute_command_helper(
            f"cp {new_processing_path} {s3_path}/processing.json"
        ):
            logger.info(out)

        for ccf_folder in ccf_folders:
            channel_name = re.search(regex_channels, str(ccf_folder)).group()
            dest = f"{ccf_s3_output}/{channel_name}"
            utils.create_folder(dest)
            for out in utils.execute_command_helper(f"mv {ccf_folder}/* {dest}/"):
                logger.info(out)

        for cell_folder in cell_folders:
            channel_name = re.search(regex_channels, str(cell_folder)).group()
            dest = f"{cell_s3_output}/{channel_name}"
            utils.create_folder(dest)
            for out in utils.execute_command_helper(f"mv {cell_folder}/* {dest}/"):
                print(out)

        for quantification_folder in quantification_folders:
            channel_name = re.search(regex_channels, str(quantification_folder)).group()
            dest = f"{quantification_s3_output}/{channel_name}"
            utils.create_folder(dest)
            for out in utils.execute_command_helper(f"mv {quantification_folder}/* {dest}/"):
                print(out)

        utils.create_folder(fusion_s3_output)
        for out in utils.execute_command_helper(f"cp -r {post_fuse_folder}/. {fusion_s3_output}/"):
            logger.info(out)

    utils.save_string_to_txt(
        f"Results of cell segmentation saved in: {cell_s3_output}",
        f"{results_folder}/output_cell.txt",
    )

    utils.save_string_to_txt(
        f"Results of quantification saved in: {quantification_s3_output}",
        f"{results_folder}/output_quantification.txt",
    )


def get_dataset_post_processing_config(
    processed_step_versions: Dict, pipeline_processing: Dict, latest_step_versions: Dict
):
    """
    Creates the configuration for the postprocessing pipeline.
    The idea is that if the versions of the image processing
    steps is different, then it will have to be executed.

    However, a step will only be executed if it has a
    configuration within the processing manifest.

    Parameters
    ----------
    processed_step_version: Dict
        Versions of the image processing steps that were
        executed for the dataset.

    pipeline_processing: Dict
        Dictionary with the steps that need to be executed
        for this dataset. It is the configuration within
        the processing_manifest.json in derivatives.

    latest_step_version: Dict
        Dictionary with the latest versions of the
        image processing steps published in the pipeline.

    Returns
    -------
    Dict
        Dictionary with the final configuration
        for each of the image processing steps.
    """
    final_config = {
        "pipeline_processing": pipeline_processing,
        "need_registration": {},
        "need_proposals": {},
        "need_classification": {},
        "need_quantification": {},
    }

    if processed_step_versions and pipeline_processing:
        process_versions = check_dataset_latest_version(
            processed_step_versions, latest_step_versions
        )

        image_reg_cfg = pipeline_processing.get("registration")
        image_seg_cfg = pipeline_processing.get("segmentation")

        reg_channels = image_reg_cfg.get("channels")
        seg_channels = image_seg_cfg.get("channels")

        version_control_reg = process_versions[
            "aind-smartspim-ccf-registration - Image atlas alignment"
        ]["process"]

        # Checking if there's something in the manifest
        manifest_reg = reg_channels[0] if reg_channels and len(reg_channels) else []
        manifest_seg = seg_channels[0] if seg_channels and len(seg_channels) else []

        version_control_proposals = process_versions[
            "aind-smartspim-segmentation - Image cell segmentation"
        ]["process"]

        version_control_classification = process_versions[
            "aind-smartspim-classification - Image cell segmentation"
        ]["process"]

        version_control_quantification = process_versions[
            "aind-smartspim-quantification - Image cell quantification"
        ]["process"]

        if len(manifest_reg) and version_control_reg:
            need_reg = process_versions[
                "aind-smartspim-ccf-registration - Image atlas alignment"
            ]

        if len(manifest_seg):
            # Might need segmentation, classification or quantification
            if version_control_proposals:
                final_config["need_proposals"] = process_versions[
                    "aind-smartspim-segmentation - Image cell segmentation"
                ]
                final_config["need_classification"] = process_versions[
                    "aind-smartspim-classification - Image cell segmentation"
                ]
                final_config["need_quantification"] = process_versions[
                    "aind-smartspim-quantification - Image cell quantification"
                ]

            elif version_control_classification:
                final_config["need_classification"] = process_versions[
                    "aind-smartspim-classification - Image cell segmentation"
                ]
                final_config["need_quantification"] = process_versions[
                    "aind-smartspim-quantification - Image cell quantification"
                ]

            elif version_control_quantification or len(need_reg):
                final_config["need_quantification"] = process_versions[
                    "aind-smartspim-quantification - Image cell quantification"
                ]

    elif pipeline_processing:
        image_reg_cfg = pipeline_processing.get("registration")
        image_seg_cfg = pipeline_processing.get("segmentation")

        reg_channels = image_reg_cfg.get("channels")
        seg_channels = image_seg_cfg.get("channels")

        manifest_reg = reg_channels[0] if reg_channels and len(reg_channels) else []
        manifest_seg = seg_channels[0] if seg_channels and len(seg_channels) else []

        if len(manifest_reg):
            final_config["need_registration"] = {"process": True}

        # Trigger everything if processing.json does not exist
        if len(manifest_seg):
            # Might need segmentation, classification or quantification
            final_config["need_proposals"] = {"process": True}
            final_config["need_classification"] = {"process": True}
            final_config["need_quantification"] = {"process": True}

    else:
        print(
            f"[!!!] Problem getting the process versions: {processed_step_versions} - manifest: {pipeline_processing}"
        )

    return final_config


def handle_postprocess_start(
    data_folder: PathLike,
    results_folder: PathLike,
    pipeline_repos: List,
    manifest_step_names: Dict,
    logger: logging.Logger,
) -> Tuple[str, list, dict]:
    """
    Handles the postprocess-start mode: detects which steps need re-running
    and writes the corresponding manifests.

    Returns
    -------
    Tuple[str, list, dict]
        (dataset_name, investigators, email_message_params)
    """
    from modes.dispatch import create_derived_stitched_metadata

    logger.info("Starting post-processing...")

    raw_path = data_folder.joinpath("raw_data")
    stitched_path = data_folder.joinpath("stitched_data")

    processing_manifest_path = get_processing_manifest_path(raw_path)

    dataset_name = ""
    investigators = []

    if processing_manifest_path is None:
        print(f"[-] ERROR GETTING {processing_manifest_path.stem}")
    else:
        print(f"[+] Processing {raw_path.stem} - {processing_manifest_path}")

        data_description_dict = utils.read_json_as_dict(
            str(raw_path / "data_description.json")
        )

        (
            output_dispatch_metadata,
            new_dataset_name,
        ) = create_derived_stitched_metadata(
            data_folder=data_folder,
            results_folder=results_folder,
            logger=logger,
        )

        investigators = data_description_dict.get("investigators")
        dataset_name = data_description_dict.get("name")
        print(
            f"Postprocessing dataset: {dataset_name} - New asset name: {new_dataset_name}"
        )

        processing_manifest_data = read_json_as_dict(processing_manifest_path)
        latest_step_versions = get_pipeline_versions(pipeline_repos)

        processing_manifest_data[
            "pipeline_processing"
        ] = get_standard_manifest_config(
            pipeline_processing=processing_manifest_data.get("pipeline_processing"),
            hashmap_stepnames=manifest_step_names,
        )

        processed_step_versions = get_dataset_step_versions(stitched_path)

        final_config = get_dataset_post_processing_config(
            processed_step_versions,
            processing_manifest_data["pipeline_processing"],
            latest_step_versions,
        )

        omezarr_folder = get_omezarr_path(stitched_path)

        atlas_alignment_path = stitched_path / "image_atlas_alignment"
        cell_seg_path = stitched_path / "image_cell_segmentation"

        print(f"Final config: {final_config}, {omezarr_folder}")

        need_reg = final_config.get("need_registration", {}).get("process", False)
        need_prop = final_config.get("need_proposals", {}).get("process", False)
        need_class = final_config.get("need_classification", {}).get("process", False)
        need_quant = final_config.get("need_quantification", {}).get("process", False)

        if not need_reg and not need_prop and not need_class and not need_quant:
            empty_pmd = processing_manifest_data.copy()
            empty_pmd["pipeline_processing"]["registration"]["channels"] = []
            empty_pmd["pipeline_processing"]["segmentation"]["channels"] = []

            utils.save_dict_as_json(
                results_folder / "processing_manifest.json", empty_pmd
            )
            utils.save_dict_as_json(
                results_folder / "segmentation_processing_manifest_empty.json",
                empty_pmd["pipeline_processing"],
            )
            utils.save_dict_as_json(
                results_folder / "classification_processing_manifest_empty.json",
                empty_pmd["pipeline_processing"],
            )

        if need_reg:
            utils.save_dict_as_json(
                results_folder / "processing_manifest.json",
                processing_manifest_data,
            )
        elif need_prop or need_class or need_quant:
            atlas_alignment_dest = results_folder / "image_atlas_alignment"
            utils.create_folder(atlas_alignment_dest)

            for ccf_folder in atlas_alignment_path.glob("Ex_*_Em_*"):
                shutil.copytree(
                    ccf_folder, atlas_alignment_dest / f"ccf_{ccf_folder.stem}"
                )

        if need_prop:
            create_segmentation_manifests(
                processing_manifest_data,
                results_folder,
                prefix="segmentation",
            )
        else:
            copy_manifests = processing_manifest_data.copy()
            copy_manifests["pipeline_processing"]["segmentation"]["channels"] = []
            create_segmentation_manifests(
                copy_manifests, results_folder, prefix="segmentation"
            )

        if need_class or need_quant:
            cell_seg_dest = results_folder / "image_cell_segmentation"
            utils.create_folder(cell_seg_dest)

            for cell_folder in cell_seg_path.glob("Ex_*_Em_*"):
                shutil.copytree(
                    cell_folder, cell_seg_dest / f"cell_{cell_folder.stem}"
                )

            create_segmentation_manifests(
                processing_manifest_data, results_folder, prefix="classification"
            )

        processing_json_path = stitched_path.joinpath("processing.json")
        if processing_json_path.exists():
            output_proc_json = results_folder.joinpath("processing.json")
            print(f"Copying {processing_json_path} to {output_proc_json}")
            utils.copy_file(str(processing_json_path), str(output_proc_json))

    return dataset_name, investigators or [], {}


def handle_postprocess_stop(
    data_folder: PathLike,
    results_folder: PathLike,
    output_path: str,
    ng_base_url: str,
    ccf_annotation_s3: str,
    axes_resolution_xyz: List,
    logger: logging.Logger,
    cloud_mode: bool = True,
) -> Tuple[str, list, dict]:
    """
    Handles the postprocess-stop mode: collects updated CCF, segmentation,
    and quantification outputs and copies everything to S3.

    Returns
    -------
    Tuple[str, list, dict]
        (dataset_name, investigators, email_message_params)
    """
    from utils.visualization import create_neuroglancer_link

    ccf_folder = data_folder.joinpath("registration")
    classification_folder = data_folder.joinpath("classification")
    quantification_folder = data_folder.joinpath("quantification")
    postprocess_dispatch_folder = data_folder.joinpath("postprocess_dispatch")
    stitched_data = data_folder.joinpath("stitched_data")

    pipeline_config = utils.read_json_as_dict(
        str(postprocess_dispatch_folder.joinpath("processing_manifest.json"))
    )

    utils.save_dict_as_json(
        f"{results_folder}/provided_processing_manifest.json",
        pipeline_config.copy(),
    )

    copy_ccf = False
    copy_classification = False
    copy_quantification = False

    if ccf_folder.exists():
        copy_ccf = bool(
            len([a for a in list(ccf_folder.glob("ccf_*")) if a.is_dir()])
        )

    if classification_folder.exists():
        copy_classification = bool(
            len([a for a in list(classification_folder.glob("cell_*")) if a.is_dir()])
        )

    if quantification_folder.exists():
        copy_quantification = bool(
            len(
                [a for a in list(quantification_folder.glob("quant_*")) if a.is_dir()]
            )
        )

    dataset_name = ""
    investigators = []
    email_message_params = {}

    if copy_ccf or copy_classification or copy_quantification:
        metadata_folder = postprocess_dispatch_folder.joinpath("output_aind_metadata")

        data_description_dict = utils.read_json_as_dict(
            str(metadata_folder.joinpath("data_description.json"))
        )
        new_dataset_name = data_description_dict.get("name")

        if new_dataset_name is None:
            raise ValueError("New dataset name is None! Please, provide it.")

        dataset_name = new_dataset_name

        bucket_path = output_path if cloud_mode else ""
        if not output_path:
            logger.warning("Output path not set; copy steps will be skipped.")
        dest_root = (
            f"s3://{output_path}/{new_dataset_name}" if cloud_mode
            else f"{output_path}/{new_dataset_name}"
        )
        s3_path = dest_root
        dest_zarr_path = f"{dest_root}/image_tile_fusing/OMEZarr"

        s3_paths_for_channels = [
            f"{dest_zarr_path}/{fused_zarr.name}"
            for fused_zarr in stitched_data.glob(
                "image_tile_fusing/OMEZarr/*.zarr"
            )
        ]

        if not len(s3_paths_for_channels):
            raise FileNotFoundError(
                f"Problem finding zarr data in {stitched_data.joinpath('image_tile_fusing/OMEZarr')}"
            )

        channel_dynamic_ranges = utils.calculate_dynamic_range(
            fuse_folder=stitched_data,
            extension="image_tile_fusing/OMEZarr/*.zarr",
            percentile=99,
            level=3,
        )
        orientation = pipeline_config["prelim_acquisition"]

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
            s3_dataset_path=s3_path,
            orientation=orientation,
            dynamic_ranges=channel_dynamic_ranges,
            segmentation=False,
            ccf=False,
            ccf_annotation_s3=ccf_annotation_s3,
        )

        # TODO Add the function to make segmentation layer for reverse transforms

        email_message_params["ng_link_path"] = ng_link_path

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
            output_path=metadata_folder,
        )

        logger.info(f"Data in stitched data: {list(stitched_data.glob('*'))}")
        input_proc_json = stitched_data.joinpath("processing.json")

        filtered_proc_data, deleted_pos = get_filtered_proc_metadata(
            input_proc_json, copy_ccf, copy_classification, copy_quantification
        )

        logger.info(f"Deleted pos from previous processing: {deleted_pos}")

        ccf_folders = list(ccf_folder.glob("ccf_*"))
        cell_folders = list(classification_folder.glob("cell_*"))
        quantification_folders = list(quantification_folder.glob("quant_*"))

        new_data_procs = []

        processing_adapter = TypeAdapter(Processing)

        if copy_ccf:
            for cf in ccf_folders:
                ccf_proc = cf.joinpath("metadata/processing.json")
                curr_processing = read_json_as_dict(str(ccf_proc))
                curr_processing_obj = processing_adapter.validate_python(curr_processing)

                for data_process in curr_processing_obj.data_processes:
                    new_data_procs.append(data_process)

        if copy_classification:
            for cell_folder in cell_folders:
                class_proc = cell_folder.joinpath("metadata/processing.json")
                proposals_proc = cell_folder.joinpath(
                    "proposals/metadata/processing.json"
                )

                curr_cell_processing = read_json_as_dict(str(class_proc))
                curr_prop_processing = read_json_as_dict(str(proposals_proc))

                curr_cell_processing_obj = processing_adapter.validate_python(
                    curr_cell_processing
                )
                curr_prop_processing_obj = processing_adapter.validate_python(
                    curr_prop_processing
                )

                for data_process in curr_cell_processing_obj.data_processes:
                    new_data_procs.append(data_process)

                for data_process in curr_prop_processing_obj.data_processes:
                    new_data_procs.append(data_process)

        if copy_quantification:
            for quant_folder in quantification_folders:
                quant_proc = quant_folder.joinpath("metadata/processing.json")
                curr_processing = read_json_as_dict(str(quant_proc))
                curr_processing_obj = processing_adapter.validate_python(curr_processing)

                for data_process in curr_processing_obj.data_processes:
                    new_data_procs.append(data_process)

        if len(new_data_procs):
            filtered_proc_data.data_processes.extend(new_data_procs)

        filtered_proc_data.write_standard_file(output_directory=str(results_folder))

        copy_postprocessed_data(
            post_fuse_folder=stitched_data.joinpath("image_tile_fusing"),
            output_dispatch_metadata=metadata_folder,
            ccf_folders=ccf_folders,
            cell_folders=cell_folders,
            quantification_folders=quantification_folders,
            s3_path=s3_path,
            results_folder=results_folder,
            new_processing_path=str(results_folder.joinpath("processing.json")),
            logger=logger,
            cloud_mode=cloud_mode,
        )

        if cloud_mode:
            for out in utils.execute_command_helper(
                f"aws s3 cp {output_json} {s3_path}/{output_json.name}"
            ):
                logger.info(out)
        else:
            utils.create_folder(s3_path)
            for out in utils.execute_command_helper(f"cp {output_json} {s3_path}/{output_json.name}"):
                logger.info(out)

    else:
        print("Avoiding copying data since there was nothing to copy.")

    return dataset_name, investigators, email_message_params
