"""
AIND data-schema generation and processing metadata utilities.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pytz
from aind_data_schema.core.data_description import DerivedDataDescription, Funding
from aind_data_schema.core.processing import DataProcess, PipelineProcess, Processing
from aind_data_schema.core.quality_control import (
    QCEvaluation,
    QCMetric,
    QCStatus,
    QualityControl,
    Stage,
    Status,
)
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.pid_names import PIDName
from aind_data_schema_models.platforms import Platform
from pydantic import TypeAdapter

from utils.io import copy_file, read_json_as_dict

PathLike = Union[str, Path]


def get_resolution(acquisition_config: dict) -> Tuple[float, float, float]:
    """
    Extracts voxel resolution (x, y, z) from an acquisition.json config.
    """
    tile_coord_transforms = acquisition_config["tiles"][0]["coordinate_transformations"]
    scale_transform = [
        x["scale"] for x in tile_coord_transforms if x["type"] == "scale"
    ][0]
    x = float(scale_transform[0])
    y = float(scale_transform[1])
    z = float(scale_transform[2])
    return x, y, z


def generate_data_description(
    raw_data_description_path,
    dest_data_description,
    process_name: Optional[str] = "stitched",
):
    """
    Generates a derived data_description.json in dest_data_description.

    Returns the new derived dataset name.
    """
    f = open(raw_data_description_path, "r")
    data = json.load(f)

    if isinstance(data["institution"], dict) and "abbreviation" in data["institution"]:
        institution = data["institution"]["abbreviation"]

    investigators = data.get("investigators", [])

    if len(investigators) and len(investigators[0]):
        investigators = [PIDName.parse_obj(inv) for inv in investigators]
    else:
        investigators = [PIDName(name="Unknown")]

    funding_adapter = TypeAdapter(Funding)
    try:
        funding_sources = [
            funding_adapter.validate_python(fund) for fund in data["funding_source"]
        ]
    except Exception as e:
        print(f"Error getting the funding source into the schema!")
        funding_sources = []

    if not len(funding_sources):
        funding_sources = [Funding(funder=Organization.AI)]

    derived = DerivedDataDescription(
        creation_time=datetime.now(),
        input_data_name=data["name"],
        process_name=process_name,
        institution=Organization.from_abbreviation(institution),
        funding_source=funding_sources,
        group=data["group"],
        investigators=investigators,
        platform=Platform.SMARTSPIM,
        project_name=data["project_name"],
        restrictions=data["restrictions"],
        modality=[Modality.SPIM],
        subject_id=data["subject_id"],
    )

    with open(f"{dest_data_description}/data_description.json", "w") as f:
        f.write(derived.model_dump_json())

    return derived.name


def copy_available_metadata(
    input_path: PathLike, output_path: PathLike, files_to_copy: List[str]
) -> List[PathLike]:
    """
    Copies metadata files that exist in input_path to output_path.
    """
    print("Files to copy: ", files_to_copy)
    input_path = Path(input_path)
    output_path = Path(output_path)

    found_metadata = []

    for metadata_filename in files_to_copy:
        metadata_filename = input_path.joinpath(metadata_filename)

        if metadata_filename.exists():
            found_metadata.append(metadata_filename)
            output_filename = output_path.joinpath(metadata_filename.name)
            copy_file(metadata_filename, output_filename)

    return found_metadata


def generate_processing(
    data_processes: List[DataProcess],
    dest_processing: str,
    processor_full_name: str,
    pipeline_version: str,
    pipeline_notes: str,
) -> str:
    """
    Writes a processing.json to dest_processing.
    """
    processing_pipeline = PipelineProcess(
        data_processes=data_processes,
        processor_full_name=processor_full_name,
        pipeline_version=pipeline_version,
        pipeline_url="https://github.com/AllenNeuralDynamics/aind-smartspim-pipeline",
    )

    processing = Processing(
        processing_pipeline=processing_pipeline,
        notes=pipeline_notes,
    )

    print(f"Output compiled processing {processing} to {dest_processing}")
    processing.write_standard_file(output_directory=dest_processing)
    return dest_processing


def compile_processing_jsons(
    processing_paths: List[str],
    output_general_processing: str,
    processor_full_name: str,
    pipeline_version: str,
    pipeline_notes: str,
) -> str:
    """
    Merges multiple processing.json files into one and writes the result.
    """
    data_processes = []
    for processing_path in processing_paths:
        curr_processing = read_json_as_dict(str(processing_path))
        print(f"Reading processing: {curr_processing}")
        processing_adapter = TypeAdapter(Processing)
        curr_processing_obj = processing_adapter.validate_python(curr_processing)

        for data_process in curr_processing_obj.processing_pipeline.data_processes:
            data_processes.append(data_process)

        msg = (
            f"Adding {len(curr_processing_obj.processing_pipeline.data_processes)} "
            f"processes from {curr_processing}"
        )
        print(msg)

    output_filename = generate_processing(
        data_processes=data_processes,
        dest_processing=str(output_general_processing),
        processor_full_name=processor_full_name,
        pipeline_version=pipeline_version,
        pipeline_notes=pipeline_notes,
    )

    return output_filename


def create_quality_control_metadata(
    qc_eval_values: List[Dict], output_path: str, time_zone: str = "America/Los_Angeles"
):
    """
    Creates and writes a quality_control.json from a list of QC evaluation dicts.
    """
    qc_metrics = []

    if len(qc_eval_values):
        pst_timezone = pytz.timezone(time_zone)
        curr_time = datetime.now(pst_timezone)
        stage_lookup = {item.value: item for item in Stage}
        status_lookup = {item.value: item for item in Status}

        print("stage lookup: ", stage_lookup)
        evaluations = []
        for curr_qc_eval in qc_eval_values:
            qc_metric_values = curr_qc_eval.get("qc_metric_values")

            print(curr_qc_eval)
            qc_metrics = [
                QCMetric(
                    name=curr_dict.get("name", ""),
                    description=curr_dict.get("desc", ""),
                    value=curr_dict.get("value", ""),
                    reference=curr_dict.get("reference"),
                    status_history=[
                        QCStatus(
                            evaluator="Automated",
                            status=status_lookup.get(curr_dict.get("status")),
                            timestamp=curr_time,
                        )
                    ],
                )
                for curr_dict in qc_metric_values
            ]

            evaluations.append(
                QCEvaluation(
                    name=curr_qc_eval.get("name"),
                    description=curr_qc_eval.get("description"),
                    modality=Modality.SPIM,
                    stage=stage_lookup.get(curr_qc_eval.get("stage")),
                    metrics=qc_metrics,
                    notes=curr_qc_eval.get("notes", ""),
                    created=curr_time,
                )
            )

        if len(evaluations):
            q = QualityControl(evaluations=evaluations)
            serialized = q.model_dump_json()
            deserialized = QualityControl.model_validate_json(serialized)
            q.write_standard_file(output_directory=output_path)
