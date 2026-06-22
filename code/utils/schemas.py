"""
AIND data-schema generation and processing metadata utilities.
"""

import json
import platform
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import psutil
from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
    ProcessStage,
    ResourceTimestamped,
    ResourceUsage,
)
from aind_data_schema.core.quality_control import (
    QCMetric,
    QCStatus,
    QualityControl,
    Stage,
    Status,
)
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import MemoryUnit

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
    with open(raw_data_description_path, "r") as f:
        data = json.load(f)

    raw_dd = DataDescription.model_validate(data)
    derived = DataDescription.from_raw(raw_dd, process_name=process_name)

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


class ResourceMonitor:
    """Tracks CPU and RAM usage in a background thread."""

    def __init__(self, interval_seconds: Optional[float] = 1.0):
        self._interval = interval_seconds
        self._cpu_usage: List[ResourceTimestamped] = []
        self._ram_usage: List[ResourceTimestamped] = []
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            now = datetime.now(timezone.utc)
            self._cpu_usage.append(
                ResourceTimestamped(timestamp=now, usage=psutil.cpu_percent(interval=None))
            )
            self._ram_usage.append(
                ResourceTimestamped(timestamp=now, usage=psutil.virtual_memory().percent)
            )
            self._stop_event.wait(self._interval)

    def start(self) -> "ResourceMonitor":
        psutil.cpu_percent(interval=None)  # prime the first sample
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=self._interval + 1)

    def __enter__(self) -> "ResourceMonitor":
        return self.start()

    def __exit__(self, *exc_info) -> None:
        self.stop()

    def to_resource_usage(self, cpu_cores: Optional[int] = None) -> ResourceUsage:
        total_gb = round(psutil.virtual_memory().total / (1024**3), 2)
        return ResourceUsage(
            os=platform.system(),
            architecture=platform.machine(),
            cpu_cores=cpu_cores,
            system_memory=total_gb,
            system_memory_unit=MemoryUnit.GB,
            ram=total_gb,
            ram_unit=MemoryUnit.GB,
            cpu_usage=self._cpu_usage,
            ram_usage=self._ram_usage,
        )


def generate_processing(
    data_processes: List[DataProcess],
    dest_processing: PathLike,
    pipeline_name: str,
    pipeline_version: str,
    pipeline_url: str,
) -> None:
    """
    Writes a processing.json to dest_processing.
    """
    pipelines = [Code(url=pipeline_url, name=pipeline_name, version=pipeline_version)]
    processing = Processing.create_with_sequential_process_graph(
        data_processes=data_processes,
        pipelines=pipelines,
        notes="SmartSPIM light-sheet microscopy image processing pipeline",
    )
    processing.write_standard_file(output_directory=dest_processing)


def compile_processing_jsons(
    processing_paths: List[str],
    output_general_processing: str,
    pipeline_name: str,
    pipeline_version: str,
    pipeline_url: str,
) -> str:
    """
    Merges multiple processing.json files into one and writes the result.
    """
    data_processes = []
    for processing_path in processing_paths:
        curr_processing = read_json_as_dict(str(processing_path))
        print(f"Reading processing: {curr_processing}")
        curr_processing_obj = Processing.model_validate(curr_processing)

        for data_process in curr_processing_obj.data_processes:
            data_processes.append(data_process)

        msg = (
            f"Adding {len(curr_processing_obj.data_processes)} "
            f"processes from {curr_processing}"
        )
        print(msg)

    generate_processing(
        data_processes=data_processes,
        dest_processing=str(output_general_processing),
        pipeline_name=pipeline_name,
        pipeline_version=pipeline_version,
        pipeline_url=pipeline_url,
    )

    return str(output_general_processing)


def create_quality_control_metadata(
    qc_eval_values: List[Dict], output_path: str, time_zone: str = "America/Los_Angeles"
):
    """
    Creates and writes a quality_control.json from a list of QC evaluation dicts.
    """
    qc_metrics = []

    if len(qc_eval_values):
        curr_time = datetime.now(timezone.utc)
        stage_lookup = {item.value: item for item in Stage}
        status_lookup = {item.value: item for item in Status}

        for curr_qc_eval in qc_eval_values:
            qc_metric_values = curr_qc_eval.get("qc_metric_values", [])
            eval_stage = stage_lookup.get(curr_qc_eval.get("stage"))
            eval_name = curr_qc_eval.get("name", "")

            for curr_dict in qc_metric_values:
                qc_metrics.append(
                    QCMetric(
                        name=curr_dict.get("name", ""),
                        modality=Modality.SPIM,
                        stage=eval_stage,
                        description=curr_dict.get("desc"),
                        value=curr_dict.get("value", ""),
                        reference=curr_dict.get("reference"),
                        status_history=[
                            QCStatus(
                                evaluator="Automated",
                                status=status_lookup.get(curr_dict.get("status")),
                                timestamp=curr_time,
                            )
                        ],
                        tags={"evaluation": eval_name} if eval_name else {},
                    )
                )

    if qc_metrics:
        q = QualityControl(
            metrics=qc_metrics,
            default_grouping=["evaluation"],
        )
        q.write_standard_file(output_directory=output_path)
