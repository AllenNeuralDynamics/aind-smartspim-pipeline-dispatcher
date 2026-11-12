"""
AIND data-schema generation and processing metadata utilities.
"""

import json
import logging
import platform
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

import psutil
from aind_data_schema.components.identifiers import Code, Person
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription, Funding
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
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
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import MemoryUnit
from pydantic import ValidationError
from utils import metadata_compat
from utils.io import copy_file, read_json_as_dict

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]

# Maps a metadata filename to its aind-data-schema v2 core model for validation.
METADATA_MODEL_MAP = {
    "data_description.json": DataDescription,
    "acquisition.json": Acquisition,
    "instrument.json": Instrument,
    "subject.json": Subject,
    "procedures.json": Procedures,
    "processing.json": Processing,
}


def validate_metadata_v2(file_path: PathLike) -> bool:
    """
    Validates a metadata json file against its aind-data-schema v2 core model.

    Logs a clear error on failure but never raises (warn-and-continue).
    Filenames without a known v2 model (e.g. session.json) are skipped.

    Returns
    -------
    bool
        True if the file validates (or has no model to validate against),
        False if validation failed.
    """
    file_path = Path(file_path)
    model = METADATA_MODEL_MAP.get(file_path.name)

    if model is None:
        logger.info("No v2 model for %s; skipping validation", file_path.name)
        return True

    try:
        data = read_json_as_dict(str(file_path))
        model.model_validate(data)
        logger.info("%s passed v2 validation", file_path.name)
        return True
    except ValidationError as err:
        logger.error("%s failed v2 validation: %s", file_path.name, err)
        return False


def get_resolution(acquisition_config: dict) -> Tuple[float, float, float]:
    """
    Extracts voxel resolution (x, y, z) from an acquisition.json config.
    """
    return metadata_compat.get_voxel_resolution(acquisition_config)


def _build_raw_dd_from_v1(data: dict) -> DataDescription:
    """
    Reconstructs a v2 RAW ``DataDescription`` from a legacy v1
    data_description dict.

    Reads the v1 file as a plain dict and maps its fields onto the v2 schema
    (institution/funding registries -> Organization enums, fundee string ->
    list of Person, investigators PIDName -> Person, modality -> modalities).
    Mirrors the fallbacks of the pre-v2 implementation so messy inputs still
    produce a valid object.
    """
    # institution: v1 carries a dict with an "abbreviation"
    try:
        institution = Organization.from_abbreviation(data["institution"]["abbreviation"])
    except Exception:
        logger.warning("Could not resolve institution; defaulting to Allen Institute")
        institution = Organization.AI

    # investigators: v1 PIDName dicts -> v2 Person (name only)
    investigators = [
        Person(name=inv["name"]) for inv in data.get("investigators", []) if inv.get("name")
    ]
    if not investigators:
        investigators = [Person(name="Unknown")]

    # funding_source: registry dict -> Organization enum, fundee str -> [Person]
    funding_sources = []
    try:
        for fund in data.get("funding_source", []):
            fundee = fund.get("fundee")
            if isinstance(fundee, str) and fundee:
                fundee = [Person(name=n.strip()) for n in fundee.split(",")]
            elif not isinstance(fundee, list):
                fundee = None

            funding_sources.append(
                Funding(
                    funder=Organization.from_abbreviation(fund["funder"]["abbreviation"]),
                    grant_number=fund.get("grant_number"),
                    fundee=fundee,
                )
            )
    except Exception as e:
        logger.warning("Error parsing funding_source into the v2 schema: %s", e)
        funding_sources = []

    if not funding_sources:
        funding_sources = [Funding(funder=Organization.AI)]

    creation_time = data.get("creation_time") or datetime.now(timezone.utc)

    return DataDescription(
        data_level=DataLevel.RAW,
        name=data["name"],
        creation_time=creation_time,
        institution=institution,
        funding_source=funding_sources,
        investigators=investigators,
        modalities=[Modality.SPIM],
        project_name=data["project_name"],
        subject_id=data["subject_id"],
        group=data.get("group"),
        restrictions=data.get("restrictions"),
    )


def generate_data_description(
    raw_data_description_path,
    dest_data_description,
    process_name: Optional[str] = "stitched",
):
    """
    Generates a derived data_description.json in dest_data_description.

    Accepts both v2 (validated directly) and legacy v1 (reconstructed) raw
    data_description.json inputs. Returns the new derived dataset name.
    """
    with open(raw_data_description_path, "r") as f:
        data = json.load(f)

    try:
        raw_dd = DataDescription.model_validate(data)
    except ValidationError:
        logger.warning("Raw data_description is not valid v2; reconstructing from v1 fields")
        raw_dd = _build_raw_dd_from_v1(data)

    # Enforce SPIM — this is the SmartSPIM pipeline (applies to both paths)
    if [Modality.SPIM] != raw_dd.modalities:
        logger.warning("Overriding modalities %s -> [SPIM]", raw_dd.modalities)
        raw_dd.modalities = [Modality.SPIM]

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
    logger.info("Metadata files to copy: %s", files_to_copy)
    input_path = Path(input_path)
    output_path = Path(output_path)

    found_metadata = []

    for metadata_filename in files_to_copy:
        metadata_filename = input_path.joinpath(metadata_filename)

        if metadata_filename.exists():
            found_metadata.append(metadata_filename)
            output_filename = output_path.joinpath(metadata_filename.name)
            copy_file(metadata_filename, output_filename)

            # Validate against the v2 core model (warn-and-continue): the file
            # is copied regardless of validity.
            validate_metadata_v2(output_filename)

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


CHANNEL_PATTERN = re.compile(r"Ex_\d{3}_Em_\d{3}")


class ProcessingCompilationError(RuntimeError):
    """Raised when stage processing records cannot be compiled into a valid processing.json."""


@dataclass
class ProcessingStage:
    """
    A group of processing.json records that ran in parallel, e.g. one
    fusion record per channel.

    Parameters
    ----------
    paths: List[str]
        Processing records of this stage. The channel of each record is
        taken from its path (e.g. ``Ex_488_Em_525_processing.json``).
    upstream_process_types: Optional[Sequence[str]]
        Process types this stage depends on, matched per channel against
        every process merged so far (e.g. cell proposals depend on the
        "Image tile fusing" process of their channel). When None, each
        record depends on the same-channel record of the previous stage,
        or on every terminal process of the previous stage.
    """

    paths: List[str]
    upstream_process_types: Optional[Sequence[str]] = field(default=None)


@dataclass
class _MergedRecord:
    """Bookkeeping for a record already added to the merged graph."""

    channel: Optional[str]
    terminals: List[str]


def get_channel_name(text: str) -> Optional[str]:
    """Returns the last Ex_XXX_Em_XXX channel found in text, if any."""
    matches = CHANNEL_PATTERN.findall(str(text))
    return matches[-1] if matches else None


def _process_type_value(process: DataProcess) -> str:
    """Process type as a plain string (enum value or raw string)."""
    return getattr(process.process_type, "value", process.process_type)


def _rename_in_record(
    processes: List[DataProcess], graph: Dict[str, List[str]], old_name: str, new_name: str
) -> None:
    """Renames a process and every reference to it inside one record."""
    for process in processes:
        if process.name == old_name:
            process.name = new_name

    graph[new_name] = graph.pop(old_name, [])
    for dependencies in graph.values():
        dependencies[:] = [new_name if dep == old_name else dep for dep in dependencies]


def _unique_name(name: str, taken: set) -> str:
    """Appends the first free _N suffix, following Processing.__add__."""
    base_name = re.sub(r"_\d+$", "", name)
    new_name = name
    suffix = 1
    while new_name in taken:
        new_name = f"{base_name}_{suffix}"
        suffix += 1
    return new_name


def _resolve_upstream(
    channel: Optional[str],
    upstream_process_types: Optional[Sequence[str]],
    previous_stage: List[_MergedRecord],
    merged_processes: List[DataProcess],
    process_channels: Dict[str, Optional[str]],
) -> List[str]:
    """
    Returns the process names a record's root processes depend on.
    """
    if upstream_process_types:
        dependencies = []
        for process_type in upstream_process_types:
            process_type = getattr(process_type, "value", process_type)
            candidates = [p for p in merged_processes if _process_type_value(p) == process_type]
            same_channel = [p for p in candidates if process_channels[p.name] == channel]
            pool = same_channel or candidates

            if pool:
                latest = max(pool, key=lambda p: p.start_date_time)
                dependencies.append(latest.name)

        if dependencies:
            return dependencies

        logger.warning(
            "No upstream processes of types %s for channel %s; "
            "linking to the previous stage instead",
            list(upstream_process_types),
            channel,
        )

    same_channel = [r for r in previous_stage if channel and r.channel == channel]
    pool = same_channel or previous_stage
    return [name for record in pool for name in record.terminals]


def merge_processing_stages(
    stages: List[List[Tuple[Processing, Optional[str]]]],
    upstream_process_types: Optional[List[Optional[Sequence[str]]]] = None,
    pipelines: Optional[List[Code]] = None,
    notes: Optional[str] = None,
) -> Processing:
    """
    Merges stage processing records into one Processing object.

    Each record keeps its own dependency graph. Processes repeated across the
    records of a stage get their channel appended (e.g. "Image tile fusing -
    Ex_488_Em_525"); any other repeated name gets a _N suffix. The root
    processes of each record are linked to the previous stage (see
    ProcessingStage for the linking policy).

    Parameters
    ----------
    stages: List[List[Tuple[Processing, Optional[str]]]]
        Ordered stages, each a list of (record, channel) pairs.
    upstream_process_types: Optional[List[Optional[Sequence[str]]]]
        Per-stage process types to depend on. None links to the previous stage.
    pipelines: Optional[List[Code]]
        Pipelines that take precedence over the records' pipelines with the
        same name.
    notes: Optional[str]
        Notes for the merged record.

    Returns
    -------
    Processing
        The merged and validated processing record.

    Raises
    ------
    ProcessingCompilationError
        If the merged record does not validate.
    """
    merged_processes: List[DataProcess] = []
    merged_graph: Dict[str, List[str]] = {}
    process_channels: Dict[str, Optional[str]] = {}
    merged_pipelines: List[Code] = list(pipelines or [])
    previous_stage: List[_MergedRecord] = []

    for stage_index, stage in enumerate(stages):
        stage_upstream = upstream_process_types[stage_index] if upstream_process_types else None

        # Work on copies so the input records are never modified
        records = []
        for record, channel in stage:
            processes = [p.model_copy(deep=True) for p in record.data_processes]
            if record.dependency_graph is not None:
                graph = {name: list(deps) for name, deps in record.dependency_graph.items()}
            else:
                graph = {
                    p.name: ([processes[i - 1].name] if i else []) for i, p in enumerate(processes)
                }
            records.append((processes, graph, channel))

            for pipeline in record.pipelines or []:
                if pipeline.name not in {p.name for p in merged_pipelines}:
                    merged_pipelines.append(pipeline)

        name_counts: Dict[str, int] = {}
        for processes, _, _ in records:
            for process in processes:
                name_counts[process.name] = name_counts.get(process.name, 0) + 1

        current_stage: List[_MergedRecord] = []
        for processes, graph, channel in records:
            for name in [p.name for p in processes]:
                new_name = name
                if name_counts[name] > 1 and channel and channel not in name:
                    new_name = f"{name} - {channel}"
                new_name = _unique_name(new_name, set(merged_graph))
                if new_name != name:
                    _rename_in_record(processes, graph, name, new_name)

            names = [p.name for p in processes]
            dependents = {dep for deps in graph.values() for dep in deps}
            roots = [name for name in names if not graph.get(name)]
            terminals = [name for name in names if name not in dependents]

            upstream = _resolve_upstream(
                channel=channel,
                upstream_process_types=stage_upstream,
                previous_stage=previous_stage,
                merged_processes=merged_processes,
                process_channels=process_channels,
            )

            for name in names:
                merged_graph[name] = list(graph.get(name, []))
            for name in roots:
                merged_graph[name] = list(upstream)
            for process in processes:
                process_channels[process.name] = get_channel_name(process.name) or channel

            merged_processes.extend(processes)
            current_stage.append(_MergedRecord(channel=channel, terminals=terminals))

        previous_stage = current_stage

    try:
        return Processing(
            data_processes=merged_processes,
            dependency_graph=merged_graph,
            pipelines=merged_pipelines,
            notes=notes,
        )
    except ValidationError as err:
        raise ProcessingCompilationError(f"Merged processing record is invalid: {err}") from err


def load_processing_record(processing_path: PathLike) -> Processing:
    """
    Reads and validates one stage processing.json.

    Raises
    ------
    ProcessingCompilationError
        If the file is missing, empty or fails v2 Processing validation.
    """
    # read_json_as_dict returns {} when the path does not exist
    curr_processing = read_json_as_dict(str(processing_path))
    if not curr_processing:
        raise ProcessingCompilationError(f"Missing or empty processing.json: {processing_path}")

    try:
        return Processing.model_validate(curr_processing)
    except ValidationError as err:
        raise ProcessingCompilationError(
            f"{processing_path} failed v2 Processing validation: {err}"
        ) from err


def compile_processing_jsons(
    stages: Sequence[ProcessingStage],
    output_general_processing: str,
    pipeline_name: str,
    pipeline_version: str,
    pipeline_url: str,
) -> str:
    """
    Merges the stage processing.json files into one and writes the result.

    Parameters
    ----------
    stages: Sequence[ProcessingStage]
        Ordered stages; empty stages are skipped.
    output_general_processing: str
        Folder where the compiled processing.json is written.
    pipeline_name: str
        Pipeline name.
    pipeline_version: str
        Pipeline version.
    pipeline_url: str
        Pipeline repository URL.

    Returns
    -------
    str
        Folder containing the compiled processing.json.

    Raises
    ------
    ProcessingCompilationError
        If any record is missing, empty or invalid, or the compiled
        processing.json does not validate. All rejected paths are listed.
    """
    stages = [stage for stage in stages if stage.paths]
    errors = []
    loaded_stages = []

    for stage in stages:
        records = []
        for processing_path in stage.paths:
            try:
                record = load_processing_record(processing_path)
            except ProcessingCompilationError as err:
                errors.append(str(err))
                continue

            records.append((record, get_channel_name(processing_path)))
            logger.info(
                "Adding %d data processes from %s",
                len(record.data_processes),
                processing_path,
            )
        loaded_stages.append(records)

    if errors:
        raise ProcessingCompilationError(
            "Cannot compile processing.json; rejected records:\n - " + "\n - ".join(errors)
        )

    processing = merge_processing_stages(
        stages=loaded_stages,
        upstream_process_types=[stage.upstream_process_types for stage in stages],
        pipelines=[Code(url=pipeline_url, name=pipeline_name, version=pipeline_version)],
        notes="SmartSPIM light-sheet microscopy image processing pipeline",
    )
    processing.write_standard_file(output_directory=output_general_processing)

    output_file = Path(output_general_processing).joinpath("processing.json")
    if not validate_metadata_v2(output_file):
        raise ProcessingCompilationError(f"Compiled {output_file} failed v2 validation")

    logger.info(
        "Compiled %d data processes from %d stages into %s",
        len(processing.data_processes),
        len(stages),
        output_file,
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
