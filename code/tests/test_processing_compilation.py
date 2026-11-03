"""
Tests for stage-aware processing.json compilation in utils/schemas.py.

Fixtures under resources/processing/ are per-step processing records copied
from published stitched assets (863973, 868931, 869318) with the resource
usage samples emptied to keep them small. Process names, dependency graphs,
pipelines and timestamps are unchanged.
"""

import json
import sys
from glob import glob
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_STUBS = [
    "smartsheet_dataframe",
    "pytz",
    "dask",
    "dask.array",
    "dask.distributed",
    "aind_codeocean_api",
    "aind_codeocean_api.codeocean",
    "aind_codeocean_api.models",
    "aind_codeocean_api.models.data_assets_requests",
    "zarr",
    "boto3",
    "botocore",
    "botocore.exceptions",
    "requests",
]
for _mod in _STUBS:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, str(Path(__file__).parent.parent))

from aind_data_schema.core.processing import Processing  # noqa: E402
from aind_data_schema_models.process_names import ProcessName  # noqa: E402
from utils.schemas import (  # noqa: E402
    ProcessingCompilationError,
    ProcessingStage,
    compile_processing_jsons,
    get_channel_name,
    load_processing_record,
)

RESOURCES = Path(__file__).parent / "resources" / "processing"
PIPELINE = dict(
    pipeline_name="SmartSPIM Pipeline",
    pipeline_version="6.0.0",
    pipeline_url="https://github.com/AllenNeuralDynamics/aind-smartspim-pipeline-dispatcher",
)
STITCHING_863973 = "BigStitcher phase correlation + global optimization - Ex_639_Em_667"


def _dispatch_stages(asset: Path, ccf_channel: str = "Ex_639_Em_667"):
    """Stages in the order compile_dispatch_processing builds them."""
    metadata = asset / "image_tile_fusing" / "metadata"
    return [
        ProcessingStage([str(metadata / "flatfield_correction/metadata/processing.json")]),
        ProcessingStage([]),  # no destripe records
        ProcessingStage([str(metadata / "stitching/metadata/processing.json")]),
        ProcessingStage(sorted(glob(f"{metadata}/fusion/*_processing.json"))),
        ProcessingStage(
            [str(asset / f"image_atlas_alignment/{ccf_channel}/metadata/processing.json")]
        ),
    ]


def _cleanup_stages(asset: Path, root: str):
    """Stages in the order clean_up builds them."""
    seg = asset / "image_cell_segmentation"
    quant = asset / "image_cell_quantification"
    return [
        ProcessingStage([root]),
        ProcessingStage(
            sorted(glob(f"{seg}/Ex_*/proposals/metadata/processing.json")),
            upstream_process_types=[ProcessName.IMAGE_TILE_FUSING],
        ),
        ProcessingStage(sorted(glob(f"{seg}/Ex_*/metadata/processing.json"))),
        ProcessingStage(
            sorted(glob(f"{quant}/Ex_*/metadata/processing.json")),
            upstream_process_types=[
                ProcessName.IMAGE_CELL_CLASSIFICATION,
                ProcessName.IMAGE_ATLAS_ALIGNMENT,
            ],
        ),
    ]


def _compile(stages, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    compile_processing_jsons(stages=stages, output_general_processing=str(out_dir), **PIPELINE)
    return json.loads((out_dir / "processing.json").read_text())


@pytest.fixture
def compiled_863973(tmp_path):
    return _compile(_dispatch_stages(RESOURCES / "863973"), tmp_path / "out")


class TestGetChannelName:
    def test_fusion_record_path(self):
        assert get_channel_name("fused/Ex_488_Em_525_processing.json") == "Ex_488_Em_525"

    def test_last_channel_wins(self):
        assert get_channel_name("Ex_639_Em_667/cell_Ex_488_Em_525/x.json") == "Ex_488_Em_525"

    def test_no_channel(self):
        assert get_channel_name("stitching/metadata/processing.json") is None


class TestDispatchCompilation:
    def test_flatten_and_rebuild_fails_on_real_records(self):
        """Regression for dispatcher#40: the old approach rejects these records."""
        stages = _dispatch_stages(RESOURCES / "863973")
        processes = [
            process
            for stage in stages
            for path in stage.paths
            for process in load_processing_record(path).data_processes
        ]
        with pytest.raises(ValueError, match="unique names"):
            Processing.create_with_sequential_process_graph(data_processes=processes)

    def test_compiles_all_13_processes(self, compiled_863973):
        assert len(compiled_863973["data_processes"]) == 13
        Processing.model_validate(compiled_863973)

    def test_fusions_named_per_channel_and_parallel(self, compiled_863973):
        graph = compiled_863973["dependency_graph"]
        for channel in ["Ex_488_Em_525", "Ex_561_Em_593", "Ex_639_Em_667", "Ex_665_Em_700"]:
            assert graph[f"Image tile fusing - {channel}"] == [STITCHING_863973]
        assert "Image tile fusing" not in graph

    def test_registration_depends_on_its_channel_fusion(self, compiled_863973):
        graph = compiled_863973["dependency_graph"]
        assert graph["Image importing - Ex_639_Em_667"] == ["Image tile fusing - Ex_639_Em_667"]

    def test_record_graphs_are_preserved(self, compiled_863973):
        graph = compiled_863973["dependency_graph"]
        assert graph["Flatfield estimation - Ex_665_Em_700"] == []
        assert graph["Flatfield estimation - Ex_639_Em_667"] == [
            "Flatfield estimation - Ex_488_Em_525"
        ]
        assert graph[STITCHING_863973] == ["Flatfield estimation - Ex_639_Em_667"]
        assert graph["Reverse annotation alignment (CCF -> LS) - Ex_639_Em_667"] == [
            "OMEZarr conversion - Ex_639_Em_667"
        ]

    def test_single_pipeline_with_dispatcher_version(self, compiled_863973):
        assert [(p["name"], p["version"]) for p in compiled_863973["pipelines"]] == [
            ("SmartSPIM Pipeline", "6.0.0")
        ]

    def test_three_channel_asset(self, tmp_path):
        compiled = _compile(_dispatch_stages(RESOURCES / "868931"), tmp_path / "out")
        fusions = [
            p["name"]
            for p in compiled["data_processes"]
            if p["process_type"] == "Image tile fusing"
        ]
        assert sorted(fusions) == [
            "Image tile fusing - Ex_488_Em_525",
            "Image tile fusing - Ex_561_Em_593",
            "Image tile fusing - Ex_639_Em_667",
        ]
        Processing.model_validate(compiled)

    def test_input_records_are_not_modified(self, tmp_path):
        stages = _dispatch_stages(RESOURCES / "863973")
        before = {p: Path(p).read_text() for stage in stages for p in stage.paths}
        _compile(stages, tmp_path / "out")
        assert {p: Path(p).read_text() for p in before} == before


class TestCompilationFailures:
    def test_missing_record_raises_with_path(self, tmp_path):
        stages = _dispatch_stages(RESOURCES / "863973")
        missing = str(tmp_path / "fused" / "Ex_999_Em_999_processing.json")
        stages[3].paths.append(missing)
        with pytest.raises(ProcessingCompilationError, match="Ex_999_Em_999"):
            _compile(stages, tmp_path / "out")
        assert not (tmp_path / "out" / "processing.json").exists()

    def test_invalid_record_raises_with_path(self, tmp_path):
        invalid = tmp_path / "Ex_488_Em_525_processing.json"
        invalid.write_text(json.dumps({"data_processes": [{"name": "broken"}]}))
        stages = _dispatch_stages(RESOURCES / "863973")
        stages[3].paths[0] = str(invalid)
        with pytest.raises(ProcessingCompilationError, match=str(invalid)):
            _compile(stages, tmp_path / "out")

    def test_all_rejected_records_are_listed(self, tmp_path):
        stages = [
            ProcessingStage([str(tmp_path / "a" / "processing.json")]),
            ProcessingStage([str(tmp_path / "b" / "processing.json")]),
        ]
        with pytest.raises(ProcessingCompilationError) as err:
            _compile(stages, tmp_path / "out")
        assert "a/processing.json" in str(err.value)
        assert "b/processing.json" in str(err.value)


class TestCleanupCompilation:
    @pytest.fixture
    def compiled_869318(self, tmp_path):
        asset = RESOURCES / "869318"
        _compile(_dispatch_stages(asset, ccf_channel="Ex_639_Em_680"), tmp_path / "dispatch")
        root = tmp_path / "dispatch" / "processing.json"
        root_graph = json.loads(root.read_text())["dependency_graph"]
        compiled = _compile(_cleanup_stages(asset, str(root)), tmp_path / "cleanup")
        return root_graph, compiled

    def test_root_graph_edges_are_kept(self, compiled_869318):
        root_graph, compiled = compiled_869318
        for name, dependencies in root_graph.items():
            assert compiled["dependency_graph"][name] == dependencies

    def test_full_chain_is_present(self, compiled_869318):
        _, compiled = compiled_869318
        process_types = {p["process_type"] for p in compiled["data_processes"]}
        assert {
            "Image flat-field correction",
            "Image tile alignment",
            "Image tile fusing",
            "Image atlas alignment",
            "Image spot detection",
            "Image cell classification",
            "Image cell quantification",
        } <= process_types
        assert len(compiled["data_processes"]) == 17
        Processing.model_validate(compiled)

    def test_proposals_depend_on_channel_fusion(self, compiled_869318):
        _, compiled = compiled_869318
        graph = compiled["dependency_graph"]
        importing = next(name for name in graph if name.startswith("Image importing - /tmp/"))
        assert graph[importing] == ["Image tile fusing - Ex_488_Em_525"]

    def test_quantification_depends_on_classification_and_registration(self, compiled_869318):
        _, compiled = compiled_869318
        graph = compiled["dependency_graph"]
        assert graph["Image cell quantification - Ex_488_Em_525"] == [
            "Image cell classification - Ex_488_Em_525.zarr",
            "Atlas alignment using transforms - Ex_488_Em_525",
        ]

    def test_missing_root_record_raises(self, tmp_path):
        stages = _cleanup_stages(RESOURCES / "869318", str(tmp_path / "missing.json"))
        with pytest.raises(ProcessingCompilationError, match="missing.json"):
            _compile(stages, tmp_path / "cleanup")
