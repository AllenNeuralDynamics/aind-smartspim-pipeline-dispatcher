"""
Tests for cloud_mode=False (local / SLURM) transport branches.

These tests verify that:
  - No ``aws`` CLI commands are issued when cloud_mode=False.
  - Appropriate ``cp`` / ``mv`` shell commands are issued instead.
  - handle_split_channels discovers channels from the local filesystem
    and writes JSON manifests with local paths.
"""

import json
import logging
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Stub optional cloud / heavy deps before importing local modules ────────────
_STUBS = [
    "smartsheet_dataframe",
    "pytz",
    "dask",
    "dask.array",
    "aind_data_schema",
    "aind_data_schema.core",
    "aind_data_schema.core.data_description",
    "aind_data_schema.core.processing",
    "aind_data_schema.core.quality_control",
    "aind_data_schema_models",
    "aind_data_schema_models.modalities",
    "aind_data_schema_models.organizations",
    "aind_data_schema_models.pid_names",
    "aind_data_schema_models.platforms",
]
for _mod in _STUBS:
    sys.modules.setdefault(_mod, MagicMock())

from modes.cleanup import clean_up  # noqa: E402
from modes.dispatch import copy_intermediate_data  # noqa: E402
from modes.postprocess import copy_postprocessed_data  # noqa: E402
from modes.split_channels import handle_split_channels  # noqa: E402

LOG = logging.getLogger("test")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _capture_commands():
    """Return (side_effect, issued_commands_list) for execute_command_helper."""
    issued = []

    def _side_effect(cmd):
        issued.append(cmd)
        return iter([])

    return _side_effect, issued


def _is_aws_cmd(cmd: str) -> bool:
    """True when the shell command itself is an aws CLI call (not just a path that mentions aws)."""
    return cmd.lstrip().startswith("aws ")


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def dispatch_inputs(tmp_path):
    """Minimal directory tree expected by copy_intermediate_data."""
    meta = tmp_path / "output_metadata"
    meta.mkdir()

    flatfield = tmp_path / "flatfield"
    (flatfield / "metadata").mkdir(parents=True)

    stitch = tmp_path / "stitched"
    (stitch / "metadata").mkdir(parents=True)

    fuse = tmp_path / "fused"
    fuse.mkdir()
    (fuse / "Ex_561_Em_600.zarr").mkdir()
    (fuse / "fuse_processing.json").write_text("{}")

    ccf = tmp_path / "ccf_Ex_561_Em_600"
    (ccf / "metadata").mkdir(parents=True)

    results = tmp_path / "results"
    results.mkdir()

    dest = str(tmp_path / "dest")

    return dict(
        output_dispatch_metadata=meta,
        flatfield_folder=flatfield,
        destripe_files=[],
        stitch_folder=stitch,
        fuse_folder=fuse,
        ccf_folders=[str(ccf)],
        s3_path=dest,
        results_folder=results,
        logger=LOG,
    )


@pytest.fixture
def cleanup_inputs(tmp_path):
    """Minimal directory tree expected by clean_up."""
    # input_aind_metadata/processing.json — counted as processing path #1
    meta = tmp_path / "input_aind_metadata"
    meta.mkdir()
    (meta / "processing.json").write_text("{}")

    # Cell folder with processing JSONs (gives >1 total processing paths)
    cell = tmp_path / "cell_Ex_561_Em_600"
    (cell / "metadata").mkdir(parents=True)
    (cell / "metadata" / "processing.json").write_text("{}")
    (cell / "proposals" / "metadata").mkdir(parents=True)
    (cell / "proposals" / "metadata" / "processing.json").write_text("{}")

    # Quant folder
    quant = tmp_path / "quant_Ex_561_Em_600"
    (quant / "metadata").mkdir(parents=True)
    (quant / "metadata" / "processing.json").write_text("{}")

    results = tmp_path / "results"
    results.mkdir()
    dest = tmp_path / "dest"

    manifest = {
        "name": "TestDataset",
        "pipeline_processing": {"stitching": {"s3_path": str(dest)}},
    }
    return dict(
        data_folder=tmp_path,
        results_folder=results,
        manifest=manifest,
        dest=dest,
    )


@pytest.fixture
def postprocess_inputs(tmp_path):
    """Minimal directory tree expected by copy_postprocessed_data."""
    meta = tmp_path / "output_metadata"
    meta.mkdir()

    fuse = tmp_path / "image_tile_fusing"
    fuse.mkdir()

    ccf = tmp_path / "ccf_Ex_561_Em_600"
    (ccf / "metadata").mkdir(parents=True)

    cell = tmp_path / "cell_Ex_561_Em_600"
    cell.mkdir()

    quant = tmp_path / "quant_Ex_561_Em_600"
    quant.mkdir()

    results = tmp_path / "results"
    results.mkdir()
    proc_json = results / "processing.json"
    proc_json.write_text("{}")

    dest = str(tmp_path / "dest")

    return dict(
        post_fuse_folder=fuse,
        output_dispatch_metadata=meta,
        ccf_folders=[ccf],
        cell_folders=[cell],
        quantification_folders=[quant],
        s3_path=dest,
        results_folder=results,
        new_processing_path=str(proc_json),
        logger=LOG,
    )


# ── Fixtures with real file content (for folder-structure tests) ───────────────

@pytest.fixture
def dispatch_inputs_real(tmp_path):
    """Like dispatch_inputs but with real files so cp/mv shell globs expand."""
    meta = tmp_path / "output_metadata"
    meta.mkdir()
    (meta / "data_description.json").write_text("{}")   # cp *.json {dest}/

    flatfield = tmp_path / "flatfield"
    (flatfield / "metadata").mkdir(parents=True)
    (flatfield / "flat_data.tif").write_bytes(b"tif")   # carried by cp -r

    stitch = tmp_path / "stitched"
    (stitch / "metadata").mkdir(parents=True)
    (stitch / "stitch_params.json").write_text("{}")    # carried by cp -r

    fuse = tmp_path / "fused"
    fuse.mkdir()
    zarr = fuse / "Ex_561_Em_600.zarr"
    zarr.mkdir()
    (zarr / "chunk.bin").write_bytes(b"zarr")           # carried by cp -r zarr
    (fuse / "fuse_processing.json").write_text("{}")    # carried by cp *.json

    ccf = tmp_path / "ccf_Ex_561_Em_600"
    ccf_meta = ccf / "metadata"
    ccf_meta.mkdir(parents=True)
    (ccf_meta / "ccf_proc.json").write_text("{}")       # moved by mv ccf/*

    results = tmp_path / "results"
    results.mkdir()

    dest = str(tmp_path / "dest")

    return dict(
        output_dispatch_metadata=meta,
        flatfield_folder=flatfield,
        destripe_files=[],
        stitch_folder=stitch,
        fuse_folder=fuse,
        ccf_folders=[str(ccf)],
        s3_path=dest,
        results_folder=results,
        logger=LOG,
    )


@pytest.fixture
def cleanup_inputs_real(tmp_path):
    """Like cleanup_inputs but with real files so mv globs expand."""
    meta = tmp_path / "input_aind_metadata"
    meta.mkdir()
    (meta / "processing.json").write_text("{}")

    cell = tmp_path / "cell_Ex_561_Em_600"
    (cell / "metadata").mkdir(parents=True)
    (cell / "metadata" / "processing.json").write_text("{}")
    (cell / "proposals" / "metadata").mkdir(parents=True)
    (cell / "proposals" / "metadata" / "processing.json").write_text("{}")
    (cell / "cells.csv").write_text("id,x,y,z")         # moved by mv cell/*

    quant = tmp_path / "quant_Ex_561_Em_600"
    (quant / "metadata").mkdir(parents=True)
    (quant / "metadata" / "processing.json").write_text("{}")
    (quant / "quant.csv").write_text("id,count")        # moved by mv quant/*

    results = tmp_path / "results"
    results.mkdir()
    (results / "processing.json").write_text("{}")      # cp {results}/processing.json {dest}/

    dest = tmp_path / "dest"

    manifest = {
        "name": "TestDataset",
        "pipeline_processing": {"stitching": {"s3_path": str(dest)}},
    }
    return dict(
        data_folder=tmp_path,
        results_folder=results,
        manifest=manifest,
        dest=dest,
    )


@pytest.fixture
def postprocess_inputs_real(tmp_path):
    """Like postprocess_inputs but with real files so mv globs expand."""
    meta = tmp_path / "output_metadata"
    meta.mkdir()
    (meta / "data_description.json").write_text("{}")   # carried by cp -r

    fuse = tmp_path / "image_tile_fusing"
    fuse.mkdir()
    (fuse / "fuse_data.bin").write_bytes(b"fuse")       # carried by cp -r

    ccf = tmp_path / "ccf_Ex_561_Em_600"
    (ccf / "metadata").mkdir(parents=True)
    (ccf / "ccf_result.json").write_text("{}")          # moved by mv ccf/*

    cell = tmp_path / "cell_Ex_561_Em_600"
    cell.mkdir()
    (cell / "cell_result.csv").write_text("id,x")      # moved by mv cell/*

    quant = tmp_path / "quant_Ex_561_Em_600"
    quant.mkdir()
    (quant / "quant_result.csv").write_text("id,n")    # moved by mv quant/*

    results = tmp_path / "results"
    results.mkdir()
    proc_json = results / "processing.json"
    proc_json.write_text("{}")

    dest = str(tmp_path / "dest")

    return dict(
        post_fuse_folder=fuse,
        output_dispatch_metadata=meta,
        ccf_folders=[ccf],
        cell_folders=[cell],
        quantification_folders=[quant],
        s3_path=dest,
        results_folder=results,
        new_processing_path=str(proc_json),
        logger=LOG,
    )


# ── copy_intermediate_data ─────────────────────────────────────────────────────

class TestCopyIntermediateDataLocal:
    def _run(self, dispatch_inputs, cloud_mode=False):
        side_effect, issued = _capture_commands()
        with ExitStack() as stack:
            stack.enter_context(
                patch("utils.utils.execute_command_helper", side_effect=side_effect)
            )
            stack.enter_context(
                patch("utils.utils.compile_processing_jsons", return_value=None)
            )
            stack.enter_context(patch("utils.utils.save_string_to_txt"))
            copy_intermediate_data(**dispatch_inputs, cloud_mode=cloud_mode)
        return issued

    def test_no_aws_commands(self, dispatch_inputs):
        issued = self._run(dispatch_inputs)
        aws_cmds = [c for c in issued if _is_aws_cmd(c)]
        assert aws_cmds == [], f"Unexpected aws commands: {aws_cmds}"

    def test_uses_cp_for_metadata(self, dispatch_inputs):
        issued = self._run(dispatch_inputs)
        cp_cmds = [c for c in issued if c.startswith("cp")]
        assert cp_cmds, "Expected at least one cp command for metadata"

    def test_uses_mv_for_ccf(self, dispatch_inputs):
        issued = self._run(dispatch_inputs)
        mv_cmds = [c for c in issued if c.startswith("mv")]
        assert mv_cmds, "Expected mv command for CCF folder"
        assert "Ex_561_Em_600" in mv_cmds[0]

    def test_dest_dirs_created(self, dispatch_inputs, tmp_path):
        self._run(dispatch_inputs)
        dest = tmp_path / "dest"
        assert (dest / "image_tile_fusing" / "metadata").is_dir()
        assert (dest / "image_tile_fusing" / "OMEZarr").is_dir()

    def test_cloud_mode_uses_aws(self, dispatch_inputs):
        """Regression: cloud mode must still use aws s3."""
        issued = self._run(dispatch_inputs, cloud_mode=True)
        aws_cmds = [c for c in issued if _is_aws_cmd(c)]
        assert aws_cmds, "Expected aws s3 commands in cloud mode"
        plain_cp = [c for c in issued if c.startswith("cp ") or c.startswith("mv ")]
        assert plain_cp == [], f"Local cp/mv commands must not appear in cloud mode: {plain_cp}"

    def test_output_folder_structure(self, dispatch_inputs_real, tmp_path):
        """Verify the exact output directory tree produced by the local branch."""
        with ExitStack() as stack:
            stack.enter_context(
                patch("utils.utils.compile_processing_jsons", return_value=None)
            )
            copy_intermediate_data(**dispatch_inputs_real, cloud_mode=False)

        dest = tmp_path / "dest"
        # top-level metadata JSON copied from output_dispatch_metadata
        assert (dest / "data_description.json").is_file()
        # flatfield carried into fusion metadata tree
        assert (dest / "image_tile_fusing" / "metadata" / "flatfield_correction" / "flat_data.tif").is_file()
        # fused per-zarr-channel directory
        assert (dest / "image_tile_fusing" / "OMEZarr" / "Ex_561_Em_600.zarr" / "chunk.bin").is_file()
        # fused JSON metadata (*.json from fuse_folder)
        assert (dest / "image_tile_fusing" / "metadata" / "fusion" / "fuse_processing.json").is_file()
        # stitch metadata
        assert (dest / "image_tile_fusing" / "metadata" / "stitching" / "stitch_params.json").is_file()
        # CCF registration folder moved (not copied) to image_atlas_alignment
        assert (dest / "image_atlas_alignment" / "Ex_561_Em_600" / "metadata" / "ccf_proc.json").is_file()


# ── clean_up ───────────────────────────────────────────────────────────────────

class TestCleanUpLocal:
    def _run(self, cleanup_inputs, cloud_mode=False):
        side_effect, issued = _capture_commands()
        d = cleanup_inputs
        with ExitStack() as stack:
            stack.enter_context(
                patch("utils.utils.execute_command_helper", side_effect=side_effect)
            )
            stack.enter_context(
                patch(
                    "utils.utils.compile_processing_jsons",
                    return_value=d["results_folder"],
                )
            )
            stack.enter_context(patch("utils.utils.AlertBot"))
            stack.enter_context(patch("utils.utils.save_string_to_txt"))
            clean_up(
                processing_manifest=d["manifest"],
                data_folder=d["data_folder"],
                results_folder=d["results_folder"],
                alert_bot_link="",
                cloud_mode=cloud_mode,
            )
        return issued

    def test_no_aws_commands(self, cleanup_inputs):
        issued = self._run(cleanup_inputs)
        aws_cmds = [c for c in issued if _is_aws_cmd(c)]
        assert aws_cmds == [], f"Unexpected aws commands: {aws_cmds}"

    def test_uses_mv_for_cell_folders(self, cleanup_inputs):
        issued = self._run(cleanup_inputs)
        mv_cmds = [c for c in issued if "mv" in c and "cell_" in c]
        assert mv_cmds, "Expected mv command for cell folder"

    def test_uses_mv_for_quant_folders(self, cleanup_inputs):
        issued = self._run(cleanup_inputs)
        mv_cmds = [c for c in issued if "mv" in c and "quant_" in c]
        assert mv_cmds, "Expected mv command for quantification folder"

    def test_uses_cp_for_processing_json(self, cleanup_inputs):
        issued = self._run(cleanup_inputs)
        cp_cmds = [c for c in issued if "cp" in c and "processing.json" in c]
        assert cp_cmds, "Expected cp command for processing.json"

    def test_dest_dirs_created(self, cleanup_inputs):
        self._run(cleanup_inputs)
        dest = cleanup_inputs["dest"]
        assert (dest / "image_cell_segmentation" / "Ex_561_Em_600").is_dir()
        assert (dest / "image_cell_quantification" / "Ex_561_Em_600").is_dir()

    def test_cloud_mode_uses_aws(self, cleanup_inputs):
        issued = self._run(cleanup_inputs, cloud_mode=True)
        aws_cmds = [c for c in issued if _is_aws_cmd(c)]
        assert aws_cmds, "Expected aws s3 mv/cp commands in cloud mode"

    def test_output_folder_structure(self, cleanup_inputs_real):
        """Verify the exact output directory tree produced by the local branch."""
        d = cleanup_inputs_real
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "utils.utils.compile_processing_jsons",
                    return_value=d["results_folder"],
                )
            )
            stack.enter_context(patch("utils.utils.AlertBot"))
            clean_up(
                processing_manifest=d["manifest"],
                data_folder=d["data_folder"],
                results_folder=d["results_folder"],
                alert_bot_link="",
                cloud_mode=False,
            )

        dest = d["dest"]
        # processing.json copied to dataset root
        assert (dest / "processing.json").is_file()
        # cell segmentation contents moved to per-channel subdirectory
        seg = dest / "image_cell_segmentation" / "Ex_561_Em_600"
        assert (seg / "cells.csv").is_file()
        assert (seg / "metadata" / "processing.json").is_file()
        assert (seg / "proposals" / "metadata" / "processing.json").is_file()
        # quantification contents moved to per-channel subdirectory
        quant_dest = dest / "image_cell_quantification" / "Ex_561_Em_600"
        assert (quant_dest / "quant.csv").is_file()
        assert (quant_dest / "metadata" / "processing.json").is_file()


# ── copy_postprocessed_data ────────────────────────────────────────────────────

class TestCopyPostprocessedDataLocal:
    def _run(self, postprocess_inputs, cloud_mode=False):
        side_effect, issued = _capture_commands()
        with ExitStack() as stack:
            stack.enter_context(
                patch("utils.utils.execute_command_helper", side_effect=side_effect)
            )
            stack.enter_context(patch("utils.utils.save_string_to_txt"))
            copy_postprocessed_data(**postprocess_inputs, cloud_mode=cloud_mode)
        return issued

    def test_no_aws_commands(self, postprocess_inputs):
        issued = self._run(postprocess_inputs)
        aws_cmds = [c for c in issued if _is_aws_cmd(c)]
        assert aws_cmds == [], f"Unexpected aws commands: {aws_cmds}"

    def test_uses_cp_for_metadata(self, postprocess_inputs):
        issued = self._run(postprocess_inputs)
        cp_cmds = [c for c in issued if "cp" in c]
        assert cp_cmds, "Expected cp command for metadata / fuse folder"

    def test_uses_mv_for_ccf(self, postprocess_inputs):
        issued = self._run(postprocess_inputs)
        mv_cmds = [c for c in issued if "mv" in c and "ccf" in c.lower()]
        assert mv_cmds, "Expected mv command for CCF folder"

    def test_skips_when_no_folders(self, postprocess_inputs):
        side_effect, issued = _capture_commands()
        with patch("utils.utils.execute_command_helper", side_effect=side_effect):
            copy_postprocessed_data(
                post_fuse_folder=postprocess_inputs["post_fuse_folder"],
                output_dispatch_metadata=postprocess_inputs["output_dispatch_metadata"],
                ccf_folders=[],
                cell_folders=[],
                quantification_folders=[],
                s3_path=postprocess_inputs["s3_path"],
                results_folder=postprocess_inputs["results_folder"],
                new_processing_path=postprocess_inputs["new_processing_path"],
                logger=LOG,
                cloud_mode=False,
            )
        assert issued == [], "No commands should be issued when there are no folders"

    def test_cloud_mode_uses_aws(self, postprocess_inputs):
        side_effect, issued = _capture_commands()
        with ExitStack() as stack:
            stack.enter_context(
                patch("utils.utils.execute_command_helper", side_effect=side_effect)
            )
            stack.enter_context(patch("utils.utils.save_string_to_txt"))
            copy_postprocessed_data(**postprocess_inputs, cloud_mode=True)
        aws_cmds = [c for c in issued if _is_aws_cmd(c)]
        assert aws_cmds, "Expected aws s3 commands in cloud mode"

    def test_output_folder_structure(self, postprocess_inputs_real, tmp_path):
        """Verify the exact output directory tree produced by the local branch."""
        copy_postprocessed_data(**postprocess_inputs_real, cloud_mode=False)

        dest = tmp_path / "dest"
        # metadata directory contents copied to dataset root
        assert (dest / "data_description.json").is_file()
        # processing.json copied to root
        assert (dest / "processing.json").is_file()
        # CCF folder contents moved to per-channel subdirectory
        assert (dest / "image_atlas_alignment" / "Ex_561_Em_600" / "ccf_result.json").is_file()
        # cell segmentation contents moved
        assert (dest / "image_cell_segmentation" / "Ex_561_Em_600" / "cell_result.csv").is_file()
        # quantification contents moved
        assert (dest / "image_cell_quantification" / "Ex_561_Em_600" / "quant_result.csv").is_file()
        # fuse folder contents copied
        assert (dest / "image_tile_fusing" / "fuse_data.bin").is_file()


# ── handle_split_channels ─────────────────────────────────────────────────────

class TestHandleSplitChannelsLocal:
    @pytest.fixture
    def spim_tree(self, tmp_path):
        dataset_name = "SmartSPIM_1234_2024-01-01_00-00-00"
        spim = tmp_path / "output" / dataset_name / "SPIM"
        (spim / "Ex_561_Em_600").mkdir(parents=True)
        (spim / "Ex_488_Em_525").mkdir(parents=True)
        (spim / "not_a_channel").mkdir()  # must be excluded
        results = tmp_path / "results"
        results.mkdir()
        data = tmp_path / "data"
        data.mkdir()
        return dict(
            output_path=str(tmp_path / "output"),
            dataset_name=dataset_name,
            results=results,
            data=data,
        )

    def test_writes_json_for_each_channel(self, spim_tree):
        with patch(
            "modes.split_channels.get_data_config",
            return_value=({}, spim_tree["dataset_name"], []),
        ):
            handle_split_channels(
                data_folder=spim_tree["data"],
                results_folder=spim_tree["results"],
                output_path=spim_tree["output_path"],
                logger=LOG,
                cloud_mode=False,
            )

        written = list(spim_tree["results"].glob("preprocess_*.json"))
        channel_names = {p.stem.replace("preprocess_", "") for p in written}
        assert channel_names == {"Ex_561_Em_600", "Ex_488_Em_525"}

    def test_json_contains_local_input_data_path(self, spim_tree):
        with patch(
            "modes.split_channels.get_data_config",
            return_value=({}, spim_tree["dataset_name"], []),
        ):
            handle_split_channels(
                data_folder=spim_tree["data"],
                results_folder=spim_tree["results"],
                output_path=spim_tree["output_path"],
                logger=LOG,
                cloud_mode=False,
            )

        for json_file in spim_tree["results"].glob("preprocess_*.json"):
            data = json.loads(json_file.read_text())
            assert "s3://" not in data["input_data"], (
                "Local mode must not produce S3 paths; got {}".format(data["input_data"])
            )
            assert spim_tree["output_path"] in data["input_data"]

    def test_excludes_non_channel_dirs(self, spim_tree):
        with patch(
            "modes.split_channels.get_data_config",
            return_value=({}, spim_tree["dataset_name"], []),
        ):
            handle_split_channels(
                data_folder=spim_tree["data"],
                results_folder=spim_tree["results"],
                output_path=spim_tree["output_path"],
                logger=LOG,
                cloud_mode=False,
            )

        written = list(spim_tree["results"].glob("preprocess_*.json"))
        assert not any("not_a_channel" in p.name for p in written)

    def test_skips_gracefully_when_spim_missing(self, tmp_path):
        results = tmp_path / "results"
        results.mkdir()
        with patch(
            "modes.split_channels.get_data_config",
            return_value=({}, "some_dataset", []),
        ):
            # Must not raise even though SPIM dir doesn't exist
            handle_split_channels(
                data_folder=tmp_path / "data",
                results_folder=results,
                output_path=str(tmp_path / "output"),
                logger=LOG,
                cloud_mode=False,
            )
        assert list(results.glob("preprocess_*.json")) == []

    def test_cloud_mode_calls_list_s3_folders(self, spim_tree):
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "modes.split_channels.get_data_config",
                    return_value=({}, spim_tree["dataset_name"], []),
                )
            )
            mock_list = stack.enter_context(
                patch(
                    "utils.utils.list_s3_folders",
                    return_value=["Ex_561_Em_600", "other"],
                )
            )
            handle_split_channels(
                data_folder=spim_tree["data"],
                results_folder=spim_tree["results"],
                output_path="my-bucket",
                logger=LOG,
                cloud_mode=True,
            )
        mock_list.assert_called_once()
