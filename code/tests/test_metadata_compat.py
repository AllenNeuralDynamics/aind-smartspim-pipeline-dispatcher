"""Tests for the v1/v2 metadata compatibility readers."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Stub optional deps transitively needed by utils/__init__.py that may not
# be installed in the test environment.
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

import json  # noqa: E402

import pytest  # noqa: E402
from utils.metadata_compat import (  # noqa: E402
    get_acquisition_axes,
    get_voxel_resolution,
    normalize_orientation,
)

# Trimmed v1 acquisition.json (schema 1.x): top-level axes with explicit
# dimension, voxel scale in tiles[].coordinate_transformations (X, Y, Z)
V1_ACQUISITION = {
    "schema_version": "1.0.1",
    "axes": [
        {"dimension": 0, "direction": "Right_to_left", "name": "Z", "unit": "micrometer"},
        {"dimension": 1, "direction": "Anterior_to_posterior", "name": "Y", "unit": "micrometer"},
        {"dimension": 2, "direction": "Superior_to_inferior", "name": "X", "unit": "micrometer"},
    ],
    "tiles": [
        {
            "coordinate_transformations": [
                {"type": "translation", "translation": ["37677.0", "15183.0", "-315.0"]},
                {"type": "scale", "scale": ["1.8", "1.8", "2.0"]},
            ],
            "file_name": "Ex_639_Em_680/376770/376770_151830/",
        }
    ],
}

# Trimmed v2 acquisition.json (schema 2.x): axes inside the imaging config's
# coordinate system (list order = dimension), voxel scale in
# images[].image_to_acquisition_transform ordered following those axes
V2_ACQUISITION = {
    "schema_version": "2.4.0",
    "coordinate_system": None,
    "data_streams": [
        {
            "configurations": [
                {
                    "object_type": "Imaging config",
                    "coordinate_system": {
                        "object_type": "Coordinate system",
                        "name": "SPIM_LPI",
                        "axes": [
                            {"direction": "Right_to_left", "name": "Z", "object_type": "Axis"},
                            {
                                "direction": "Anterior_to_posterior",
                                "name": "Y",
                                "object_type": "Axis",
                            },
                            {
                                "direction": "Superior_to_inferior",
                                "name": "X",
                                "object_type": "Axis",
                            },
                        ],
                    },
                    "images": [
                        {
                            "object_type": "Image spim",
                            "channel_name": "Ex_488_Em_525_left",
                            "image_to_acquisition_transform": [
                                {"object_type": "Scale", "scale": [2.0, 1.8, 1.8]},
                                {
                                    "object_type": "Translation",
                                    "translation": [-109.9, 14026.0, 36903.0],
                                },
                            ],
                        }
                    ],
                },
                {"object_type": "Sample chamber config"},
            ]
        }
    ],
}

EXPECTED_AXES = [
    ("Z", 0, "Right_to_left"),
    ("Y", 1, "Anterior_to_posterior"),
    ("X", 2, "Superior_to_inferior"),
]


class TestGetVoxelResolution:
    def test_v1_returns_xyz(self):
        assert get_voxel_resolution(V1_ACQUISITION) == (1.8, 1.8, 2.0)

    def test_v2_reorders_axes_scale_to_xyz(self):
        assert get_voxel_resolution(V2_ACQUISITION) == (1.8, 1.8, 2.0)

    def test_unknown_shape_raises(self):
        with pytest.raises(ValueError):
            get_voxel_resolution({"schema_version": "2.4.0"})


class TestGetAcquisitionAxes:
    def test_v1_axes_pass_through(self):
        assert get_acquisition_axes(V1_ACQUISITION) is V1_ACQUISITION["axes"]

    def test_v2_axes_get_dimension_from_list_order(self):
        axes = get_acquisition_axes(V2_ACQUISITION)
        assert [(a["name"], a["dimension"], a["direction"]) for a in axes] == EXPECTED_AXES

    def test_v1_and_v2_normalize_identically(self):
        v1_axes = [
            (a["name"], a["dimension"], a["direction"])
            for a in get_acquisition_axes(V1_ACQUISITION)
        ]
        v2_axes = [
            (a["name"], a["dimension"], a["direction"])
            for a in get_acquisition_axes(V2_ACQUISITION)
        ]
        assert v1_axes == v2_axes == EXPECTED_AXES

    def test_prelim_acquisition_blob_passes_through(self):
        blob = {"axes": [{"name": "Z", "dimension": 0, "direction": "Left_to_right"}]}
        assert get_acquisition_axes(blob) == blob["axes"]

    def test_missing_axes_raises(self):
        with pytest.raises(ValueError):
            get_acquisition_axes({"schema_version": "2.4.0"})


class TestNormalizeOrientation:
    def test_returns_v1_shaped_dict(self):
        normalized = normalize_orientation(V2_ACQUISITION)
        assert set(normalized.keys()) == {"axes"}
        assert normalized["axes"] == get_acquisition_axes(V2_ACQUISITION)


class TestVolumeOrientationCompat:
    """volume_orientation must accept v1 axes blobs and full v2 acquisitions."""

    def _spr_axes(self):
        return [
            {"dimension": 0, "direction": "Superior_to_inferior", "name": "Z"},
            {"dimension": 1, "direction": "Posterior_to_anterior", "name": "Y"},
            {"dimension": 2, "direction": "Right_to_left", "name": "X"},
        ]

    def test_v1_blob_and_v2_acquisition_give_same_quaternion(self):
        from utils.visualization import volume_orientation

        v1_blob = {"axes": self._spr_axes()}
        v2_acq = json.loads(json.dumps(V2_ACQUISITION))
        v2_axes = v2_acq["data_streams"][0]["configurations"][0]["coordinate_system"]["axes"]
        for axis, ref in zip(v2_axes, self._spr_axes()):
            axis["direction"] = ref["direction"]

        assert volume_orientation(v1_blob) == volume_orientation(v2_acq) == [0.5, 0.5, 0.5, -0.5]


class TestGetDatasetStepVersions:
    """versioning.get_dataset_step_versions must read v1 and v2 processing.json."""

    V1_PROCESSING = {
        "processing_pipeline": {
            "data_processes": [
                {
                    "name": "Image importing",
                    "code_url": "https://github.com/AllenNeuralDynamics/aind-smartspim-stitch",
                    "software_version": "1.2.9",
                }
            ]
        }
    }

    V2_PROCESSING = {
        "data_processes": [
            {
                "name": "Compression",
                "process_type": "Compression",
                "object_type": "Data process",
                "code": {
                    "object_type": "Code",
                    "url": "ghcr.io/allenneuraldynamics/aind-smartspim-data-transformation",
                    "version": "0.1.9",
                },
            }
        ]
    }

    def _run(self, tmp_path, processing_dict):
        from utils.versioning import get_dataset_step_versions

        (tmp_path / "processing.json").write_text(json.dumps(processing_dict))
        return get_dataset_step_versions(tmp_path)

    def test_v1_processing(self, tmp_path):
        versions = self._run(tmp_path, self.V1_PROCESSING)
        assert versions == {"aind-smartspim-stitch - Image importing": {"version": "1.2.9"}}

    def test_v2_processing(self, tmp_path):
        versions = self._run(tmp_path, self.V2_PROCESSING)
        assert versions == {
            "aind-smartspim-data-transformation - Compression": {"version": "0.1.9"}
        }
