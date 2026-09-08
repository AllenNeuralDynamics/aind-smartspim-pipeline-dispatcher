# aind-smartspim-pipeline-dispatcher

Code Ocean capsule that orchestrates parallel image-processing steps in the SmartSPIM pipeline.
It exploits the Code Ocean "flatten connection" feature to fan out per-channel workloads to
downstream capsules (segmentation, CCF registration, quantification) and later collects their
results.

---

## Capsule Modes

| Mode | When to use | Description |
|---|---|---|
| `dispatch` | Middle of the pipeline (after fusion) | Registers the stitched data asset in Code Ocean, creates per-channel segmentation manifests for the flatten connection, and copies intermediate results to S3. |
| `clean` | End of the pipeline | Moves all segmentation and quantification outputs to the `destination bucket` S3 bucket and sends a completion notification. |
| `postprocess-start` | Start of a re-processing run | Detects which downstream steps (registration, segmentation, classification, quantification) need to be re-run and prepares the corresponding manifests. |
| `postprocess-stop` | End of a re-processing run | Collects updated CCF, cell segmentation, and quantification outputs, compiles a new `processing.json`, and copies everything back to S3. |
| `split_channels` | Pre-processing | Lists the channels of the raw acquisition data in the input bucket (or local input path) and writes per-channel preprocessing manifests. |

---

## Repository Structure

```
aind-smartspim-pipeline-dispatcher/
├── code/
│   ├── __init__.py                 # Package version and maintainer constants
│   ├── run                         # Bash entry-point (calls run_capsule.py)
│   ├── run_capsule.py              # Thin orchestrator: parses mode, calls handlers
│   ├── modes/
│   │   ├── __init__.py
│   │   ├── dispatch.py             # dispatch() and Code Ocean registration helpers
│   │   ├── cleanup.py              # clean_up() — moves results to S3, sends alert
│   │   └── postprocess.py          # postprocess helpers (copy_postprocessed_data, etc.)
│   ├── manifests/
│   │   ├── __init__.py
│   │   └── builder.py              # Manifest creation, reading, format conversion
│   └── utils/
│       ├── __init__.py             # Re-exports all sub-module symbols
│       ├── utils.py                # Backwards-compatibility shim (re-exports)
│       ├── io.py                   # File I/O: copy_file, create_folder, read/save JSON, etc.
│       ├── aws.py                  # S3 helpers, Secrets Manager credential retrieval
│       ├── notifications.py        # AlertBot (Teams), SES email alerts
│       ├── schemas.py              # AIND metadata generation (data description, processing, QC)
│       └── visualization.py        # Neuroglancer link generation, wavelength → hex, dynamic range
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Shared fixtures (tmp dirs, sample JSON, mock boto3)
│   ├── test_io.py
│   ├── test_aws.py
│   ├── test_notifications.py
│   ├── test_visualization.py
│   ├── test_manifests.py
│   └── test_run_capsule.py
├── environment/
│   └── Dockerfile
├── .env.example                    # Environment variable template for external users
├── pyproject.toml                  # pytest configuration
└── README.md
```

---

## Dependencies

Installed in the capsule Docker image (see [environment/Dockerfile_local](environment/Dockerfile_local)):
---

## Configuration for External Use

All organisation-specific values are read from environment variables. Copy `.env.example`
to `.env` and fill in the values before running locally. In Code Ocean, set them as
capsule environment variables instead.

| Variable | Required | Description | If absent |
|---|---|---|---|
| `CUSTOM_KEY` | Yes | MS Teams webhook URL for pipeline alerts | Runtime error |
| `API_SECRET` | Yes (dispatch) | Code Ocean API token | Runtime error |
| `CODEOCEAN_DOMAIN` | No | Code Ocean organisation URL | Data-asset registration skipped |
| `OUTPUT_BUCKET` | No | S3 bucket for processed data | S3 copy and dispatch skipped |
| `INPUT_BUCKET` | No | S3 bucket with the raw acquisition data (used by `split_channels`) | Defaults to `OUTPUT_BUCKET` |
| `SES_TOKEN_PATH` | No | AWS Secrets Manager path to SES/Smartsheet token | Email alerts skipped |
| `SMARTSHEET_ID` | No | Smartsheet sheet ID for investigator lookup | Email alerts skipped |
| `SOURCE_EMAIL` | No | SES-verified source email address | Email alerts skipped |
| `NG_BASE_URL` | No | Neuroglancer base URL | Defaults to public demo instance |
| `CCF_ANNOTATION_S3` | No | S3 path to CCF annotation precomputed volume | CCF annotation layer omitted |

The capsule is designed so that **missing optional variables disable only the feature they
support** — the core data-processing pipeline continues unaffected.

---

## Running Locally

The capsule entry-point is the `code/run` bash script, which calls `python run_capsule.py <mode>`.

```bash
# From the repository root
cd code
python run_capsule.py split_channels
python run_capsule.py dispatch
python run_capsule.py clean
python run_capsule.py postprocess-start
python run_capsule.py postprocess-stop
```

Positional form (Nextflow compat) — the bucket order is mode-dependent:

```bash
python run_capsule.py split_channels <cloud> <input_bucket> [<output_bucket>]   # raw data first
python run_capsule.py <other_mode>   <cloud> <output_bucket> [<input_bucket>]
```

The input location (raw acquisition data, used by `split_channels`) defaults to the
output location when omitted, so single-bucket invocations keep working.

Required environment variables:

| Variable | Description |
|---|---|
| `CUSTOM_KEY` | MS Teams webhook URL for pipeline notifications |
| `API_SECRET` | Code Ocean API token (dispatch mode only) |

---

## Running Tests

```bash
# Install test dependencies in the capsule environment first (see Dockerfile)
pip install pytest PyYAML smartsheet-dataframe "aind-data-schema==1.3.0"

# From the repository root
pytest tests/ -v
```

---

## Pipeline Overview

The dispatcher sits at two key positions in the SmartSPIM pipeline:

```
Raw acquisition
      │
  Preprocessing (microscope-to-zarr, flatfield, destripe, stitch, fuse)
      │
  [dispatcher — dispatch mode]  ←─ registers stitched asset, fans out per-channel manifests
      │
  ┌───┼──────────────────────────┐
  │   │                          │
CCF reg   Cell segmentation   (other channels)
  │   │
  └───┼──────────────────────────┘
      │
  [dispatcher — clean mode]  ←─ collects results, copies to the destination bucket, sends alert
      │
  Done ✓
```

For re-processing existing stitched datasets the `postprocess-start` / `postprocess-stop`
pair is used instead of `dispatch` / `clean`.

---
