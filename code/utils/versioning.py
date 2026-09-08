"""
Version-checking utilities for the SmartSPIM pipeline.
"""

import logging
import re
from typing import Dict, List, Optional

import requests

from utils.io import read_json_as_dict

logger = logging.getLogger(__name__)


def get_version(
    owner: str, repo: str, path: str, branch: Optional[str] = "main"
) -> str:
    """
    Gets the version of a repository,

    Parameters
    ----------
    owner: str
        Github owner of the repository.

    repo: str
        Repository name

    path: str
        Path within the repository

    branch: Optional[str]
        Branch from where we will pull
        the version. Default: "main"

    Returns
    -------
    str
        String with the version
    """
    url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    response = requests.get(url)

    if response.status_code == 200:
        match = re.search(r'__version__\s*=\s*"([^"]+)"', response.text)
        return match.group(1) if match else "Version not found"
    else:
        return None


def get_pipeline_versions(
    pipeline_repos: List, owner: Optional[str] = "AllenNeuralDynamics"
) -> Dict:
    """
    Gets the SmartSPIM pipeline version for
    each of the image processing steps.

    Parameters
    ----------
    pipeline_repos: List
        List with tuples correspoding to Tuple[
            repo_name, metadata name in aind schema
        ]

    owner: Optional[str]
        Repository owner.
        Default: "AllenNeuralDynamics"

    Returns
    -------
    Dict
        Dictionary with the versions of the latest
        version of each of the SmartSPIM pipeline steps.
    """
    step_versions = {}

    for repo, step_name in pipeline_repos:
        if "ccf" in repo:
            package_name = "aind_ccf_reg"
        else:
            package_name = repo.replace("-", "_")

        version = get_version(owner, repo, path=f"code/{package_name}/__init__.py")
        step_versions[f"{repo} - {step_name}"] = {
            "version": version,
        }

    return step_versions


def get_dataset_step_versions(dataset_path):
    processing_path = dataset_path / "processing.json"
    dataset_step_versions = None

    if processing_path.exists():
        try:
            processing_data = read_json_as_dict(filepath=str(processing_path))
        except BaseException:
            logger.error(f"Error reading {processing_path}", exc_info=True)
            processing_data = {}

        processing_pipeline = processing_data.get("processing_pipeline")
        pipeline_steps = processing_data.get("data_processes")

        if pipeline_steps is None:
            pipeline_steps = (
                processing_pipeline.get("data_processes")
                if processing_pipeline
                else None
            )

        if pipeline_steps:
            dataset_step_versions = {}

            for step in pipeline_steps:
                # v1 steps carry flat code_url/software_version fields, while
                # v2 steps nest them inside a "code" object
                code = step.get("code") or {}
                code_url = step.get("code_url") or code.get("url")
                step_name = step.get("name") or step.get("process_type")
                code_version = (
                    step.get("software_version")
                    or step.get("version")
                    or code.get("version")
                )

                package_name = code_url.split("/")[-1] if code_url else "unknown"
                dataset_step_versions[f"{package_name} - {step_name}"] = {
                    "version": code_version
                }

        else:
            logger.warning(f"No pipeline steps found in {processing_path}")
            logger.debug(f"Processing data without steps: {processing_data}")

    else:
        logger.warning(
            f"Processing path does not exist for {dataset_path.stem}: {processing_path}"
        )

    return dataset_step_versions


def check_dataset_latest_version(
    dataset_versions: Dict,
    latest_versions: Dict,
):
    """
    Checks within the metadata (processing.json)
    and the image processing versions to see
    if any of the steps need to be rerun. If it
    is not the latest version, the step will
    be flagged as True to reprocess.

    Parameters
    ----------
    dataset_versions: Dict
        Image processing steps with their versions
        in the SmartSPIM pipeline for a given dataset.
        This metadata can be found in the processing.json

    latest_versions: Dict
        Latest versions of the image processing steps
        in the SmartSPIM pipeline. This is related to
        the pipeline and not a specific dataset.

    Returns
    -------
    Dict
        Dictionary for each of the steps that dictates
        if we need to process a specific step in the pipeline.
    """
    process_versions = {}

    for step, values in latest_versions.items():
        dataset_step = dataset_versions.get(step)

        if dataset_step is None:
            curr_key = None
            if "tile alignment" in step:
                curr_key = [
                    d
                    for d in list(dataset_versions.keys())
                    if "tile alignment" in d.lower()
                ]

            elif "tile fusing" in step:
                curr_key = [
                    d
                    for d in list(dataset_versions.keys())
                    if "tile fusing" in d.lower()
                ]

            elif "atlas alignment" in step:
                curr_key = [
                    d
                    for d in list(dataset_versions.keys())
                    if "atlas alignment".lower() in d.lower()
                ]

            curr_key = curr_key[0] if curr_key and len(curr_key) else None
            dataset_step = dataset_versions.get(curr_key)

        values_version = values.get("version")

        process_versions[step] = {
            "process": True,
            "latest_version": values_version,
            "dataset_version": None,
        }

        if dataset_step:
            dataset_step_version = dataset_step.get("version")
            if dataset_step_version == values_version:
                process_versions[step] = {
                    "process": False,
                    "dataset_version": dataset_step_version,
                    "latest_version": values_version,
                }

            else:
                process_versions[step]["dataset_version"] = dataset_step_version

    return process_versions
