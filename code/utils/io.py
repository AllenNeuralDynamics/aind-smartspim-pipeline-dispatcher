"""
File I/O, path helpers, and shell-execution utilities.
"""

import json
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

PathLike = Union[str, Path]


def copy_file(input_filename: PathLike, output_filename: PathLike):
    """
    Copies a file to an output path.
    """
    try:
        shutil.copy(input_filename, output_filename)
    except shutil.SameFileError:
        raise shutil.SameFileError(
            f"The filename {input_filename} already exists in the output path."
        )
    except PermissionError:
        raise PermissionError(
            "Not able to copy the file. Please, check the permissions in the output path."
        )


def create_folder(dest_dir: PathLike, verbose: Optional[bool] = False) -> None:
    """
    Create new folders.
    """
    if not (os.path.exists(dest_dir)):
        try:
            if verbose:
                print(f"Creating new directory: {dest_dir}")
            os.makedirs(dest_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise


def delete_folder(dest_dir: PathLike, verbose: Optional[bool] = False) -> None:
    """
    Delete a folder path.
    """
    if os.path.exists(dest_dir):
        try:
            shutil.rmtree(dest_dir)
            if verbose:
                print(f"Folder {dest_dir} was removed!")
        except shutil.Error as e:
            print(f"Folder could not be removed! Error {e}")


def execute_command_helper(
    command: str,
    print_command: bool = False,
    stdout_log_file: Optional[PathLike] = None,
) -> None:
    """
    Execute a shell command and yield each stdout line.
    """
    if print_command:
        print(command)

    if stdout_log_file and len(str(stdout_log_file)):
        save_string_to_txt("$ " + command, stdout_log_file, "a")

    popen = subprocess.Popen(
        command, stdout=subprocess.PIPE, universal_newlines=True, shell=True
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        yield str(stdout_line).strip()
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, command)


def execute_command(config: dict) -> None:
    """
    Execute a shell command with a given configuration dict.
    """
    if config["info"]:
        config["logger"].info(config["command"])
    else:
        for out in execute_command_helper(
            config["command"], config["verbose"], config["stdout_log_file"]
        ):
            if len(out):
                config["logger"].info(out)

            if config["exists_stdout"]:
                save_string_to_txt(out, config["stdout_log_file"], "a")


def check_path_instance(obj: object) -> bool:
    """
    Checks if an object belongs to pathlib.Path subclasses.
    """
    for childclass in Path.__subclasses__():
        if isinstance(obj, childclass):
            return True
    return False


def save_dict_as_json(
    filename: str, dictionary: dict, verbose: Optional[bool] = False
) -> None:
    """
    Saves a dictionary as a json file.
    """
    if dictionary is None:
        dictionary = {}
    else:
        for key, value in dictionary.items():
            if check_path_instance(value):
                dictionary[key] = str(value)

    with open(filename, "w") as json_file:
        json.dump(dictionary, json_file, indent=4)

    if verbose:
        print(f"- Json file saved: {filename}")


def read_json_as_dict(filepath: str) -> dict:
    """
    Reads a json file as a dictionary.
    """
    dictionary = {}

    if os.path.exists(filepath):
        with open(filepath) as json_file:
            dictionary = json.load(json_file)

    return dictionary


def save_string_to_txt(txt: str, filepath: PathLike, mode="w") -> None:
    """
    Saves a text string to a file.
    """
    with open(filepath, mode) as file:
        file.write(txt + "\n")


def check_type_helper(value: Any, val_type: type) -> bool:
    """
    Checks if a value belongs to a specific type.

    Note: the current implementation is inverted — it checks if type(value)
    is an instance of val_type rather than checking value itself. This is a
    known bug (see BUGS.md #5); behaviour preserved intentionally to avoid
    logic changes.
    """
    if not isinstance(type(value), val_type):
        return False
    return True


def generate_timestamp(time_format: str = "%Y-%m-%d_%H-%M-%S") -> str:
    """
    Generates a timestamp string.
    """
    return datetime.now().strftime(time_format)


def validate_capsule_inputs(input_elements: List[str]) -> List[str]:
    """
    Returns a list of paths from input_elements that do not exist.
    """
    missing_inputs = []
    for required_input_element in input_elements:
        required_input_element = Path(required_input_element)
        if not required_input_element.exists():
            missing_inputs.append(str(required_input_element))
    return missing_inputs


def get_yaml_config(filename: PathLike) -> dict:
    """
    Loads a YAML file and returns its contents as a dict.
    """
    with open(filename, "r") as stream:
        config = yaml.safe_load(stream)
    return config
