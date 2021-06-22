import os
import shutil

from pathlib import Path
from typing import Any, Union


def get_path(base_path: Union[str, Path], *args: Any, create: bool = False, delete_before_create: bool = False) -> Path:
    """
    Join path with default separator. Optionally it can create and/or clean the specified path if it is a folder.

    :param base_path: base path to use to new path
    :param args: eventually subfolders
    :param create: create the folder if not exists
    :param delete_before_create: clear folder if it already exists
    :return: the required path
    """
    path = Path(base_path, *[str(arg) for arg in args if arg])
    if create:
        if delete_before_create:
            shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)
    return path


def get_filename(path: Union[str, Path]) -> str:
    """
    Get filename or last folder from a path.

    :param path: path of a file
    :return: the filename or last folder
    """
    return Path(path).stem


def set_env_variables(**kwargs: Any) -> None:
    """
    Set environment variables.

    :param kwargs: dictionary of key-value environment variables
    :return:
    """
    for key, value in kwargs.items():
        os.environ[key] = str(value)


def is_empty(path: Union[str, Path]) -> bool:
    """
    Check if a folder is empty.

    :param path: path of a folder
    :return:
    """
    return not bool(os.listdir(path))
