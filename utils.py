import csv
import logging
import os
import shutil

from datetime import datetime
from pathlib import Path
from types import FunctionType
from typing import Any, Union, Callable, Iterator, Tuple

import psutil


def delete_path(path: Union[str, Path]) -> None:
    """
    Remove folder and all subfolders.

    :param path: path to be removed
    """
    shutil.rmtree(path, ignore_errors=True)


def get_path(base_path: Union[str, Path], *args: Any, create: bool = False, delete: bool = False) -> Path:
    """
    Join path with default separator. Optionally it can create and/or clean the specified path if it is a folder.

    :param base_path: base path to use to new path
    :param args: eventually subfolders
    :param create: create the folder if not exists
    :param delete: clear folder if it already exists
    :return: the required path
    """
    path = Path(base_path, *[str(arg) for arg in args if arg])
    if delete:
        delete_path(path)
    if create:
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
    """
    for key, value in kwargs.items():
        os.environ[key] = str(value)


def is_empty(path: Union[str, Path]) -> bool:
    """
    Check if a folder is empty.

    :param path: path of a folder
    :return: true if path does not exists of if it has no files
    """
    path = Path(path)

    return not (path.exists() and bool(os.listdir(path)))


def timer(func: FunctionType) -> Callable:
    """
    Calculating execution time for a specific function.

    :param func: function to be executed
    :return: wrapper for the function
    """
    def wrapper(*args, **kwargs) -> Any:
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        print(f'Execution time for {func.__name__}: {end - start}')

        return result

    return wrapper


def memory_used(func: FunctionType) -> Callable:
    """
    Calculating memory usage for a specific function.

    :param func: function to be executed
    :return: wrapper for the function
    """
    def wrapper(*args, **kwargs) -> Any:
        start_mem = psutil.virtual_memory().used
        result = func(*args, **kwargs)
        end_mem = psutil.virtual_memory().used
        print(f'Memory used for {func.__name__ }: {end_mem - start_mem}')

        return result

    return wrapper


def read_csvfile(path: Union[str, Path]) -> Iterator[Tuple[str, str]]:
    """
    Read csv file as list of fields skipping header.

    :param path: path to csv file
    :return:
    """
    logging.info(f'Reading csv path {path}')
    with open(path) as file:
        reader = csv.reader(file)
        for row in reader:
            yield row


def save_csvfile(data: Iterator, path: Union[str, Path]) -> None:
    """
    Save an iterator as csv file.

    :param data: dict to save to file
    :param path: path where save csv file
    :return:
    """
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(data)
