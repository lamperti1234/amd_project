import os
import shutil
from pathlib import Path
from typing import Any


def get_path(base_path: str, *args: Any, create: bool = False, delete_before_create: bool = False) -> str:
    path = os.path.join(base_path, *[str(arg) for arg in args if arg])
    if create:
        if delete_before_create:
            shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)
    return path


def get_filename(path: str) -> str:
    return Path(path).stem


def set_env_variables(**kwargs: Any) -> None:
    for key, value in kwargs.items():
        os.environ[key] = str(value)


def is_empty(path: str) -> bool:
    return not bool(os.listdir(path))
