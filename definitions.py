import json
import logging
import os

from pathlib import Path
from typing import Any

from utils import get_path

ROOT_DIR = str(Path(__file__).parent.absolute())
CONFIG = get_path(ROOT_DIR, 'conf', create=True)
DATASETS = get_path(ROOT_DIR, 'datasets', create=True)
RESULTS = get_path(ROOT_DIR, 'results', create=True)
TRAINING = get_path(RESULTS, 'training', create=True)
PREDICTIONS = get_path(RESULTS, 'predictions', create=True)


def _update_configs():
    """
    Read config file.

    :return:
    """
    path = get_path(CONFIG, 'config.json')
    if os.path.exists(path):
        with open(path) as f:
            config = json.load(f)
        _params.update(config)


def update_config(**kwargs: Any):
    """
    Update a variable at runtime.

    :param kwargs: variables that needs to be updated
    :return:
    """
    logging.debug(f'Updated config variables: {kwargs}')
    _params.update(kwargs)


# variables
_params = locals()
DATASET = ''
LOG_FORMAT = '%(levelname)s:%(name)s:%(message)s'
LOG_LEVEL = logging.INFO

_update_configs()

logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
