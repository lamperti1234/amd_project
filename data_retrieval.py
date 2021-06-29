import json
import logging
import os

from definitions import CONFIG, DATASETS, DATASET
from utils import get_path, set_env_variables, is_empty


def _set_api() -> None:
    """
    Set kaggle api required configuration from file or ask for them.

    :return:
    """
    path = get_path(CONFIG, 'kaggle.json')
    credentials = {}
    if os.path.exists(path):
        with open(path) as f:
            credentials = json.load(f)
    else:
        path = get_path(os.environ['HOME'], '.kaggle', 'kaggle.json')
        if os.path.exists(path):
            with open(path) as f:
                credentials = json.load(f)
    if not credentials or not credentials['username'] or not credentials['key']:
        credentials['username'] = input("Insert Kaggle username: ")
        credentials['key'] = input("Insert Kaggle API key: ")
    set_env_variables(KAGGLE_USERNAME=credentials['username'], KAGGLE_KEY=credentials['key'])
    logging.debug('Set environment variables for Kaggle')


def download_dataset(name: str, force: bool = False) -> None:
    """
    Download the specified dataset if not already download.

    :param name: name of the dataset
    :param force: force downlaod if dataset already exists
    """
    path = get_path(DATASETS, name, create=True, delete=force)
    if not is_empty(path):
        logging.info(f'Dataset {name} already downloaded!')
        return
    _set_api()

    # require authentication before importing
    from kaggle import api

    api.dataset_download_cli(dataset=name, path=path, force=force, unzip=True)
    logging.info(f'Dataset {name} downloaded!')


if __name__ == '__main__':
    download_dataset(DATASET)
