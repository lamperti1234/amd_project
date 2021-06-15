import json
import os

from definitions import CONFIG, DATASETS, DATASET
from utils import get_path, set_env_variables, is_empty


def _set_api():
    """Setta, se non presente, la chiave necessaria per utilizzare le API di kaggle."""
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
    print('credentials: ', credentials)
    set_env_variables(KAGGLE_USERNAME=credentials['username'], KAGGLE_KEY=credentials['key'])


def download_dataset(name: str, force: bool = False):
    """Scarica il dataset indicato se non gia' presente.

    :param name: nome del dataset
    :param force: per forzare nuovamente il download del dataset
    """
    path = get_path(DATASETS, name, create=True)
    if not is_empty(path) and not force:
        print(f'Dataset {name} already downloaded!')
        return
    _set_api()

    # require authentication before importing
    from kaggle import api

    api.dataset_download_cli(dataset=name, path=path, force=force, unzip=True)
    print(f'Dataset {name} downloaded!')


if __name__ == '__main__':
    if not DATASET:
        DATASET = input("Insert dataset name: ")
    download_dataset(DATASET)
