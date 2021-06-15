import json
import os
from pathlib import Path

from utils import get_path

ROOT_DIR = str(Path(__file__).parent.absolute())
CONFIG = get_path(ROOT_DIR, 'conf', create=True)
DATASETS = get_path(ROOT_DIR, 'datasets', create=True)
RESULTS = get_path(ROOT_DIR, 'results', create=True)
TRAINING = get_path(RESULTS, 'training', create=True)
PREDICTIONS = get_path(RESULTS, 'predictions', create=True)


def update_configs():
    path = get_path(CONFIG, 'config.json')
    if os.path.exists(path):
        with open(path) as f:
            config = json.load(f)
        params.update(config)


# variables
params = locals()
DATASET = None

update_configs()
