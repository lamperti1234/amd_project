import logging

from pyspark.sql import SparkSession, DataFrame

from definitions import DATASETS, DATASET
from utils import get_path


def get_spark() -> SparkSession:
    """
    Create a SparkSession with predefined values.

    :return: a SparkSession object
    """
    return SparkSession.builder.appName('amd').getOrCreate()


def read_csv(path: str, header: bool = True, sep: str = ',') -> DataFrame:
    """
    Read a csv file and put it on a DataFrame. It will escape quote to allow reading multiple lines column.

    :param path: path to csv file
    :param header: if file has a first row with column names
    :param sep: separator used in csv file
    :return: dataframe from selected path
    """
    logging.info(f'Reading path {path} with header:{header} and sep:{sep})')
    return (get_spark()
            .read
            .option('header', header)
            .option('multiLine', True)
            .option("escape", "\"")
            .option('sep', sep)
            .csv(path))


def read_data(path: str) -> DataFrame:
    """
    Extract data from dataset.

    :param path: path to csv file.
    :return: dataframe from selected path
    """
    return read_csv(path).select('Body').persist()


if __name__ == '__main__':
    DATASET_PATH = get_path(DATASETS, DATASET, 'Questions.csv')
    df = read_data(str(DATASET_PATH))
