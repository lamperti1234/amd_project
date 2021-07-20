import logging

from pathlib import Path

from pyspark import AccumulatorParam, RDD
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Union, Dict

from utils import timer


def get_spark() -> SparkSession:
    """
    Create a SparkSession with predefined values.

    :return: a SparkSession object
    """
    return (SparkSession.builder
            .appName('amd')
            .getOrCreate())


def check_empty(df: DataFrame) -> bool:
    """
    Check if a dataframe is empty.

    :param df: dataframe to be checked
    :return: true if dataframe has 0 rows, false otherwise
    """
    return df.take(1).count == 0


def read_csv_rdd(path: Union[Path, str], sep: str = ',') -> Optional[RDD]:
    """
        Read a csv file and put it on a RDD. It returns None if no such path exists.

        :param path: path to csv file
        :param sep: separator used in csv file
        :return: dataframe from selected path
        """
    logging.info(f'Reading csv path {path} with sep:{sep}')

    path = Path(path)
    if path.exists():
        return (get_spark()
                .sparkContext
                .textFile(str(path))
                .map(lambda row: row.split(sep))
                )

    return None


def read_csv_df(path: Union[Path, str], header: bool = True, sep: str = ',') -> Optional[DataFrame]:
    """
    Read a csv file and put it on a DataFrame. It will escape quote to allow reading multiple lines column.
    It returns None if no such path exists.

    :param path: path to csv file
    :param header: if file has a first row with column names
    :param sep: separator used in csv file
    :return: dataframe from selected path
    """
    logging.info(f'Reading csv path {path} with header:{header} and sep:{sep}')

    path = Path(path)
    if path.exists():
        return (get_spark()
                .read
                .option('header', header)
                .option('multiLine', True)
                .option("escape", "\"")
                .option('sep', sep)
                .csv(str(path)))

    return None


@timer
def save_csv_df(df: DataFrame, path: Union[Path, str], header: bool = True) -> None:
    """
    Save a DataFrame in a csv file.

    :param df: dataframe to be saved
    :param path: path where to save the file
    :param header: if csv file will have an header
    :return:
    """
    logging.info(f'Saving csv path {path}')

    df.write.csv(str(path), header=header)


def read_parquet(path: Union[Path, str]) -> Optional[DataFrame]:
    """
    Read a parquet file and put in a DataFrame. It returns None if no such path exists.

    :param path: path to parquet file
    :return: dataframe from selected path
    """
    logging.info(f'Reading parquet path {path}')

    path = Path(path)
    if path.exists():
        return get_spark().read.parquet(str(path))

    return None


@timer
def save_parquet(df: DataFrame, path: Union[Path, str]) -> None:
    """
    Save a DataFrame in a parquet file.

    :param df: dataframe to be saved
    :param path: path where to save the file
    :return:
    """
    logging.info(f'Saving parquet path {path}')

    df.write.parquet(str(path))


class DictParam(AccumulatorParam):
    """
    It allows to have a dict accumulator.
    """

    def zero(self, value: Dict = None) -> Dict:
        return {}

    def addInPlace(self, value1: Dict, value2: Dict) -> Dict:
        value1.update(value2)

        return value1
