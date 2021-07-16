import logging
import os
from pathlib import Path
from typing import Tuple, Dict, Union, Iterator, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_utils import get_spark
from utils import get_path, timer, read_csvfile, save_csvfile

Itemset = Tuple[int, ...]
CandidateFrequentItemsets = Dict[Itemset, int]
FrequentItemsets = CandidateFrequentItemsets
BitMap = Dict[int, bool]
Transaction = Tuple[str, str]


class State:
    """
    State of an algorithm.
    At least k (size of the itemset), lk (frequent itemset) and force (if it is needed to recalculate) are present.
    """

    def __init__(self, **state):
        self.state = state

    def __setitem__(self, key: str, value: Any) -> None:
        self.state[key] = value

    def __getitem__(self, item: str) -> Any:
        return self.state[item]

    def __add__(self, other: 'State') -> 'State':
        state = {**self.state}
        state.update(other.state)

        return State(**state)

    def update(self, other: 'State') -> None:
        self.state.update(other.state)


Algorithm = Iterator[State]


@timer
def dump_frequent_itemsets_stats(df: DataFrame, n: int) -> None:
    """
    Dump some useful information about frequent itemsets.

    :param df: dataframe which contains frequent itemsets
    :param n: lenght of frequent itemset
    :return:
    """
    logging.info('Calculating frequent itemsets for dataframe')
    columns = [column.format(i) for i in range(1, n + 1) for column in ['actor{}', 'actor{}Name']]
    # distinct to remove duplicate from eliminating movie column
    df = df.select('support', *columns).distinct().orderBy(F.col('support').desc())
    df.show()
    stats = df.select(
        F.count('*').alias('count'),
        F.avg('support').alias('avg'),
        F.max('support').alias('max'),
        F.min('support').alias('min')
    )
    stats.show()


def create_temp_df(lk: CandidateFrequentItemsets, names: DataFrame) -> DataFrame:
    """
    Create a dataframe starting from frequent itemsets.

    :param lk: frequent itemsets with count
    :param names: names of the actors
    :return: dataframe for lk
    """
    df = get_spark().createDataFrame(lk.items(), ['actors', 'support'])
    df = df.select('actors.*', 'support')
    columns = ['support']

    for column in df.columns:
        if column.startswith('_'):
            number = column[1:]
            new_column = f'actor{number}'
            name_column = f'{new_column}Name'
            df = (df
                  .withColumn(column, F.concat(F.lit('nm'), F.lpad(column, 7, '0')))
                  .withColumnRenamed(column, new_column)
                  )
            df = (df
                  .join(names, names['nconst'] == F.col(new_column), how='left')
                  .withColumnRenamed('PrimaryName', name_column)
                  .select(*df.columns, new_column, name_column)
                  )

            columns.append(new_column)
            columns.append(name_column)

    return df.select(*columns)


def find_csv(path: Union[str, Path]) -> Path:
    """
    Find the csv file created by spark.

    :param path: path where to search the csv file
    :return: the correct path to csv file
    """
    for f in os.listdir(path):
        if f.endswith('.csv'):
            return get_path(path, f)


def read_frequent_itemsets(path: Union[str, Path]) -> FrequentItemsets:
    """
    Read csv file as frequent itemsets.

    :param path: path where frequent itemsets are stored
    :return: frequent itemsets in the csv file
    """
    path = get_path(path, 'results.csv')

    return {tuple([int(item) for item in row[0].split('|')]): int(row[1]) for row in read_csvfile(path)}


def save_frequent_itemsets(itemsets: FrequentItemsets, path: Union[str, Path]) -> None:
    """
    Save frequent itemsets as csv file where keys are the first column and values the second column.

    :param itemsets:
    :param path:
    :return:
    """
    get_path(path, create=True)
    save_csvfile((('|'.join([str(item) for item in key]), value) for key, value in itemsets.items()),
                 get_path(path, 'results.csv'))
