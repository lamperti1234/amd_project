import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import Iterator

from data_analysis import dump_frequent_itemsets_stats
from data_extraction import extract_data
from definitions import RESULTS, APRIORI_THRESHOLD
from spark_utils import check_empty, save_parquet, read_parquet
from utils import get_path, is_empty


def get_ck(df: DataFrame, *cols: str) -> DataFrame:
    """
    Extract candidate sets of lenght + 1 with respect to existing itemsets.

    :param df: dataframe which contains itemsets
    :param cols: columns needed to create new candidate itemsets
    :return: a dataframe which contains candidate itemsets
    """

    size = len(cols)

    # singleton
    if not size:
        return df

    column = cols[-1]
    next_column = f'actor{size + 1}'

    small_df = (df.select('movie', f'{column}Name', *cols)
                .withColumnRenamed(column, next_column)
                .withColumnRenamed(f'{column}Name', f'{next_column}Name'))

    # join on movie and all actors needed
    join_cond = ['movie'] + list(cols)[:-1]
    return df.join(small_df, on=join_cond).filter(f'{column} < {next_column}').persist()


def get_lk(df: DataFrame, threshold: float, *cols: str, force: bool = False) -> DataFrame:
    """
    Extract frequent itemsets from candidate itemsets.

    :param df: dataframe which contains candidate itemsets
    :param threshold: threshold value to consider candidate itemset as frequent itemset
    :param cols: columns needed to create new itemsets
    :param force: to force recalculating frequent itemsets
    """
    size = len(cols)
    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'parquet', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_parquet(path)

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    actors_movies = Window.partitionBy(*cols)

    df = get_ck(df, *cols[:-1])

    df_with_count = df.withColumn('support', F.count('*').over(actors_movies))
    df = df_with_count.filter(f'support > {threshold}')
    save_parquet(df, path)
    return df


def apriori_algorithm(df: DataFrame, threshold: int, force: bool = False) -> Iterator[DataFrame]:
    """
    Executing apriori algorith starting from data and a given threshold.

    :param df: data to be analyzed
    :param threshold: threshold for the apriori algorithm
    :param force: to force recalculating frequent itemsets
    :return: dataframe with frequent itemsets
    """
    columns = ['actor1']
    while not check_empty(df):
        df = get_lk(df, threshold, *columns, force=force)

        yield df

        columns.append(f'actor{len(columns) + 1}')


if __name__ == '__main__':
    dataframe = extract_data()
    algorithm = apriori_algorithm(dataframe, APRIORI_THRESHOLD, force=True)

    singleton = next(algorithm)
    doubleton = next(algorithm)
    triple = next(algorithm)
    quadruple = next(algorithm)
    quintuple = next(algorithm)

    dump_frequent_itemsets_stats(singleton, 1)
    dump_frequent_itemsets_stats(doubleton, 2)
    dump_frequent_itemsets_stats(triple, 3)
    dump_frequent_itemsets_stats(quadruple, 4)
    dump_frequent_itemsets_stats(quintuple, 5)
