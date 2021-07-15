import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from data_analysis import dump_frequent_itemsets_stats, State, Algorithm
from data_extraction import extract_data
from definitions import RESULTS, APRIORI_THRESHOLD, SAVE, DUMP
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


def get_lk(df: DataFrame, state: State) -> DataFrame:
    """
    Extract frequent itemsets from candidate itemsets.

    :param df: dataframe which contains candidate itemsets
    :param state: state of the algorithm
    """
    threshold = state['threshold']
    size = state['k']
    cols = [f'actor{i}' for i in range(1, size + 1)]
    force = state['force']

    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'parquet', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_parquet(path)

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    actors_movies = Window.partitionBy(*cols)

    df = get_ck(df, *cols[:-1])

    df_with_count = df.withColumn('support', F.count('*').over(actors_movies))
    df = df_with_count.filter(f'support >= {threshold}')
    # dataframe has movie column so without distinct there are duplicates
    logging.info(f'Found {df.select(*cols).distinct().count()} frequent itemsets')

    if SAVE:
        save_parquet(df, path)

    return df


def apriori_algorithm(df: DataFrame, state: State) -> Algorithm:
    """
    Executing apriori algorithm starting from data and a given threshold.

    :param df: data to be analyzed
    :param state: state of the algorithm:
        - threshold: threshold for the apriori algorithm
        - force: to force recalculating frequent itemsets
    :return: dataframe with frequent itemsets
    """
    state = State(k=1) + state
    while not check_empty(df):
        df = get_lk(df, state)
        state['k'] += 1
        state['df'] = df

        yield state


if __name__ == '__main__':
    dataframe = extract_data()
    algorithm = apriori_algorithm(dataframe, State(threshold=APRIORI_THRESHOLD, force=True))

    singleton = next(algorithm)['df']
    doubleton = next(algorithm)['df']
    triple = next(algorithm)['df']
    quadruple = next(algorithm)['df']
    quintuple = next(algorithm)['df']

    if DUMP:
        dump_frequent_itemsets_stats(singleton, 1)
        dump_frequent_itemsets_stats(doubleton, 2)
        dump_frequent_itemsets_stats(triple, 3)
        dump_frequent_itemsets_stats(quadruple, 4)
        dump_frequent_itemsets_stats(quintuple, 5)
