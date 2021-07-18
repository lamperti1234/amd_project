import logging
import random
from collections import defaultdict
from itertools import combinations
from typing import Iterator, Callable, List, Optional, Set

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_analysis import Transaction, Itemset, CandidateFrequentItemsets, State, FrequentItemsets, \
    read_frequent_itemsets, save_frequent_itemsets, Algorithm, find_csv, dump_frequent_itemsets_stats, create_temp_df
from definitions import RESULTS, SAVE, RAW_PATH, TOIVONEN_SIZE_SAMPLE, APRIORI_THRESHOLD, TOIVONEN_THRESHOLD_ADJUST, \
    TOIVONEN_MAX_ITERATIONS, DUMP, DATASET_PATH
from spark_utils import get_spark, read_csv
from utils import timer, memory_used, get_path, is_empty, read_csvfile


@timer
@memory_used
def get_ck(transactions: Iterator[Transaction], k: int,
           monotonicity_filter: Callable[[Itemset], int]) -> CandidateFrequentItemsets:
    """
    Scan the sample and extract candidate frequent itemsets checking the monotonicity condition.

    :param transactions: iterator of transactions
    :param k: size of the itemset
    :param monotonicity_filter: filter for the monotonicity condition
    :return: candidate frequent itemsets
    """
    accumulator = defaultdict(int)
    for row in transactions:
        raw_actors = [int(actor[2:]) for actor in row[1].split('|')]
        for comb in combinations(sorted(raw_actors), k):
            if monotonicity_filter(comb):
                accumulator[comb] += 1
    return accumulator


@timer
@memory_used
def get_lk(transactions: Iterator[Transaction], sample: List[Transaction], state: State) -> Optional[FrequentItemsets]:
    """
    Extract frequent itemsets checking the support.

    :param transactions: iterator of transactions
    :param sample: sample of all transactions
    :param state: state of the algorithm
    :return: frequent itemsets
    """
    n = state['n']
    size = state['k']
    old_lk = state['lk']
    threshold_adjust = state['threshold_adjust']
    threshold = threshold_adjust * len(sample) / n * state['threshold']
    force = state['force']

    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'csv', 'toivonen', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_frequent_itemsets(path)

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    if size == 1:
        ck = get_ck(sample, size, lambda _: True)
    else:
        ck = get_ck(sample, size,
                    lambda itemset: all([item in old_lk for item in combinations(itemset, size - 1)]))

    lk = {item
          for item, support in ck.items()
          if support >= threshold}
    negative_border = {item
                       for item, support in ck.items()
                       if support < threshold}

    full_support = last_full_scan(transactions, size, lk, negative_border)
    real_lk = {item: support
               for item, support in full_support.items()
               if support >= state['threshold']}

    common = set(real_lk.keys()) & negative_border
    if common:
        logging.warning(f'Found {len(common)} frequent itemsets in the negative border.\n'
                        f'Example: {[(i, ck[i], full_support[i]) for i in list(common)[:10]]}')
        return None

    not_found = set(real_lk.keys()) - lk
    if not_found:
        logging.warning(f'Found {len(not_found)} frequent itemsets not found.\n'
                        f'Example: {not_found}')
        return None

    logging.info(f'Found {len(lk)} frequent itemsets')

    if state['save']:
        save_frequent_itemsets(real_lk, path)

    return real_lk


def sampling(df: DataFrame, k: int) -> DataFrame:
    """
    Get a random sample of the dataframe of size k.

    :param df: dataframe to be sampled
    :param k: size of the sample
    :return: sampled dataframe
    """
    n = df.count()
    df = (df
          .withColumn('fake', F.lit(0))
          .withColumn('row', F.row_number().over(Window.partitionBy('fake').orderBy('_c0')))
          .drop('fake')
          )
    sample = get_spark().createDataFrame(random.sample(range(1, n + 1), k), T.IntegerType())
    return df.join(sample, on=df.row == sample.value).drop('row', 'value')


def last_full_scan(transactions: Iterator[Transaction], k: int,
                   lk: Set[Itemset], negative_border: Set[Itemset]) -> FrequentItemsets:
    """
    Scan the file and extract candidate frequent itemsets.

    :param transactions: iterator of transactions
    :param k: size of the itemset
    :param lk: candidate frequent itemset
    :param negative_border: infrequent itemset
    :return: candidate frequent itemsets
    """
    accumulator = defaultdict(int)
    for row in transactions:
        raw_actors = [int(actor[2:]) for actor in row[1].split('|')]
        for comb in combinations(sorted(raw_actors), k):
            if comb in lk or comb in negative_border:
                accumulator[comb] += 1
    return accumulator


def toivonen_algorithm(transactions: Callable[[], Iterator[Transaction]],
                       sample: Iterator[Transaction], state: State) -> Algorithm:
    """
    Executing apriori algorithm starting from data and a given threshold.

    :param transactions: callable to an iterator of transactions
    :param sample: sample of all transactions
    :param state: state of the algorithm:
        - threshold: threshold for the apriori algorithm
        - k: size of the itemsets
        - lk: frequent itemsets with size k-1
        - force: to force recalculating frequent itemsets
    :return: dict of frequent itemsets
    """
    state = State(k=1, lk={}, force=False, save=SAVE) + state

    while state['k'] == 1 or state['lk']:
        state['lk'] = get_lk(transactions(), sample, state)
        if state['lk'] is None:
            yield state
        state['k'] += 1

        yield state


if __name__ == '__main__':
    file = find_csv(get_path(RAW_PATH, 'csv'))

    df1 = read_csv(file, header=False)
    n = df1.count()

    for i in range(TOIVONEN_MAX_ITERATIONS):
        print(f'\tIteration number {i}')
        sample = [(row['_c0'], row['_c1']) for row in sampling(df1, TOIVONEN_SIZE_SAMPLE).collect()]
        algorithm = toivonen_algorithm(lambda: read_csvfile(file), sample,
                                       State(threshold_adjust=TOIVONEN_THRESHOLD_ADJUST,
                                             threshold=APRIORI_THRESHOLD, n=n))
        try:
            singleton = next(algorithm)['lk']
            doubleton = next(algorithm)['lk']
            triple = next(algorithm)['lk']
            quadruple = next(algorithm)['lk']
            quintuple = next(algorithm)['lk']
            print('Toivonen completed')

            if DUMP:
                names = read_csv(get_path(DATASET_PATH, 'name.basics.tsv.gz'), sep='\t')
                dump_frequent_itemsets_stats(create_temp_df(singleton, names), 1)
                dump_frequent_itemsets_stats(create_temp_df(doubleton, names), 2)
                dump_frequent_itemsets_stats(create_temp_df(triple, names), 3)
                dump_frequent_itemsets_stats(create_temp_df(quadruple, names), 4)
                dump_frequent_itemsets_stats(create_temp_df(quintuple, names), 5)

            break
        except StopIteration:
            pass
