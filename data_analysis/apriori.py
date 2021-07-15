import logging
from collections import defaultdict
from itertools import combinations
from typing import Callable, Iterator

from data_analysis import CandidateFrequentItemsets, FrequentItemsets, read_frequent_itemsets, save_frequent_itemsets, \
    find_csv, dump_frequent_itemsets_stats, create_temp_df, Itemset, Transaction, Algorithm, State
from definitions import RAW_PATH, APRIORI_THRESHOLD, RESULTS, DATASET_PATH, SAVE, DUMP
from spark_utils import read_csv
from utils import timer, get_path, is_empty, read_csvfile, memory_used


@timer
@memory_used
def get_ck(transactions: Iterator[Transaction], k: int,
           monotonicity_filter: Callable[[Itemset], int]) -> CandidateFrequentItemsets:
    """
    Scan the file and extract candidate frequent itemsets checking the monotonicity condition.

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


@memory_used
def get_lk(transactions: Iterator[Transaction], state: State) -> FrequentItemsets:
    """
    Extract frequent itemsets checking the support.

    :param transactions: iterator of transactions
    :param state: state of the algorithm
    :return: frequent itemsets
    """
    size = state['k']
    old_lk = state['lk']
    threshold = state['threshold']
    force = state['force']

    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'csv', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_frequent_itemsets(path)

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    if size == 1:
        ck = get_ck(transactions, size, lambda _: True)
    else:
        ck = get_ck(transactions, size,
                    lambda itemset: all([item in old_lk for item in combinations(itemset, size - 1)]))

    lk = {item: support
          for item, support in ck.items()
          if support >= threshold}
    logging.info(f'Found {len(lk)} frequent itemsets')

    if SAVE:
        save_frequent_itemsets(lk, path)

    return lk


def apriori_algorithm(transactions: Callable[[], Iterator[Transaction]], state: State) -> Algorithm:
    """
    Executing apriori algorithm starting from data and a given threshold.

    :param transactions: callable to an iterator of transactions
    :param state: state of the algorithm:
        - threshold: threshold for the apriori algorithm
        - k: size of the itemsets
        - lk: frequent itemsets with size k-1
        - force: to force recalculating frequent itemsets
    :return: dict of frequent itemsets
    """
    state = State(k=1, lk={}) + state

    while state['k'] == 1 or state['lk']:
        state['lk'] = get_lk(transactions(), state)
        state['k'] += 1

        yield state


if __name__ == '__main__':
    file = find_csv(get_path(RAW_PATH, 'csv'))

    algorithm = apriori_algorithm(lambda: read_csvfile(file), State(threshold=APRIORI_THRESHOLD, force=True))

    singleton = next(algorithm)['lk']
    doubleton = next(algorithm)['lk']
    triple = next(algorithm)['lk']
    quadruple = next(algorithm)['lk']
    quintuple = next(algorithm)['lk']

    if DUMP:
        names = read_csv(get_path(DATASET_PATH, 'name.basics.tsv.gz'), sep='\t')
        dump_frequent_itemsets_stats(create_temp_df(singleton, names), 1)
        dump_frequent_itemsets_stats(create_temp_df(doubleton, names), 2)
        dump_frequent_itemsets_stats(create_temp_df(triple, names), 3)
        dump_frequent_itemsets_stats(create_temp_df(quadruple, names), 4)
        dump_frequent_itemsets_stats(create_temp_df(quintuple, names), 5)
