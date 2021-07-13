import logging
from collections import defaultdict
from itertools import combinations
from typing import Callable, Iterator

from data_analysis import CandidateFrequentItemsets, FrequentItemsets, read_frequent_itemsets, save_frequent_itemsets, \
    find_csv, dump_frequent_itemsets_stats, create_temp_df, Itemset
from definitions import RAW_PATH, APRIORI_THRESHOLD, RESULTS, DATASET_PATH
from spark_utils import read_csv
from utils import timer, get_path, is_empty, read_csvfile, memory_used


@timer
@memory_used
def get_ck(k: int, monotonicity_filter: Callable[[Itemset], int]) -> CandidateFrequentItemsets:
    """
    Scan the file and extract candidate frequent itemsets checking the monotonicity condition.

    :param k: size of the itemset
    :param monotonicity_filter: filter for the monotonicity condition
    :return: candidate frequent itemsets
    """
    accumulator = defaultdict(int)
    for row in read_csvfile(file):
        raw_actors = [int(actor[2:]) for actor in row[1].split('|')]
        for comb in combinations(sorted(raw_actors), k):
            if monotonicity_filter(comb):
                accumulator[comb] += 1
    return accumulator


@memory_used
def get_lk(size: int, old_lk: FrequentItemsets, threshold: float, force: bool = False) -> FrequentItemsets:
    """
    Extract frequent itemsets checking the support.

    :param size: size of the itemset
    :param old_lk: frequent itemset with size-1
    :param threshold: threshold for the apriori algorithm
    :param force: to force recalculating frequent itemsets
    :return:
    """
    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'csv', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_frequent_itemsets(path)

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    if size == 1:
        ck = get_ck(size, lambda _: True)
    else:
        ck = get_ck(size, lambda itemset: all([item in old_lk for item in combinations(itemset, size - 1)]))

    lk = {item: support
          for item, support in ck.items()
          if support > threshold}
    save_frequent_itemsets(lk, path)

    return lk


def apriori_algorithm(threshold: int, force: bool = False) -> Iterator[FrequentItemsets]:
    """
    Executing apriori algorith starting from data and a given threshold.

    :param threshold: threshold for the apriori algorithm
    :param force: to force recalculating frequent itemsets
    :return: dict of frequent itemsets
    """
    k = 1
    lk = {}

    while k == 1 or lk:
        lk = get_lk(k, lk, threshold, force=force)

        yield lk

        k += 1


if __name__ == '__main__':
    file = find_csv(get_path(RAW_PATH, 'csv'))

    algorithm = apriori_algorithm(APRIORI_THRESHOLD, force=True)

    singleton = next(algorithm)
    doubleton = next(algorithm)
    triple = next(algorithm)
    quadruple = next(algorithm)
    quintuple = next(algorithm)

    names = read_csv(get_path(DATASET_PATH, 'name.basics.tsv.gz'), sep='\t')
    dump_frequent_itemsets_stats(create_temp_df(singleton, names), 1)
    dump_frequent_itemsets_stats(create_temp_df(doubleton, names), 2)
    dump_frequent_itemsets_stats(create_temp_df(triple, names), 3)
    dump_frequent_itemsets_stats(create_temp_df(quadruple, names), 4)
    dump_frequent_itemsets_stats(create_temp_df(quintuple, names), 5)
