import logging
import psutil
from collections import defaultdict
from itertools import combinations
from typing import Callable, Iterator, List, Tuple

from data_analysis import CandidateFrequentItemsets, FrequentItemsets, read_frequent_itemsets, save_frequent_itemsets, \
    find_csv, dump_frequent_itemsets_stats, create_temp_df, Itemset, BitMap, Transaction, Algorithm, State
from definitions import RAW_PATH, APRIORI_THRESHOLD, RESULTS, DATASET_PATH, SAVE, DUMP
from spark_utils import read_csv
from utils import timer, get_path, is_empty, read_csvfile, memory_used


@timer
@memory_used
def get_ck(transactions: Iterator[Transaction], monotonicity_filter: Callable[[Itemset], bool],
           bitmaps_filter: Callable[[Itemset], bool],
           state: State) -> Tuple[CandidateFrequentItemsets, List[BitMap]]:
    """
    Scan the file and extract candidate frequent itemsets checking the monotonicity condition and bitmaps.

    :param transactions: iterator of transactions
    :param monotonicity_filter: filter for the monotonicity condition
    :param bitmaps_filter: filter for the bitmaps
    :param state: state of the algorithm
    :return: candidate frequent itemsets
    """
    k = state['k']
    hash_functions = state['hash_functions']
    buckets = state['buckets']
    threshold = state['threshold']

    accumulator = defaultdict(int)
    counters = [defaultdict(int)] * len(hash_functions)
    for row in transactions:
        raw_actors = [int(actor[2:]) for actor in row[1].split('|')]
        for comb in combinations(sorted(raw_actors), k):
            if monotonicity_filter(comb) and bitmaps_filter(comb):
                accumulator[comb] += 1
        for comb in combinations(sorted(raw_actors), k + 1):
            for counter, hash_function in zip(counters, hash_functions):
                bucket = hash_function(buckets, comb)
                counter[bucket] += 1
    return accumulator, [
        {bucket: count >= threshold for bucket, count in counter.items()}
        for counter in counters
    ]


@memory_used
def get_lk(transactions: Iterator[Transaction], state: State) -> Tuple[FrequentItemsets, List[BitMap]]:
    """
    Extract frequent itemsets checking the support.

    :param transactions: iterator of transactions
    :param state: state of the algorithm
    :return:
    """
    size = state['k']
    old_lk = state['lk']
    threshold = state['threshold']
    bitmaps = state['bitmaps']
    old_buckets = state['old_buckets']
    hash_functions = state['hash_functions']
    force = state['force']

    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'csv', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_frequent_itemsets(path), []

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    def monotonicity(itemset: Itemset) -> bool:
        return size == 1 or all([item in old_lk for item in combinations(itemset, size - 1)])

    def check_bitmaps(itemset: Itemset) -> bool:
        return all([bitmap[hash_function(old_buckets, itemset)]]
                   for bitmap, hash_function in zip(bitmaps, hash_functions))

    ck, bitmaps = get_ck(transactions, monotonicity, check_bitmaps, state)
    lk = {item: support
          for item, support in ck.items()
          if support >= threshold}
    logging.info(f'Found {len(lk)} frequent itemsets')

    if SAVE:
        save_frequent_itemsets(lk, path)

    return lk, bitmaps


def apriori_algorithm(transactions: Callable[[], Iterator[Transaction]], state: State) -> Algorithm:
    """
    Executing apriori algorithm starting from data and a given threshold.

    :param transactions: callable to an iterator of transactions
    :param state: state of the algorithm:
        - threshold: threshold for the apriori algorithm
        - k: size of the itemsets
        - lk: frequent itemsets with size k-1
        - bitmaps: bitmaps previously calculated for frequent itemset with size k+1
        - hash_functions: hash function to be applied to candidate frequent itemsets
        - old_buckets: number of buckets for the previous bitmaps
        - buckets: number of buckets for the current bitmaps
        - force: to force recalculating frequent itemsets
    :return: dict of frequent itemsets
    """
    length = len(state['hash_functions'])
    buckets = int(psutil.virtual_memory().free * 0.7 / length)
    state = State(k=1, lk={}, bitmaps=[], old_buckets=buckets, buckets=buckets) + state

    while state['k'] == 1 or state['lk']:
        lk, bitmaps = get_lk(transactions(), state)
        state['lk'] = lk
        state['bitmaps'] = bitmaps
        state['k'] += 1
        old_buckets, buckets = buckets, int(psutil.virtual_memory().free * 0.7 / length)

        yield state


def hash_function1(buckets: int, itemset: Itemset) -> int:
    acc = 1
    for item in itemset:
        acc *= item

    return acc % buckets


def hash_function2(buckets: int, itemset: Itemset) -> int:
    coeff = list(range(1, len(itemset) + 1))

    acc = 1
    for c, item in zip(coeff, itemset):
        acc *= c * item

    return acc % buckets


if __name__ == '__main__':
    file = find_csv(get_path(RAW_PATH, 'csv'))

    algorithm = apriori_algorithm(lambda: read_csvfile(file), State(threshold=APRIORI_THRESHOLD, force=True,
                                                                    hash_functions=(hash_function1, hash_function2)))

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
