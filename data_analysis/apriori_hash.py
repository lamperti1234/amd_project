import logging
import psutil
from collections import defaultdict
from itertools import combinations
from typing import Callable, Iterator, List, Tuple

from data_analysis import CandidateFrequentItemsets, FrequentItemsets, read_frequent_itemsets, save_frequent_itemsets, \
    find_csv, dump_frequent_itemsets_stats, create_temp_df, Itemset, BitMap
from definitions import RAW_PATH, APRIORI_THRESHOLD, RESULTS, DATASET_PATH
from spark_utils import read_csv
from utils import timer, get_path, is_empty, read_csvfile, memory_used


@timer
@memory_used
def get_ck(k: int, monotonicity_filter: Callable[[Itemset], bool], threshold: int,
           bitmaps: Callable[[Itemset], bool], buckets: int,
           *hash_functions: Callable[[int, Itemset], int]) -> Tuple[CandidateFrequentItemsets, List[BitMap]]:
    """
    Scan the file and extract candidate frequent itemsets checking the monotonicity condition and bitmaps.

    :param k: size of the itemset
    :param monotonicity_filter: filter for the monotonicity condition
    :param threshold: threshold value for bitmaps
    :param bitmaps: bitmaps calculated previously
    :param buckets: number of buckets available for each hash function
    :param hash_functions: hash function to be applied to candidate frequent itemsets
    :return: candidate frequent itemsets
    """
    accumulator = defaultdict(int)
    counters = [defaultdict(int)] * len(hash_functions)
    for row in read_csvfile(file):
        raw_actors = [int(actor[2:]) for actor in row[1].split('|')]
        for comb in combinations(sorted(raw_actors), k):
            if monotonicity_filter(comb) and bitmaps(comb):
                accumulator[comb] += 1
        for comb in combinations(sorted(raw_actors), k + 1):
            for counter, hash_function in zip(counters, hash_functions):
                bucket = hash_function(buckets, comb)
                counter[bucket] += 1
    return accumulator, [
        {bucket: count > threshold for bucket, count in counter.items()}
        for counter in counters
    ]


@memory_used
def get_lk(size: int, old_lk: FrequentItemsets, threshold: float, bitmaps: List[BitMap], buckets: int,
           *hash_functions: Callable[[int, Itemset], int],
           force: bool = False) -> Tuple[FrequentItemsets, List[BitMap]]:
    """
    Extract frequent itemsets checking the support.

    :param size: size of the itemset
    :param old_lk: frequent itemset with size-1
    :param threshold: threshold for the apriori algorithm
    :param bitmaps: bitmaps calculated previously
    :param buckets: number of buckets available for each hash function
    :param force: to force recalculating frequent itemsets
    :param hash_functions: hash function to be applied to candidate frequent itemsets
    :return:
    """
    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'csv', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_frequent_itemsets(path), []

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    def monotonicity(itemset: Itemset) -> bool:
        return size == 1 or all([item in old_lk for item in combinations(itemset, size - 1)])

    def check_bitmaps(itemset: Itemset) -> bool:
        return all([bitmap[hash_function(buckets, itemset)]] for bitmap, hash_function in zip(bitmaps, hash_functions))

    ck, bitmaps = get_ck(size, monotonicity, threshold, check_bitmaps, buckets, *hash_functions)
    lk = {item: support
          for item, support in ck.items()
          if support > threshold}
    save_frequent_itemsets(lk, path)

    return lk, bitmaps


def apriori_algorithm(threshold: int, *hash_functions: Callable[[int, Itemset], int],
                      force: bool = False) -> Iterator[FrequentItemsets]:
    """
    Executing apriori algorith starting from data and a given threshold.

    :param threshold: threshold for the apriori algorithm
    :param hash_functions: hash function to be applied to candidate frequent itemsets
    :param force: to force recalculating frequent itemsets
    :return: dict of frequent itemsets
    """
    k = 1
    lk = {}
    bitmaps = []
    # to avoid division by zero
    length = max(len(hash_functions), 1)
    buckets = int(psutil.virtual_memory().free * 0.7 / length)

    while k == 1 or lk:
        lk, bitmaps = get_lk(k, lk, threshold, bitmaps, buckets, *hash_functions, force=force)

        yield lk

        k += 1


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

    algorithm = apriori_algorithm(APRIORI_THRESHOLD, hash_function1, hash_function2, force=True)

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
