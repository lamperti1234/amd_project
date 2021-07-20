import logging
from collections import defaultdict
from itertools import combinations
from typing import Iterable, Tuple, Callable, Any, Set

from pyspark import RDD, Broadcast, Accumulator

from data_analysis import find_csv, CandidateFrequentItemsets, Transaction, FrequentItemsets, Itemset, \
    dump_frequent_itemsets_stats, create_temp_df, Algorithm, State, read_frequent_itemsets, save_frequent_itemsets
from data_analysis.apriori import apriori_algorithm
from definitions import RAW_PATH, SON_CHUNKS, APRIORI_THRESHOLD, DATASET_PATH, DUMP, RESULTS, SAVE
from spark_utils import read_csv_df, get_spark, DictParam, read_csv_rdd
from utils import get_path, timer, is_empty


@timer
def get_ck(rdd: RDD, algorithm: Callable[[Any, ...], Algorithm],
           old_state: Broadcast, new_state: Accumulator) -> Set[Itemset]:
    """
    Scan the chunk and extract candidate frequent itemsets.

    :param rdd: rdd which contains transactions
    :param algorithm: algorithm to extract frequent itemsets
    :param old_state: previous state of the algorithm
    :param new_state: next state of the algorithm
    :return: candidate frequent itemsets
    """

    def apply_algorithm(index: int, transactions: Iterable[Transaction]) -> CandidateFrequentItemsets:
        name = f'partitions_{index}'
        if name not in old_state.value:
            buckets = [bucket for bucket in transactions]
            state = old_state.value
            state['threshold'] *= len(buckets) / old_state.value['n']
            state['save'] = False
            alg = algorithm(lambda: buckets, State(**old_state.value))
        else:
            state = old_state.value[name]
            state['lk'] = old_state.value['lk']
            state['save'] = False
            alg = algorithm(lambda: transactions, State(**state))

        state = next(alg)
        new_state.add({name: state.state})
        return {itemset: 1 for itemset in state['lk'].items()}

    # set is useful to lookup. RDD cannot be used inside mapPartitions
    return set(rdd.mapPartitionsWithIndex(apply_algorithm).map(lambda x: x[0]).collect())


@timer
def get_lk(rdd: RDD, algorithm: Callable[[Any, ...], Algorithm], state: State) -> FrequentItemsets:
    """
    Extract frequent itemsets checking the support.

    :param rdd: rdd which contains transactions
    :param algorithm: algorithm to extract frequent itemsets
    :param state: previous state of the algorithm
    :return:
    """
    threshold = state['threshold']
    size = state['k']
    force = state['force']

    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'csv', 'son', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_frequent_itemsets(path)

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    old_state = get_spark().sparkContext.broadcast(state.state)
    new_state = get_spark().sparkContext.accumulator(state.state, DictParam())
    ck = get_spark().sparkContext.broadcast(get_ck(rdd, algorithm, old_state, new_state))
    state.update(State(**new_state.value))

    def get_support(transactions: Iterable[Transaction]) -> Iterable[Tuple[Itemset, int]]:
        accumulator = defaultdict(int)
        for row in transactions:
            raw_actors = [int(actor[2:]) for actor in row[1].split('|')]
            for comb in combinations(sorted(raw_actors), size):
                if comb in ck.value:
                    accumulator[comb] += 1
        return accumulator.items()

    lk = dict(rdd.mapPartitions(get_support)
              .reduceByKey(lambda x, y: x + y)
              .filter(lambda x: x[1] >= threshold)
              .collect())
    logging.info(f'Found {len(lk)} frequent itemsets')

    if state['save']:
        save_frequent_itemsets(lk, path)

    return lk


def son_algorithm(rdd: RDD, algorithm: Callable[[Any, ...], Algorithm], state: State) -> Algorithm:
    """
    Executing son algorithm starting from data and a given threshold.

    :param rdd: dataframe which contains transactions
    :param algorithm: algorithm to extract frequent itemsets
    :param state: state of the algorithm:
        - threshold: threshold for the algorithm
        - k: size of the itemsets
        - lk: frequent itemsets with size k-1
        - n: number of distinct buckets
        - force: to force recalculating frequent itemsets
    :return: dict of frequent itemsets
    """
    state = State(n=rdd.count(), chunks=rdd.getNumPartitions(), k=1, lk={}, force=False, save=SAVE) + state

    while state['k'] == 1 or state['lk']:
        state['lk'] = get_lk(rdd, algorithm, state)
        state['k'] += 1

        yield state


if __name__ == '__main__':
    file = find_csv(get_path(RAW_PATH, 'csv'))

    data = read_csv_rdd(file).repartition(SON_CHUNKS).map(lambda row: (row[0], row[1])).persist()

    algorithm = son_algorithm(data, apriori_algorithm, State(threshold=APRIORI_THRESHOLD))

    singleton = next(algorithm)['lk']
    doubleton = next(algorithm)['lk']
    triple = next(algorithm)['lk']
    quadruple = next(algorithm)['lk']
    quintuple = next(algorithm)['lk']

    if DUMP:
        names = read_csv_df(get_path(DATASET_PATH, 'name.basics.tsv.gz'), sep='\t')
        dump_frequent_itemsets_stats(create_temp_df(singleton, names), 1)
        dump_frequent_itemsets_stats(create_temp_df(doubleton, names), 2)
        dump_frequent_itemsets_stats(create_temp_df(triple, names), 3)
        dump_frequent_itemsets_stats(create_temp_df(quadruple, names), 4)
        dump_frequent_itemsets_stats(create_temp_df(quintuple, names), 5)
