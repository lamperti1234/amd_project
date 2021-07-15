import logging
from collections import defaultdict
from itertools import combinations
from typing import Iterable, Tuple

from pyspark import RDD, Broadcast, Accumulator

from data_analysis import find_csv, CandidateFrequentItemsets, Transaction, FrequentItemsets, read_frequent_itemsets, \
    save_frequent_itemsets, Itemset, dump_frequent_itemsets_stats, create_temp_df, Algorithm, State
from data_analysis.apriori import apriori_algorithm
from definitions import RAW_PATH, SON_CHUNKS, RESULTS, APRIORI_THRESHOLD, DATASET_PATH, SAVE, DUMP
from spark_utils import read_csv, get_spark, DictParam
from utils import get_path, timer, memory_used, is_empty


@timer
@memory_used
def get_ck(rdd: RDD, old_state: Broadcast, new_state: Accumulator) -> CandidateFrequentItemsets:
    """
    Scan the chunk and extract candidate frequent itemsets.

    :param rdd: rdd which contains transactions
    :param old_state: previous state of the algorithm
    :param new_state: next state of the algorithm
    :return: candidate frequent itemsets
    """

    def apply_algorithm(index: int, transactions: Iterable[Transaction]) -> CandidateFrequentItemsets:
        name = f'algorithms_{index}'
        if name not in old_state.value:
            buckets = [bucket for bucket in transactions]
            state = old_state.value
            state['threshold'] *= len(buckets) / old_state.value['n']
            alg = old_state.value['algorithm'](lambda: buckets, State(**old_state.value))
            new_state.add({
                name: state
            })
        else:
            state = old_state.value[name]
            state['k'] = old_state.value['k']
            state['lk'] = old_state.value['lk']
            alg = old_state.value['algorithm'](lambda: transactions, State(**state))

        state = next(alg)
        new_state.add({name: state.state})
        return {itemset: 1 for itemset in state['lk'].items()}

    return rdd.mapPartitionsWithIndex(apply_algorithm).reduceByKey(lambda x, y: x).map(lambda x: x[0]).collect()


@memory_used
def get_lk(rdd: RDD, state: State) -> FrequentItemsets:
    """
    Extract frequent itemsets checking the support.

    :param rdd: rdd which contains transactions
    :param state: previous state of the algorithm
    :return:
    """
    threshold = state['threshold']
    size = state['k']
    force = state['force']

    path = get_path(RESULTS, f'apriori_{threshold}_{size}', 'csv', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_frequent_itemsets(path)

    logging.info(f'Executing apriori algorithm with {size} items and {threshold} as threshold')

    old_state = get_spark().sparkContext.broadcast(state.state)
    new_state = get_spark().sparkContext.accumulator(state.state, DictParam())
    ck = get_spark().sparkContext.broadcast(get_ck(rdd, old_state, new_state))
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

    if SAVE:
        save_frequent_itemsets(lk, path)

    return lk


def son_algorithm(rdd: RDD, state: State) -> Algorithm:
    """
    Executing son algorithm starting from data and a given threshold.

    :param rdd: dataframe which contains transactions
    :param state: state of the algorithm:
        - threshold: threshold for the algorithm
        - k: size of the itemsets
        - lk: frequent itemsets with size k-1
        - n: number of distinct buckets
        - force: to force recalculating frequent itemsets
        - algorithm: algorithm to extract frequent itemsets
    :return: dict of frequent itemsets
    """
    state = State(n=rdd.count(), chunks=rdd.getNumPartitions(), k=1, lk={}) + state

    while state['k'] == 1 or state['lk']:
        state['lk'] = get_lk(rdd, state)
        state['k'] += 1

        yield state


if __name__ == '__main__':
    file = find_csv(get_path(RAW_PATH, 'csv'))

    data = read_csv(file).repartition(SON_CHUNKS).rdd.map(lambda row: (row['movie'], row['actors'])).persist()

    algorithm = son_algorithm(data, State(threshold=APRIORI_THRESHOLD, force=True, algorithm=apriori_algorithm))

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
