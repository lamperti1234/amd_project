import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import Iterator

from data_extraction import extract_data
from definitions import PREPROCESSED_PATH, APRIORI_THRESHOLD
from spark_utils import check_empty, save_parquet, read_parquet
from utils import get_path, is_empty


def apriori(df: DataFrame, threshold: float, *cols: str, force: bool = False) -> DataFrame:
    size = len(cols)
    path = get_path(PREPROCESSED_PATH, f'apriori_{threshold}_{size}', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_parquet(path)

    logging.info(f'Executing apriori algorithm with {cols} and {threshold} as threshold')

    count = Window.partitionBy(*cols)
    column = cols[-1]
    next_column = f'actor{size}'

    df_with_count = df.withColumn('count', F.count('*').over(count))
    df_filtered = df_with_count.filter(f'count > {threshold}')

    small_df = (df_filtered.select('movie', f'{column}Name', *cols)
                .withColumnRenamed(column, next_column)
                .withColumnRenamed(f'{column}Name', f'{next_column}Name'))

    # join on movie and all actors needed
    join_cond = ['movie'] + list(cols)[:-1]
    df = df_filtered.join(small_df, on=join_cond).filter(f'{column} < {next_column}').persist()
    save_parquet(df, path)

    return df


def apriori_algorithm(df: DataFrame, threshold: int) -> Iterator[DataFrame]:
    columns = ['actor']
    while not check_empty(df):
        df = apriori(df, threshold, *columns)
        stats(df)
        yield df

        columns.append(f'actor{len(columns)}')


def stats(df: DataFrame) -> None:
    logging.info('Calculating stats for dataframe')
    df.show()
    df.printSchema()
    stat = df.select(
        F.count('*').alias('count'),
        F.avg('count').alias('avg'),
        F.max('count').alias('max'),
        F.min('count').alias('min')
    )
    stat.show()


if __name__ == '__main__':
    dataframe = extract_data()
    algorithm = apriori_algorithm(dataframe, APRIORI_THRESHOLD)
    singleton = next(algorithm)
    couple = next(algorithm)
    triplet = next(algorithm)
    quadruplet = next(algorithm)
