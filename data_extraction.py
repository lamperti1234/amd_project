import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from definitions import DATASET_PATH, RAW_PATH
from spark_utils import read_csv_df, save_parquet, read_parquet, save_csv_df
from utils import get_path, is_empty, timer


@timer
def extract_data(force: bool = False) -> DataFrame:
    """
    Extract dataframe using movies, actors and names.

    :return: a dataframe
    """
    path = get_path(RAW_PATH, delete=force)
    raw_parquet = get_path(path, 'parquet')
    raw_csv = get_path(path, 'csv')

    if not is_empty(raw_parquet):
        logging.info('Reading already extracted data')

        return read_parquet(raw_parquet)

    if not is_empty(raw_csv):
        logging.info('Reading already extracted data')

        return read_csv_df(raw_csv)

    logging.info('Extracing data from movies, actors and names')

    movies = read_csv_df(get_path(DATASET_PATH, 'title.basics.tsv.gz'), sep='\t').filter('titleType = "movie"')
    actors = (read_csv_df(get_path(DATASET_PATH, 'title.principals.tsv.gz'), sep='\t')
              .filter('category IN ("actor", "actress")'))
    names = read_csv_df(get_path(DATASET_PATH, 'name.basics.tsv.gz'), sep='\t')

    df = (actors
          .join(movies, on='tconst')
          .join(names, on='nconst', how='left')
          .select('tconst', 'nconst', 'primaryName')
          .withColumnRenamed('tconst', 'movie')
          .withColumnRenamed('nconst', 'actor1')
          .withColumnRenamed('primaryName', 'actor1Name')
          ).persist()

    save_parquet(df, raw_parquet)
    save_csv_df(df.select('movie', 'actor1')
                .groupBy('movie').agg(F.collect_list('actor1').alias('actors'))
                .withColumn('actors', F.concat_ws('|', 'actors'))
                .coalesce(1), raw_csv, header=False)

    return df


if __name__ == '__main__':
    extract_data(force=True).show()
