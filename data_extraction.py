import logging

from pyspark.sql import DataFrame

from definitions import DATASET_PATH, PREPROCESSED_PATH
from spark_utils import read_csv, save_parquet, read_parquet
from utils import get_path, is_empty


def extract_data(force: bool = False) -> DataFrame:
    """
    Extract dataframe using movies, actors and names.

    :return: a dataframe
    """
    path = get_path(PREPROCESSED_PATH, 'raw', delete=force)

    if not is_empty(path):
        logging.info('Reading already extracted data')

        return read_parquet(path)

    logging.info('Extracing data from movies, actors and names')

    movies = read_csv(get_path(DATASET_PATH, 'title.basics.tsv.gz'), sep='\t').filter('titleType = "movie"')
    actors = (read_csv(get_path(DATASET_PATH, 'title.principals.tsv.gz'), sep='\t')
              .filter('category IN ("actor", "actress")'))
    names = read_csv(get_path(DATASET_PATH, 'name.basics.tsv.gz'), sep='\t')

    df = (actors
          .join(movies, on='tconst')
          .join(names, on='nconst', how='left')
          .select('tconst', 'nconst', 'primaryTitle', 'primaryName')
          .withColumnRenamed('tconst', 'movie')
          .withColumnRenamed('nconst', 'actor')
          .withColumnRenamed('primaryTitle', 'title')
          .withColumnRenamed('primaryName', 'actorName')
          ).persist()
    save_parquet(df, path)

    return df


if __name__ == '__main__':
    extract_data().show()
