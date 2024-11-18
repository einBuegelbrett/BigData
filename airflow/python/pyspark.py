import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col, expr, explode, concat_ws


def get_args():
    """
    Parses Command Line Args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', help='Partion Year To Process', required=True, type=str)
    parser.add_argument('--month', help='Partion Month To Process', required=True, type=str)
    parser.add_argument('--day', help='Partion Day To Process', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/imdb', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/imdb/ratings', required=True, type=str)
    parser.add_argument('--hdfs_target_format', help='HDFS target format, e.g. csv or parquet or...', required=True, type=str)

    return parser.parse_args()

if __name__ == '__main__':
    """
    Format MTG Cards.
    """
    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read raw cards from HDFS
    mtg_cards_df = spark.read.json(f'/user/hadoop/mtg/raw/{args.year}/{args.month}/{args.day}')

    # Explode the array into single elements
    mtg_cards_exploded_df = mtg_cards_df.select(explode('cards').alias('exploded')).select('exploded.*')

    # Replace all null values with empty strings
    mtg_cards_renamed_null_df = mtg_cards_exploded_df .na.fill('')

    # Remove all unnecessary columns
    columns = ['name', 'subtypes', 'text', 'flavor', 'artist', 'multiverseid', 'imageUrl']
    reduced_cards_df = mtg_cards_renamed_null_df.select(*columns)

    # Flatten the subtypes from an array to a comma seperated string
    flattened_subtypes_df = reduced_cards_df.withColumn('subtypes', concat_ws(', ', 'subtypes'))

    # Write data to HDFS
    flattened_subtypes_df.write.format('json').mode('overwrite').save(f'/user/hadoop/mtg/final/{args.year}/{args.month}/{args.day}')
