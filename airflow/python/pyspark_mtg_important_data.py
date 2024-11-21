import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col, expr, explode, concat_ws, when


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

    # Karten und foreignNames extrahieren
    cards_df = mtg_cards_df.select(explode(col("cards")).alias("card"))

    english_cards_df = cards_df.filter(
        col("card.foreignNames").isNull()
    ).select(
        col("card.name").alias("name"),
        when(col("card.imageUrl").isNotNull(), col("card.imageUrl"))
        .otherwise("No Image Available")
        .alias("imageUrl")
    )


    filtered_json = english_cards_df.toJSON().collect()
    print("TEST")
    for json_row in filtered_json:
        print(json_row)

    # Ausgabe in das Zielverzeichnis schreiben
    output_path = f"{args.hdfs_target_dir}/{args.year}/{args.month}/{args.day}"
    english_cards_df.write.format(args.hdfs_target_format).mode('overwrite').save(output_path)

    # Spark-Session beenden
    spark.stop()