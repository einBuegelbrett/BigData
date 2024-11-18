from pyspark.sql import SparkSession
import argparse

def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on IMDb data stored within HDFS.')
    parser.add_argument('--hdfs_source_dir', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', required=True, type=str)
    parser.add_argument('--hdfs_target_format', required=True, type=str)
    
    return parser.parse_args()

if __name__ == '__main__':
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    spark = SparkSession.builder.appName("Card Data Processing").getOrCreate()

    # Read Title Basics from HDFS
    cards_dataframe = spark.read.format('json').load(args.hdfs_source_dir)

    # Print Schema to verify column names
    cards_dataframe.printSchema()

    # Explode the cards array to flatten the structure
    exploded_df = cards_dataframe.select(explode("cards").alias("card"))

    # Select the required fields
    important_data_df = exploded_df.select("card.name", "card.multiverseid")

    # Write data to HDFS
    important_data_df.write.format(args.hdfs_target_format).save(args.hdfs_target_dir)

    spark.stop()

