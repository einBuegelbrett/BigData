import pyspark
import argparse
import json
import psycopg2
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, expr, explode, concat_ws

def get_args():
    """
    Parses command line args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', help='Partition Year To Process', required=True, type=str)
    parser.add_argument('--month', help='Partition Month To Process', required=True, type=str)
    parser.add_argument('--day', help='Partition Day To Process', required=True, type=str)

    return parser.parse_args()


if __name__ == '__main__':
    """
    Export final cards to PostgreSQL.
    """
    # Parse Command Line Args
    args = get_args()

    spark = SparkSession.builder \
        .appName("spark_export_cards_to_postgresql") \
        .getOrCreate()

    # Read final cards as json from HDFS
    processed_cards_df = spark.read.json(f'/user/hadoop/mtg/final/{args.year}/{args.month}/{args.day}')

    # Convert dataframe to json
    mtg_cards_json = processed_cards_df.toJSON().map(lambda j: json.loads(j)).collect()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgresql",  # Name des PostgreSQL-Containers
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            name VARCHAR(255),
            subtypes VARCHAR(255),
            text TEXT,
            flavor TEXT,
            artist VARCHAR(255),
            multiverseid VARCHAR(255),
            imageUrl VARCHAR(255)
        );
    """)
    conn.commit()

    # Remove all records from the table
    cur.execute("DELETE FROM cards")
    conn.commit()

    '''
    # Insert new records
    for card in mtg_cards_json:
        columns = card.keys()
        values = [card[column] for column in columns]
        insert_statement = f"INSERT INTO cards ({', '.join(columns)}) VALUES %s"
        cur.execute(insert_statement, (tuple(values),))
    '''
    for row in processed_cards_df.collect():
        cur.execute("""
            INSERT INTO cards (name, subtypes, text, flavor, artist, multiverseid, imageUrl)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['name'], row['subtypes'], row['text'], row['flavor'], row['artist'], row['multiverseid'], row['imageUrl']))

    conn.commit()

    # Close the connection
    cur.close()
    conn.close()
