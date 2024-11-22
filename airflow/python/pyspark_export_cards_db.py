import argparse
import json
import psycopg2
from pyspark.sql import SparkSession

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
    args = get_args()
    spark = SparkSession.builder.appName("spark_export_cards_to_postgresql").getOrCreate()

    # Read final cards as json from HDFS
    processed_cards_df = spark.read.json(f'/user/hadoop/mtg/final/{args.year}/{args.month}/{args.day}')

    # Convert dataframe to json
    mtg_cards_json = processed_cards_df.toJSON().map(lambda j: json.loads(j)).collect()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgresql",
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    # Remove all records from the table to remove any data that was already present
    cur.execute("DROP TABLE IF EXISTS cards;")
    conn.commit()

    # Create a table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            name VARCHAR(255),
            imageUrl VARCHAR(255)
        );
    """)
    conn.commit()

    # Insert data into the table
    for row in processed_cards_df.collect():
        cur.execute("""
            INSERT INTO cards (name, imageUrl)
            VALUES (%s, %s)
        """, (row['name'], row['imageUrl']))

    conn.commit()

    # Close the connection
    cur.close()
    conn.close()
