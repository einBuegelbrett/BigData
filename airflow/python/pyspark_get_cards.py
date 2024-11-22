import argparse
import json
import requests
import os
from pyspark.sql import SparkSession

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', help='Partition Year To Process', required=True, type=str)
    parser.add_argument('--month', help='Partition Month To Process', required=True, type=str)
    parser.add_argument('--day', help='Partition Day To Process', required=True, type=str)
    parser.add_argument('--hdfs-path', help='Base HDFS Path', default='/user/hadoop/mtg/raw/', type=str)
    return parser.parse_args()

def fetch_data(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

if __name__ == '__main__':
    args = get_args()

    spark = SparkSession.builder \
        .appName("MTG Data Processor") \
        .config("spark.rpc.message.maxSize", "256") \
        .getOrCreate()

    mtg_cards = []
    url = 'https://api.magicthegathering.io/v1/cards'

    # Go through the different pages of the API
    while url:
        print(f"Fetching data from {url}")
        response = fetch_data(url)
        if not response:
            break
        mtg_cards.append(json.dumps(response.json()))
        url = response.links.get('next', {}).get('url')
        # break

    if mtg_cards:
        mtg_cards_rdd = spark.sparkContext.parallelize(mtg_cards)
        mtg_cards_df = spark.read.option('multiline', 'true').json(mtg_cards_rdd)
        hdfs_path = os.path.join(args.hdfs_path, args.year, args.month, args.day)

        mtg_cards_df.write.format('json').mode('overwrite').save(hdfs_path)
        print(f"Data successfully written to {hdfs_path}")
    else:
        print("No data downloaded.")

    spark.stop()
