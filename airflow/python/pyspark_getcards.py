import argparse
import json
import requests
import os
from pyspark.sql import SparkSession

LIMIT = 10 #Anzahl der Seiten, für Zeit und Ressourcen sparen

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=True, type=str)
    parser.add_argument('--month', required=True, type=str)
    parser.add_argument('--day', required=True, type=str)
    parser.add_argument('--hdfs_path', required=True, type=str)
    return parser.parse_args()

def fetch_data(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response
    
if __name__ == '__main__':
    args = get_args()

    spark = SparkSession.builder \
        .appName("MTG Data Processor") \
        .getOrCreate()

    mtg_cards = []
    url = 'https://api.magicthegathering.io/v1/cards'

    counter = 0

    while url:  
        counter += 1
        response = fetch_data(url)
        mtg_cards.append(json.dumps(response.json()))
        url = response.links.get('next', {}).get('url')
        if counter > LIMIT:
            break

    if mtg_cards:
        mtg_cards_rdd = spark.sparkContext.parallelize(mtg_cards)
        mtg_cards_df = spark.read.option('multiline', 'true').json(mtg_cards_rdd)
        hdfs_path = os.path.join(args.hdfs_path, f"cards_{args.year}-{args.month}-{args.day}.json")

        mtg_cards_df.write.format('json').mode('overwrite').save(hdfs_path)
    