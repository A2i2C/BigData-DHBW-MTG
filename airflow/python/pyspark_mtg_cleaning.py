from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col, explode, concat_ws


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=True, type=str)
    parser.add_argument('--month', required=True, type=str)
    parser.add_argument('--day', required=True, type=str)
    return parser.parse_args()


def format_cards():
    args = get_args()

    # Initialize Spark Session
    print("Starting Spark Session")
    spark = SparkSession.builder \
        .appName("MTG Card Cleaning") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop:9001") \
        .getOrCreate()
    print("Spark Session started successfully")

    # Read raw cards from HDFS
    input_path = f'/user/hadoop/mtg_raw/{args.year}/{args.month}/{args.day}'
    print(f"Reading data from {input_path}")
    mtg_cards_df = spark.read.json(input_path)

    # Transformations
    mtg_cards_exploded_df = mtg_cards_df.select(explode(col('cards')).alias
    ('exploded')).select
    ('exploded.*')

    mtg_cards_cleaned_df = mtg_cards_exploded_df.na.fill('').select
    ('name', 'subtypes', 'text', 'artist', 'rarity', 'imageUrl').withColumn
    ('subtypes', concat_ws(', ', 'subtypes'))

    # Write to HDFS
    output_path = f'/user/hadoop/mtg_final/{args.year}/{args.month}/{args.day}'
    print(f"Writing transformed data to {output_path}")
    mtg_cards_cleaned_df.write.format('json').mode('overwrite').save(output_path)
    print("Data written successfully")


if __name__ == '__main__':
    format_cards()
