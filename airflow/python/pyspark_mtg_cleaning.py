import pyspark
from pyspark.sql import SparkSession
import argparse
import os
from pyspark.sql.functions import col, explode, concat_ws

def get_args():
    """
    Parses Command Line Args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', help='Partition Year To Process', required=True, type=str)
    parser.add_argument('--month', help='Partition Month To Process', required=True, type=str)
    parser.add_argument('--day', help='Partition Day To Process', required=True, type=str)
    parser.add_argument('--hdfs_raw', help='HDFS Path To Read Data From', required=True, type=str)
    parser.add_argument('--hdfs_final', help='HDFS Path To Write Data To', required=True, type=str)
    parser.add_argument('--format', help='Output Format', required=True, type=str)


    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read raw cards from HDFS
    hdfs_raw_path = os.path.join(args.hdfs_raw, f"cards_{args.year}-{args.month}-{args.day}.json")
    mtg_cards_df = spark.read.json(hdfs_raw_path)

    # Explode the array into single elements
    mtg_cards_exploded_df = mtg_cards_df \
        .select(explode('cards').alias('exploded')) \
        .select('exploded.*')

    # Replace all null values with empty strings
    mtg_cards_cleaned_df = mtg_cards_exploded_df \
        .na.fill('') \
        .select(
            'name',
            'subtypes',
            'text',
            'artist',
            'rarity',
            'imageUrl'
        ) \
        .withColumn('subtypes', concat_ws(', ', 'subtypes'))

    # Filter out rows with empty imageUrl
    filtered_cards_df = mtg_cards_cleaned_df.filter(col('imageUrl') != '')

    # Write data to HDFS
    filtered_cards_df.write.format(args.format) \
        .mode('overwrite') \
        .save(args.hdfs_final)
