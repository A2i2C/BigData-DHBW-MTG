import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col, expr, explode, concat_ws

def get_args():
    """
    Parses Command Line Args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', help='Partition Year To Process', required=True, type=str)
    parser.add_argument('--month', help='Partition Month To Process', required=True, type=str)
    parser.add_argument('--day', help='Partition Day To Process', required=True, type=str)

    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read raw cards from HDFS
    mtg_cards_df = spark.read.json(f'/user/hadoop/mtg_raw/{args.year}/{args.month}/{args.day}/cards_{args.year}-{args.month}-{args.day}.json')

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
    filtered_cards_df.write.format('json') \
        .mode('overwrite') \
        .save(f'/user/hadoop/mtg_final/{args.year}/{args.month}/{args.day}')
