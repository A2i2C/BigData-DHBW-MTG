import argparse
import json
import psycopg2
from pyspark.sql import SparkSession

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', help='Partition Year To Process', required=True, type=str)
    parser.add_argument('--month', help='Partition Month To Process', required=True, type=str)
    parser.add_argument('--day', help='Partition Day To Process', required=True, type=str)
    parser.add_argument('--hdfs_path', help='HDFS Path To Read Final Data From', required=True, type=str)
    return parser.parse_args()

def commitsql(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def drop_table(conn):
    sql = """
        DROP TABLE IF EXISTS cards;
    """
    commitsql(conn, sql)

def create_table(conn):
    sql = """
        CREATE TABLE IF NOT EXISTS cards (
    name VARCHAR(255),
    subtypes VARCHAR(255),
    text TEXT,
    artist VARCHAR(255),
    rarity VARCHAR(50),
    imageUrl VARCHAR(255)
);
    """
    commitsql(conn, sql)



if __name__ == '__main__':
    args = get_args()
    spark = SparkSession.builder \
    .appName("spark_export_cards_to_postgresql") \
    .getOrCreate()

    # Read final cards as json from HDFS
    processed_cards_df = spark.read.json(args.hdfs_path)

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

    # Drop and create table
    cur = conn.cursor()
    drop_table(conn)
    create_table(conn)
    for row in processed_cards_df.collect():
            cur.execute("""
                INSERT INTO cards (name, subtypes, text, artist, rarity, imageUrl)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['name'], row['subtypes'], row['text'], row['artist'], row['rarity'], row['imageUrl']))

    conn.commit()

    cur.close()
    conn.close()