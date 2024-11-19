import pyspark
import argparse
import json
import pymysql

from pyspark.sql import SparkSession
from pyspark import SparkContext


def get_args():
    """
    Parses command line args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',
                        help='Partition Year To Process',
                        required=True, type=str)
    parser.add_argument('--month',
                        help='Partition Month To Process',
                        required=True, type=str)
    parser.add_argument('--day',
                        help='Partition Day To Process',
                        required=True, type=str)
    
    return parser.parse_args()


def export_cards():
    """
    Export final cards to MySQL.
    """
    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read final cards as json from HDFS
    mtg_cards_df = spark.read.json(f'/user/hadoop/mtg/final/{args.year}/{args.month}/{args.day}')

    # Optional: Hier kannst du Transformationen oder Bereinigungen auf deinem DataFrame durchführen
    # Zum Beispiel Spalten umbenennen oder transformieren, falls nötig
    # mtg_cards_df = mtg_cards_df.select("id", "name", "type", ...)

    # Convert DataFrame to list of dictionaries (JSON-like format)
    mtg_cards_json = mtg_cards_df.collect()

    # Connect to MySQL
    connection = pymysql.connect(host='localhost', user='user', password='userpassword', database='mtg_database')
    cursor = connection.cursor()

    # Create a SQL insert query (Anpassen je nach Struktur deiner Tabelle)
    insert_query = """
    INSERT INTO mtg_cards (name, artist, type, manaCost, rarity)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    # Loop through the records and insert them into MySQL
    for card in mtg_cards_json:
        # Anpassung je nach den Namen der Spalten in deinem DataFrame und der MySQL-Tabelle
        values = (
            card['name'],
            card.get('artist', None),  # Beispiel für optionale Felder
            card.get('type', None),
            card.get('manaCost', None),
            card.get('rarity', None)
        )
        
        cursor.execute(insert_query, values)

    # Commit und Verbindung schließen
    connection.commit()
    cursor.close()
    connection.close()


if __name__ == '__main__':
    export_cards()
