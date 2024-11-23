# -*- coding: utf-8 -*-

"""
Title: MTG Dag 
Author: Aziz Carducci
Description: MTG DAG for the module BigData it downloads MTG Data via an API puts them into HDFS and creates HiveTable.
See Lecture Material: https://github.com/marcelmittelstaedt/BigData
"""

from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
import requests
import json
import os

args = {
    'owner': 'airflow_mtg'
}

# Definiere den DAG
dag = DAG(
    'mtg_data_etl',  # Name des DAGs
    description='ETL Pipeline for MTG Trading Cards',
    schedule_interval='@daily',  # Der DAG wird täglich ausgeführt
    start_date=datetime(2024, 11, 17),  # Startdatum des DAGs
    catchup=False,  # Verhindert, dass der DAG nachträglich ausgeführt wird
)

hiveQL_create_table_mtg_cards='''
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_cards(
    id STRING,
    name STRING,
    artist STRING,
    type STRING,
    manaCost STRING,
    rarity STRING
) 
COMMENT 'MTG Card Data'
PARTITIONED BY (partition_year INT, partition_month INT, partition_day INT)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hadoop/mtg_raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}/'
    '''

hiveSQL_add_partition_mtg_cards = '''
ALTER TABLE mtg_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d") }})
LOCATION '/user/hadoop/mtg_raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}/';
'''


create_hdfs_mtg_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_mtg_partition_dir',
    directory='/user/hadoop/mtg_raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}',
    hdfs_conn_id='hdfs',
    dag=dag,
)


download_mtg_data = SparkSubmitOperator(
    task_id='download_mtg_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_getcards.py',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='1g',
    num_executors='1',
    name='spark_download_mtg',
    application_args=[
        '--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}',
        '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}',
        '--day', '{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}',
        '--hdfs_path', '/user/hadoop/mtg_raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}'
    ],
    verbose=True,
    dag = dag
)

create_HiveTable_mtg_cards = HiveOperator(
    task_id='create_mtg_cards_table',
    hql=hiveQL_create_table_mtg_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

addPartition_HiveTable_mtg_cards = HiveOperator(
    task_id='add_partition_mtg_cards_table',
    hql=hiveSQL_add_partition_mtg_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

pyspark_mtg_final_cards = SparkSubmitOperator(
    task_id='pyspark_write_mtg_cards_to_final',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_mtg_cleaning.py',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='1g',
    num_executors='1',
    name='spark_calculate_mtg',
    verbose=True,
     application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
                      '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
                      '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
)

pyspark_mtg_export_cards = SparkSubmitOperator(
    task_id='pyspark_export_mtg_cards',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_export.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_export_mtg',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
                      '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
                      '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
)

create_hdfs_mtg_partition_dir >> download_mtg_data >> create_HiveTable_mtg_cards >> addPartition_HiveTable_mtg_cards >> pyspark_mtg_final_cards >> pyspark_mtg_export_cards 