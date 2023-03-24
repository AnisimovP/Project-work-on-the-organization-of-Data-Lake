import os
import datetime as dt

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'Анисимов Павел',
}
with DAG(
        'events',
        default_args=default_args,
        description='Получение данных о событиях.',
        start_date=dt.datetime(2022, 12, 22),
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
) as dag:
    mart_1 = SparkSubmitOperator(
        task_id='mart_1',
        application='/lessons/mart_1.py',
        conn_id='yarn_spark',
        application_args=[
            '/user/anisimovp/data/geo/geo_2.csv',
            '/user/master/data/geo/events',
            '/user/anisimovp/data/analytics',
        ]
    )

    mart_2 = SparkSubmitOperator(
        task_id='mart_2',
        application='/lessons/mart_2.py',
        conn_id='yarn_spark',
        application_args=[
            '/user/anisimovp/data/geo/geo_2.csv',
            '/user/master/data/geo/events',
            '/user/anisimovp/data/analytics',
        ]
    )

    mart_3 = SparkSubmitOperator(
        task_id='mart_3',
        application='/lessons/mart_3.py',
        conn_id='yarn_spark',
        application_args=[
            '/user/anisimovp/data/geo/geo_2.csv',
            '/user/master/data/geo/events',
            '/user/anisimovp/data/analytics',
        ]
    )

    mart_1 >> mart_2 >> mart_3