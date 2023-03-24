#!/usr/bin/env python
# coding: utf-8

# In[]:


import sys
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window


## Получим датафрейм городов Австралии с их координатами (координаты указаны в грудасх, переведем их в радианы)


def get_city_loc(url, spark):
    geo_city = spark.read.csv(url, sep=";", header=True)
    geo_city = geo_city.withColumn('lat1', F.col('lat') / F.lit(57.3)).withColumn('lng1',
                                                                                  F.col('lng') / F.lit(57.3)).drop(
        'lat', 'lng')
    return geo_city


## Прочитаем датафрейм с событиями (координаты указаны в грудасх, переведем их в радианы)

def get_events_geo(events_url, spark):
    raw_path_geo_events = events_url

    events_geo = spark.read.parquet(raw_path_geo_events).sample(0.1).withColumn('lat2',
                                                                                F.col('lat') / F.lit(57.3)).withColumn(
        'lng2', F.col('lon') / F.lit(57.3)).withColumn('user_id',
                                                       F.when(F.col('event_type') == 'reaction',
                                                              F.col('event.reaction_from')) \
                                                       .when(F.col('event_type') == 'subscription',
                                                             F.col('event.user')) \
                                                       .otherwise(F.col('event.message_from'))
                                                       ) \
        .withColumn('ts',
                    F.when((F.col('event_type') == 'reaction') | (F.col('event_type') == 'subscription'),
                           F.col('event.datetime')) \
                    .when((F.col('event_type') == 'message') & (F.col('event.message_channel_to').isNotNull()),
                          F.col('event.datetime')) \
                    .otherwise(F.col('event.message_ts'))
                    ) \
        .drop('lat', 'lon') \
        .cache()
    return events_geo


## Объединим два датафрейма в один

def get_events_city(messeges, geo_city):
    window = Window().partitionBy('event.message_id').orderBy('distance')

    events_city = messages.join(geo_city).withColumn('distance', F.lit(2) * F.lit(6371) * F.asin(F.sqrt(F.pow(F.sin(
        (F.col('lat2') - F.col('lat1')) / F.lit(2)), 2) \
                                                                                                        + F.cos('lat1') \
                                                                                                        * F.cos('lat2') \
                                                                                                        * F.pow(
        F.sin((F.col('lng2') - F.col('lng1')) / F.lit(2)), 2)))) \
        .withColumn('rank', F.rank().over(window)) \
        .where(F.col('rank') == 1) \
        .select('user_id',
                'event_type',
                F.col('id').alias('zone_id'),
                'ts') \
        .cache()
    return events_city


## Получим город, в котором было отправлено последнее сообщение

def get_message_city(events_city):
    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
    message_city = events_city.select('user_id',
                                      F.first('zone_id', True).over(window).alias('zone_id')) \
        .distinct()
    return message_city


## Получим другие события, не сообщения, с указанием города

def get_not_message_events_with_cities(not_message_events, message_city):
    not_message_events_with_cities = not_message_events.join(message_city, 'user_id', 'left').select('user_id',
                                                                                                     'event_type',
                                                                                                     'zone_id',
                                                                                                     'ts')
    return not_message_events_with_cities


## Объеденим датафреймы всех событий

def get_all_events(events_city, not_messege_events_with_cities):
    all_events = events_city.union(not_messege_events_with_cities)
    return all_events


## Получим витрину в разрезе зон

def get_mart_2(all_events):
    w = Window
    mart_2 = all_events.withColumn('week', F.date_trunc('week', F.col('ts'))) \
        .withColumn('month', F.date_trunc('month', F.col('ts'))) \
        .withColumn('action_num', F.row_number().over(w().partitionBy('user_id').orderBy('ts'))) \
        .select('month',
                'week',
                'zone_id',
                F.count(F.when(F.col('event_type') == 'message', 1)).over(
                    w().partitionBy('week', 'zone_id')).alias('week_message'),
                F.count(F.when(F.col('event_type') == 'reaction', 1)).over(
                    w().partitionBy('week', 'zone_id')).alias(
                    'week_reaction'),
                F.count(F.when(F.col('event_type') == 'subscription', 1)).over(
                    w().partitionBy('week', 'zone_id')).alias('week_subscription'),
                F.count(F.when((F.col('event_type') == 'message') & (F.col('action_num') == 1), 1)).over(
                    w().partitionBy('week', 'zone_id')).alias('week_user'),
                F.count(F.when(F.col('event_type') == 'message', 1)).over(
                    w().partitionBy('month', 'zone_id')).alias(
                    'month_message'),
                F.count(F.when(F.col('event_type') == 'reaction', 1)).over(
                    w().partitionBy('month', 'zone_id')).alias(
                    'month_reaction'),
                F.count(F.when(F.col('event_type') == 'subscription', 1)).over(
                    w().partitionBy('month', 'zone_id')).alias('month_subscription'),
                F.count(F.when((F.col('event_type') == 'message') & (F.col('action_num') == 1), 1)).over(
                    w().partitionBy('month', 'zone_id')).alias('month_user')) \
        .distinct()
    return mart_2


def main():
    conf = SparkConf().setAppName("mart_2_Anisimovp")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    url = sys.argv[1]
    events_url = sys.argv[2]
    user_url = sys.argv[3]

    geo_city = get_city_loc(url, spark)

    events_geo = get_events_geo(events_url, spark)

    messages = events_geo.where(F.col('event_type') == 'message')
    not_message_events = events_geo.where(F.col('event_type') != 'message')

    events_city = get_events_city(messages, geo_city)

    message_city = get_message_city(events_city)

    not_message_events_with_cities = get_not_message_events_with_cities(not_message_events, message_city)

    all_events = get_all_events(events_city, not_message_events_with_cities)

    mart_2 = get_mart_2(all_events)

    mart_2.write.mode('overwrite').parquet(user_url)


if __name__ == '__main__':
    main()
