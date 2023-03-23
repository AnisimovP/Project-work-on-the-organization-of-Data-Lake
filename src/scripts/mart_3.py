#!/usr/bin/env python
# coding: utf-8

# In[1]:


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



## Прочитаем датафрейм с геолокациями городов Австралии и их координатами (координаты указаны в грудасх, переведем их в радианы)

def get_city_loc(url, spark):
    geo_city = spark.read.csv(url, sep=";", header=True)
    geo_city = geo_city.withColumn('lat1', F.col('lat')/F.lit(57.3))                        .withColumn('lng1', F.col('lng')/F.lit(57.3))                         .drop('lat', 'lng')
    return geo_city


## Прочитаем датафрейм с событиями (координаты указаны в грудасх, переведем их в радианы)

def get_events_geo(events_url, spark):
    raw_path_geo_events = events_url
    events_geo = spark.read.parquet(raw_path_geo_events)                 .where(
                    ((F.col('event_type') == 'message') & (F.col('event.message_to').isNotNull())) | (F.col('event_type') == 'subscription')
                ) \
                .sample(0.01) \
                .withColumn('lat2', F.col('lat')/F.lit(57.3)) \
                .withColumn('lon2', F.col('lon')/F.lit(57.3)) \
                .withColumn('user_id',
                    F.when(F.col('event_type') == 'subscription',
                        F.col('event.user')) \
                    .otherwise(F.col('event.message_from'))
                ) \
                .withColumn('ts',
                    F.when(F.col('event_type') == 'subscription',
                        F.col('event.datetime')) \
                    .otherwise(F.col('event.message_ts'))
                ) \
                .drop('lat', 'lon', 'date') \
                .cache()
    return events_geo

## Объединим два датафрейма в один

def get_events_city(messages, geo_city):
    window = Window().partitionBy('event.message_id').orderBy('distance')
    events_city = messages.join(geo_city)                     .withColumn('distance',F.lit(2)*F.lit(6371)*F.asin(F.sqrt(F.pow(
                                        F.sin((F.col('lat2') - F.col('lat1'))/F.lit(2)), 2) \
                                    + F.cos('lat1') * F.cos('lat2') * F.pow(F.sin((F.col('lon2')-F.col('lng1'))/F.lit(2)), 2)
                                )
                        )
                    ) \
                    .withColumn('rank', F.rank().over(window)) \
                    .where(F.col('rank') == 1) \
                    .cache()
    return events_city

## Определим пользователей, подписанных на один канал.

def get_subscription_together (subscriptions):
    
    users_in_chanel = subscriptions                         .select('event.subscription_channel', 'user_id')

    user_left = users_in_chanel.withColumnRenamed('user_id', 'user_left')

    user_right = users_in_chanel.withColumnRenamed('user_id', 'user_right')

    users_pair = user_left                    .join(
                        user_right,
                        [user_left.subscription_channel == user_right.subscription_channel,
                        user_left.user_left != user_right.user_right],
                        'inner'
                    )\
                    .select('user_left', 'user_right')\
                    .distinct()
    return users_pair

## Определим пользователей, который не общались друг с другом 

def get_non_message (users_pair, events_city):
    
    contacts = events_city                .select('event.message_from', 'event.message_to')                .distinct()

    non_message = users_pair                    .join(
                        contacts,
                        [((users_pair.user_left == contacts.message_from)\
                            & (users_pair.user_right == contacts.message_to)) |\
                        ((users_pair.user_right == contacts.message_from)\
                            & (users_pair.user_left == contacts.message_to))],
                        'leftanti'
                    )
    return non_message

## Определим пользователей, расстояние между которыми не превышает 1 км

def get_coordinates_users (events_city):

    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
    user_coordinates = events_city                         .select(
                            'user_id',
                            F.first('lat2', True).over(window).alias('act_lat'),
                            F.first('lon2', True).over(window).alias('act_lng'),
                            F.first('id', True).over(window).alias('zone_id'),
                            F.first('ts', True).over(window).alias('act_ts'),
                        ) \
                        .distinct()
    return user_coordinates

def get_mart_3 (non_message, user_coordinates):
    
    mart_3 = non_message             .join(user_coordinates, users_pair.user_left == user_coordinates.user_id, 'left')             .withColumnRenamed('user_id', 'lu')             .withColumnRenamed('act_lat', 'lat1')             .withColumnRenamed('act_lng', 'lng1')             .withColumnRenamed('zone_id', 'zone_id1')             .withColumnRenamed('act_ts', 'act_ts1')             .join(user_coordinates, users_pair.user_right == user_coordinates.user_id, 'left')             .withColumnRenamed('user_id', 'ru')             .withColumnRenamed('act_lat', 'lat2')             .withColumnRenamed('act_lng', 'lon2')             .withColumnRenamed('zone_id', 'zone_id2')             .withColumnRenamed('act_ts', 'act_ts2')             .withColumn(
                'distance',
                        F.lit(2)*F.lit(6371)*F.asin(F.sqrt(F.pow(F.sin(
                                            (F.col('lat2') - F.col('lat1'))/F.lit(2)), 2)\
                                    + F.cos('lat1')\
                                    * F.cos('lat2')\
                                    * F.pow(F.sin((F.col('lon2')-F.col('lng1'))/F.lit(2)), 2)))) \
            .where(F.col('distance') <= 30) \
            .select(
                'user_left',
                'user_right',
                F.current_timestamp().alias('processed_dttm'),
                F.when(F.col('zone_id1') == F.col('zone_id2'), F.col('zone_id1')).alias('zone_id'),
                F.from_utc_timestamp(F.current_timestamp(), 'Australia/Sydney').alias('local_time'))
    return mart_3

def main():
    
    conf = SparkConf().setAppName("mart_3_Anisimovp")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)
    
    url = sys.argv[1]
    events_url = sys.argv[2]
    user_url = sys.argv[3]
    
    geo_city = get_city_loc(url, spark)
    
    events_geo = get_events_geo(events_url, spark)
    
    messages = events_geo.where(F.col('event_type') == 'message')
    subscriptions = events_geo.where(F.col('event_type') == 'subscription')
    
    events_city = get_events_city(messages, geo_city)
    
    users_pair = get_subscription_together (subscriptions)
    
    non_message = get_non_message (users_pair, events_city)
    
    user_coordinates = get_coordinates_users (events_city)
    
    mart_3 = get_mart_3 (non_message, user_coordinates)
    
    mart_3.write         .mode('overwrite')         .parquet(user_url)

if __name__ == '__main__':
    main()