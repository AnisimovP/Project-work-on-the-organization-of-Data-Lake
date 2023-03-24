# !/usr/bin/env python
# coding: utf-8

# In[ ]:


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
from pyspark.sql.functions import radians
import pyspark.sql.functions as F
from pyspark.sql.window import Window


## Получим датафрейм городов Австралии с их координатами (координаты указаны в грудасх, переведем их в радианы)

def get_city_loc(url, spark):
    geo_city = spark.read.csv(url, sep=";", header=True)
    geo_city = geo_city.withColumn('lat1', radians(F.col('lat'))).withColumn('lng1',
                                                                             radians(F.col('lng'))).drop(
        'lat', 'lng')
    return geo_city


## Прочитаем датафрейм с событиями (координаты указаны в грудасх, переведем их в радианы)

def get_events_geo(events_url, spark):
    raw_path_geo_events = events_url
    events_geo = spark.read.parquet(raw_path_geo_events).sample(0.10).where(
        F.col('event_type') == 'message').withColumn('lat2', radians(F.col('lat'))).withColumn('lon2', radians(
        F.col('lon'))).withColumn('user_id', F.col('event.message_from')).withColumn('ts', F.when(
        F.col('event.message_channel_to').isNotNull(),
        F.col('event.datetime')) \
                                                                                     .otherwise(
        F.col('event.message_ts'))) \
        .drop('lat', 'lon')
    return events_geo


## Объединим два датафрейма в один

def get_events_city(events_geo, geo_city):
    window = Window().partitionBy('event.message_id').orderBy('distance')
    events_city = events_geo.join(geo_city).withColumn('distance', F.lit(2) * F.lit(6371) * F.asin(F.sqrt(F.pow(
        F.sin((F.col('lat2') - F.col('lat1')) / F.lit(2)), 2) \
                                                                                                          + F.cos(
        'lat1') * F.cos('lat2') * F.pow(F.sin((F.col('lon2') - F.col('lng1')) / F.lit(2)), 2)
                                                                                                          )
                                                                                                   )
                                                       ) \
        .withColumn('rank', F.row_number().over(window)) \
        .where(F.col('rank') == 1) \
        .cache()
    return events_city


## Получим город из которого в последний раз отправляли сообщение

def get_message_city(events_city):
    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
    message_city = events_city.select('user_id',
                                      F.first('city', True).over(window).alias('act_city'),
                                      F.from_utc_timestamp(
                                          F.first('ts', True).over(window).alias('act_city'),
                                          F.concat(F.lit("Australia/"), F.lit('Sydney'))).alias('local_time')) \
        .distinct()
    return message_city


## Получим новый датафрейм:  
## Если события происходят подряд в одном городе, то это один визит. 
## Если происходит смена города, то это новый визит.


def get_visits(message_city):
    window = Window().partitionBy('user_id').orderBy('date')
    city_change = message_city.select('user_id',
                                      F.to_date('ts').alias('date'),
                                      'city') \
        .distinct() \
        .withColumn('prev_city', F.lag('city').over(window)) \
        .withColumn('num_visit', F.when(
        (F.col('city') != F.col('prev_city')) | (F.col('prev_city').isNull()),
        F.monotonically_increasing_id()
    )
                    ) \
        .withColumn('num_visit_full', F.max('num_visit').over(window)) \
        .cache()
    return city_change


## Получим домашний адрес, если пользователь был на этом адресе больше 27 дней

def get_home_adress(city_change):
    window = Window().partitionBy('user_id').orderBy(F.desc('num_visit_full'))
    home_adress = city_change.groupBy('user_id', 'city', 'num_visit_full').count().where(F.col('count') > 27).select(
        'user_id',
        F.first('city').over(window).alias('home_city')
    ).distinct()

    return home_adress


## Получим количество посещенных городов.

def get_city_count(city_change):
    city_count = city_change.groupBy('user_id').agg(F.countDistinct('num_visit_full').alias('travel_count'))
    return city_count


## Получим список городов в порядке посещения

def get_city_array(city_change):
    window_spec = Window.partitionBy('user_id').orderBy('num_visit_full')
    city_array = city_change.select('user_id', 'city', 'num_visit_full') \
        .distinct() \
        .groupBy('user_id') \
        .agg(F.collect_list('city').alias('travel_array')) \
        .withColumn('row_num', F.row_number().over(window_spec)) \
        .filter(F.col('row_num') == F.max('row_num').over(window_spec)) \
        .select('user_id', 'travel_array')
    return city_array


## Соберем все метрики в одну

def get_mart_1(message_city, home_adress, city_count, city_array):
    mart_1 = message_city.join(home_adress, 'user_id', 'full').join(city_count, 'user_id', 'full').join(city_array,
                                                                                                        'user_id',
                                                                                                        'full')
    return mart_1


def main():
    conf = SparkConf().setAppName("mart_1_Anisimovp")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    url = sys.argv[1]
    events_url = sys.argv[2]
    user_url = sys.argv[3]

    geo_city = get_city_loc(url, spark)

    events_geo = get_events_geo(events_url, spark)

    events_city = get_events_city(events_geo, geo_city)

    message_city = get_message_city(events_city)

    city_change = get_visits(events_city)

    home_adress = get_home_adress(city_change)

    city_count = get_city_count(city_change)

    city_array = get_city_array(city_change)

    mart_1 = get_mart_1(message_city, home_adress, city_count, city_array)

    mart_1.write.mode('overwrite').parquet(user_url)


if __name__ == '__main__':
    main()
