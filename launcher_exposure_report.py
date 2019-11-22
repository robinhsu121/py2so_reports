#! /usr/bin/env pyspark
# -*- coding: utf-8 -*-
# endcoding:utf-8

# edited:2019-04-24
# update:2019-04-24

''' launcher页曝光报表:launcher_exposure_report '''
######################################################################################################################

# 从日志里解析数据，并存入hive数据库

# load libraries
from __future__ import absolute_import, division, print_function
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
#import numpy as np
#import pandas as pd
import os
import datetime
import getopt
import argparse
import commands
import subprocess

# 解决因为编码，导致写入数据报错(ERROR - failed to write data to stream: <open file '<stdout>', mode 'w' at)
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

######################################################################################################################

# def spark_configs():
""" spark环境具体配置参数  """
spark = SparkSession.builder.master("spark://master:7077").appName("launcher_page_stay_report").enableHiveSupport().getOrCreate()
spark.conf.set("spark.master", "spark://master:7077")
# spark.conf.set("spark.sql.shuffle.partitions",240) ##发生聚合操作的并行度，默认是200，太小容易导致OOM,executor丢失，任务执行时间过长,太大会导致保存的小文件过多，默认是200个小文件
# spark.conf.set("spark.sql.result.partitions",20)  ####最后的执行计划中加入一个repartition transformation。通过参数控制最终的partitions数且不影响shuffle partition的数量,减少小文件的个数
spark.conf.set("spark.sql.execution.arrow.enabled","true")  # spark df & pandas df性能优化，需开启
# spark.conf.set("spark.driver.maxResultSize","3g")  #一般是spark默认会限定内存，可以使用以下的方式提高
spark.conf.set("spark.yarn.executor.memoryOverhead", 2048)
spark.conf.set("spark.core.connection.ack.wait.timeout", 300)
spark.conf.set("spark.debug.maxToStringFields", 500)
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.rdd.compress", "true")
spark.conf.set("spark.sql.codegen", "true")
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed","true")  # 设置了会降低运算速度
# spark.conf.set("spark.storage.memoryFraction",0.6) #spark.executor.memory内存资源分为两部分，一部分用于缓存，缓存比例是0.6;另一部分用于任务计算，计算资源为spark.executor.memory*0.4
#spark.conf.set("")  ###
# return spark




##设定日期
__today = datetime.date.today()
__day_before_0 = __today - datetime.timedelta(days=1)  # 昨天
__day_before_1 = __day_before_0 - datetime.timedelta(days=1)  # 昨天前1天
__day_before_7 = __day_before_0 - datetime.timedelta(days=7)  # 昨天前7天
#日期转化为字符串
__str_dt_0 = datetime.datetime.strftime(__day_before_0, '%Y-%m-%d')
__str_dt_1 = datetime.datetime.strftime(__day_before_1, '%Y-%m-%d')
__str_dt_7 = datetime.datetime.strftime(__day_before_7, '%Y-%m-%d')



# 解析launcher_page_stay日志函数
def __parse_launcher_page_stay():
    """解析launcher_page_stay日志数据"""
    # json日志文件的路径
    json_path = "hdfs://master:9000/data/{0}/*/*.gz".format(__str_dt_0)
    # 判断路径文件条件
    cmd = "hadoop fs -ls -R /data/{} | egrep '.gz$' | wc -l" .format(__str_dt_0)
    if_zero = subprocess.check_output(cmd, shell=True).strip().split('\n')[0]
    # 判断日志文件路径是否存在
    if int(if_zero) == 0:
        print("the launcher_page_stay_logs does not exists!")
        raise SystemExit(123)
    else:
        # json日志数据路径,并解析
        df = spark.read.json(json_path).select('custom_uuid', 'account', 'rectime', 'vercode',
                                               'vername', 'device_name', 'site', F.explode('data.launcher_page_stay').alias('launcher_page_stay'))
        # 日志解析后的数据
        df_stat = df.filter(F.col("launcher_page_stay.time").isNotNull()).select(['custom_uuid', 'account', F.to_date(F.from_unixtime(F.col('rectime')/1000)).cast('date').alias("date"), F.to_date(F.from_unixtime(F.col('rectime')/1000)).cast('string').alias('dt'), F.col('vercode').alias('vercode'), F.col('vername').alias('vername'), F.col('device_name').alias('device_name'), F.when(F.col('site') == 'ALI', 'youku').when(F.col('site') == 'IQIYI', 'iqiyi').when(F.col('site') == 'BESTV', 'bestv').otherwise('others').alias('site'), F.from_unixtime(F.col("launcher_page_stay.enter")).alias("enter"), F.from_unixtime(F.col("launcher_page_stay.exit")).alias('exit'),F.col("launcher_page_stay.title").alias("title")]).select(["custom_uuid", "account", "date", "site", "vercode", "vername", "device_name", "enter", "exit","title","dt"])
      # 把数据插入hive动态分区表中
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("show databases")
        spark.sql("use sharp")
        df_stat.write.format("orc").mode("append").partitionBy("dt").saveAsTable("sharp.launcher_page_stay")




# launcher页曝光函数(离开时间-开始时间<=5min or 30min,最终30min)
def __launcher_exposure():
    """ launcher页曝光 """
    sql_0 = """ select site,title,grouping_id() id_1,count(custom_uuid) playNum,round(sum(unix_timestamp(exit)-unix_timestamp(enter))/3600,2) playTime,count(distinct custom_uuid) users,round(count(custom_uuid)/count(distinct custom_uuid),2) avgPlayNum,round((sum(unix_timestamp(exit)-unix_timestamp(enter))/count(distinct custom_uuid))/60,2) avgPlayTime
from sharp.launcher_page_stay where dt="{date_0}" and  exit >= enter and unix_timestamp(exit)-unix_timestamp(enter) <= 1800 group by site,title with cube """ .format(date_0=__str_dt_0)
    sql_1 = """ select site,title,grouping_id() id_1,count(custom_uuid) playNum,round(sum(unix_timestamp(exit)-unix_timestamp(enter))/3600,2) playTime,count(distinct custom_uuid) users,round(count(custom_uuid)/count(distinct custom_uuid),2) avgPlayNum,round((sum(unix_timestamp(exit)-unix_timestamp(enter))/count(distinct custom_uuid))/60,2) avgPlayTime
from sharp.launcher_page_stay where dt="{date_1}" and exit >= enter and unix_timestamp(exit)-unix_timestamp(enter) <= 1800 group by site,title  with cube """ .format(date_1=__str_dt_1)
    sql_7 = """ select site,title,grouping_id() id_1,count(custom_uuid) playNum,round(sum(unix_timestamp(exit)-unix_timestamp(enter))/3600,2) playTime,count(distinct custom_uuid) users,round(count(custom_uuid)/count(distinct custom_uuid),2) avgPlayNum,round((sum(unix_timestamp(exit)-unix_timestamp(enter))/count(distinct custom_uuid))/60,2) avgPlayTime
from sharp.launcher_page_stay where dt="{date_7}" and exit >= enter and unix_timestamp(exit)-unix_timestamp(enter) <= 1800 group by site,title  with cube """ .format(date_7=__str_dt_7)
    spark.sql("show databases")
    spark.sql("use sharp")
    df_cube_0 = spark.sql(sql_0)
    df_cube_1 = spark.sql(sql_1)
    df_cube_7 = spark.sql(sql_7)
    # 连接条件
    left_conditions_0_1 = (F.coalesce(F.col("t_0.site"), F.lit("123")) == F.coalesce(F.col("t_1.site"), F.lit("123"))) & (F.coalesce(F.col("t_0.title"),F.lit("123")) == F.coalesce(F.col("t_1.title"),F.lit("123")))  & (F.col("t_0.id_1") == F.col("t_1.id_1"))
    left_conditions_0_7 = (F.coalesce(F.col("t_0.site"), F.lit("123")) == F.coalesce(F.col("t_7.site"), F.lit("123"))) & (F.coalesce(F.col("t_0.title"),F.lit("123")) == F.coalesce(F.col("t_7.title"),F.lit("123")))  & (F.col("t_0.id_1") == F.col("t_7.id_1"))
    # 最终报表
    report = df_cube_0.alias("t_0").join(df_cube_1.alias("t_1"), left_conditions_0_1, "left_outer").join(df_cube_7.alias("t_7"), left_conditions_0_7, "left_outer").select(F.regexp_replace(F.lit(__str_dt_0), "-", "").cast("int").alias("date"), F.col("t_0.site").alias("channelName"),F.col("t_0.title").alias("typeName"),F.col("t_0.id_1").alias("id_1"),F.col("t_0.playNum").alias("totalPlayNum"),F.concat(F.round((F.col("t_0.playNum")/F.col("t_1.playNum")-1)*100, 2), F.lit("%")).alias("playNumCompareDay"), F.concat(F.round((F.col("t_0.playNum")/F.col("t_7.playNum")-1)*100, 2), F.lit("%")).alias("playNumCompareWeek"), F.col("t_0.playTime").alias("totalPlayTime"), F.concat(F.round((F.col("t_0.playTime")/F.col("t_1.playTime")-1)*100, 2), F.lit("%")).alias("playTimeCompareDay"), F.concat(F.round((F.col("t_0.playTime")/F.col("t_7.playTime")-1)*100, 2), F.lit("%")).alias("playTimeCompareWeek"),F.col("t_0.users").alias("totalUserCount"), F.concat(F.round((F.col("t_0.users")/F.col("t_1.users")-1)*100, 2), F.lit("%")).alias("userCountCompareDay"), F.concat(F.round((F.col("t_0.users")/F.col("t_7.users")-1)*100, 2), F.lit("%")).alias("userCountCompareWeek"), F.col("t_0.avgPlayNum").alias("averagePlayNum"), F.concat(F.round((F.col("t_0.avgPlayNum")/F.col("t_1.avgPlayNum")-1)*100, 2), F.lit("%")).alias("avgPlayNumCompareDay"), F.concat(F.round((F.col("t_0.avgPlayNum")/F.col("t_7.avgPlayNum")-1)*100, 2), F.lit("%")).alias("avgPlayNumCompareWeek"), F.col("t_0.avgPlayTime").alias("averagePlayTime"), F.concat(F.round((F.col("t_0.avgPlayTime")/F.col("t_1.avgPlayTime")-1)*100, 2), F.lit("%")).alias("avgPlayTimeCompareDay"), F.concat(F.round((F.col("t_0.avgPlayTime")/F.col("t_7.avgPlayTime")-1)*100, 2), F.lit("%")).alias("avgPlayTimeCompareWeek"))
    return report




##launcher页曝光写入mysql
def __hdfs_to_mysql(src_df, table_name, mode_type='append'):
    """ 将数据框src_df从HDFS写入MySQL表table_name """
    src_df.write.format('jdbc').options(
        url='jdbc:mysql://196.168.100.88:3306/sharpbi',
        driver='com.mysql.jdbc.Driver',
        dbtable=table_name,
        user='biadmin',
        password='bi_12345').mode(mode_type).save()



def main():
    """ launcher_page_stay报表入口 """
    import gc
    from time import sleep

    # 解析launcher_page_stay日志
    __parse_launcher_page_stay()
    
    # launcher页曝光并写入mysql：
    report = __launcher_exposure()
    __hdfs_to_mysql(report, "launcher_exposure_report", "append")
    del report
    

