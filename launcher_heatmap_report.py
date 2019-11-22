#! /usr/bin/env pyspark
# -*- coding: utf-8 -*-
# endcoding:utf-8
#edited:robin
#update:2019-06-24
"""launcher_heatmap_report:launcher页面热力图"""
######################################################################################################################

##从日志里解析数据，并存入hive数据库

##load libraries
from __future__ import absolute_import,division,print_function
import pyspark
from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
#import numpy as np
#import pandas as pd
import os
import datetime
import getopt
import argparse
import commands
import subprocess
import operator

##解决因为编码，导致写入数据报错(ERROR - failed to write data to stream: <open file '<stdout>', mode 'w' at)
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


######################################################################################################################

""" spark环境具体配置参数  """
spark = SparkSession.builder.master("spark://master:7077").appName("launcher_heatmap_report").enableHiveSupport().getOrCreate()
spark.conf.set("spark.master", "spark://master:7077")
# 发生shuffle时的并行度，默认是核数，太大导致GC，太小执行速度慢
# spark.conf.set("spark.sql.shuffle.partitions",240) ##发生聚合操作的并行度，默认是200，太小容易导致OOM,executor丢失，任务执行时间过长,太大会导致保存的小文件过多，默认是200个小文件
# spark.conf.set("spark.sql.result.partitions",20)  ####最后的执行计划中加入一个repartition transformation。通过参数控制最终的partitions数且不影响shuffle partition的数量,减少小文件的个数
spark.conf.set("spark.sql.execution.arrow.enabled","true")  # spark df & pandas df性能优化，需开启
# spark.conf.set("spark.driver.maxResultSize","3g")  #一般是spark默认会限定内存，可以使用以下的方式提高
spark.conf.set("spark.yarn.executor.memoryOverhead", 2048)
spark.conf.set("spark.core.connection.ack.wait.timeout", 300)
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.debug.maxToStringFields", 500)
spark.conf.set("spark.rdd.compress", "true")
spark.conf.set("spark.sql.codegen","true")
# spark.conf.set("spark.storage.memoryFraction",0.6) #spark.executor.memory内存资源分为两部分，一部分用于缓存，缓存比例是0.6;另一部分用于任务计算，计算资源为spark.executor.memory*0.4
#spark.conf.set("")  ###


##设定日期
__today = datetime.date.today()
__day_before_0 = __today - datetime.timedelta(days=1)  # 昨天
__day_before_1 = __day_before_0 - datetime.timedelta(days=1)  # 昨天前1天
__day_before_7 = __day_before_0 - datetime.timedelta(days=7)  # 昨天前7天
#日期转化为字符串
__str_dt_0 = datetime.datetime.strftime(__day_before_0, '%Y-%m-%d')
__str_dt_1 = datetime.datetime.strftime(__day_before_1, '%Y-%m-%d')
__str_dt_7 = datetime.datetime.strftime(__day_before_7, '%Y-%m-%d')



##解析home_item_click日志函数
def __parse_home_item_click():
    """ 解析home_item_click日志内容 """
    ##测验json日志文件的路径
    json_datails_test="hdfs://master:9000/data/{0}/*/*.gz".format(__str_dt_1)
    ##判断路径文件条件
    cmd = "hadoop fs -ls -R /data/{} | egrep '.gz$' | wc -l" .format(__str_dt_1)
    if_zero = subprocess.check_output(cmd,shell=True).strip().split('\n')[0]
    ##判断日志文件路径是否存在
    if int(if_zero) == 0:
        print(" The logs does not exists!")
        raise SystemExit(123)
    else:
        json_path_0="hdfs://master:9000/data/{0}/*/*.gz".format(__str_dt_0)
        json_path_1="hdfs://master:9000/data/{0}/*/*.gz".format(__str_dt_1)
        json_path_7="hdfs://master:9000/data/{0}/*/*.gz".format(__str_dt_7)
        ##解析home_item_click   
        df_0=spark.read.json(json_path_0).select('custom_uuid','packageName','site',F.explode('data.home_item_click').alias('home_item_click')).select('custom_uuid','site',F.col('home_item_click.time').alias('time'),F.col('home_item_click.category').alias('category'),F.col('home_item_click.position').alias('position'),F.col('home_item_click.title').alias('title'),F.col('home_item_click.type').alias('type'))
        df_1=spark.read.json(json_path_1).select('custom_uuid','packageName','site',F.explode('data.home_item_click').alias('home_item_click')).select('custom_uuid','site',F.col('home_item_click.time').alias('time'),F.col('home_item_click.category').alias('category'),F.col('home_item_click.position').alias('position'),F.col('home_item_click.title').alias('title'),F.col('home_item_click.type').alias('type'))
        df_7=spark.read.json(json_path_7).select('custom_uuid','packageName','site',F.explode('data.home_item_click').alias('home_item_click')).select('custom_uuid','site',F.col('home_item_click.time').alias('time'),F.col('home_item_click.category').alias('category'),F.col('home_item_click.position').alias('position'),F.col('home_item_click.title').alias('title'),F.col('home_item_click.type').alias('type'))
        ##最终解析数据
        if ("category" in df_0.columns) and  ("category" in df_1.columns) and ("category" in df_7.columns):
            df_stat_0=df_0.filter(F.col("time").isNotNull()).select(['custom_uuid',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.col('category').alias('category'),F.col('position').alias('position'),F.col('title').alias('title'),F.col('type').alias('type')])
            df_stat_1=df_1.filter(F.col("time").isNotNull()).select(['custom_uuid',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.col('category').alias('category'),F.col('position').alias('position'),F.col('title').alias('title'),F.col('type').alias('type')])
            df_stat_7=df_7.filter(F.col("time").isNotNull()).select(['custom_uuid',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.col('category').alias('category'),F.col('position').alias('position'),F.col('title').alias('title'),F.col('type').alias('type')])
        else:
            df_stat_0=df_0.filter(F.col("time").isNotNull()).select(['custom_uuid',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.lit('null').cast('string').alias('category'),F.col('position').alias('position'),F.col('title').alias('title'),F.col('type').alias('type')])
            df_stat_1=df_1.filter(F.col("time").isNotNull()).select(['custom_uuid',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.lit('null').cast('string').alias('category'),F.col('position').alias('position'),F.col('title').alias('title'),F.col('type').alias('type')])
            df_stat_7=df_7.filter(F.col("time").isNotNull()).select(['custom_uuid',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.lit('null').cast('string').alias('category'),F.col('position').alias('position'),F.col('title').alias('title'),F.col('type').alias('type')])
    return df_stat_0,df_stat_1,df_stat_7



##取得每个分组的 title top 3
def __heatmap_title_top3_(df_stat_0):
    """ launcher热力图,返回每个分组title的top 3 """
    #global df_stat_0,df_stat_1,df_stat_2
    df_stat_0.createOrReplaceTempView("v_df_0")
    sql = """ select t.site,t.category,t.position,concat_ws('|',collect_set(t.title)) title_set from (
select t1.site,t1.category,t1.position,t1.title,row_number() over(partition by t1.site,t1.category,t1.position  order by playNum desc) rn from(
select site,category,position,title,count(custom_uuid) playNum from v_df_0 where  position like "%,%" group by site,category,position,title having count(custom_uuid) >= 20) t1
) t where t.rn <=3 group by t.site,t.category,t.position """
    spark.sql("show databases")
    spark.sql("use sharp")
    df_title_set = spark.sql(sql)
    return df_title_set

# define udf 
def sorter(l): 
    res = sorted(l, key=operator.itemgetter(0),reverse = True) 
    return [item[1] for item in res] 

sort_udf = F.udf(sorter) 

'''
def __heatmap_title_top3(df_stat):
    """ launcher热力图,返回每个分组title的top 3 """
    df_title_set = df_stat.filter(F.col("position").like("%,%")).groupBy(F.col("site"),F.col("category"),F.col("position"),F.col("title")).agg(F.count("custom_uuid").alias("playNum")).filter(F.col("playNum") >= 20) \
        .select(F.col("site").alias("site"),F.col("category").alias("category"),F.col("position").alias("position"),F.col("title").alias("title"),F.col("playNum").alias("playNum"),F.row_number().over(Window.partitionBy(F.col("site"),F.col("category"),F.col("position")).orderBy(F.col("playNum").desc())).alias("rn")).filter(F.col("rn") <= 3) \
            .groupBy(F.col("site"),F.col("category"),F.col("position")).agg(F.concat_ws("|",F.collect_set("title")).alias("title_set"))
    return df_title_set
'''
##concat_ws('|',sort_array(collect_list(title))) titleSet

def __heatmap_title_top3(df_stat):
    """ launcher热力图,返回每个分组title的top 3 """
    df_title_set = df_stat.filter(F.col("position").like("%,%")).filter(F.col("title").isNotNull()).groupBy(F.col("site"),F.col("category"),F.col("position"),F.col("title")).agg(F.count("custom_uuid").alias("playNum")).filter(F.col("playNum") >= 20) \
        .select(F.col("site").alias("site"),F.col("category").alias("category"),F.col("position").alias("position"),F.col("title").alias("title"),F.col("playNum").alias("playNum"),F.row_number().over(Window.partitionBy(F.col("site"),F.col("category"),F.col("position")).orderBy(F.col("playNum").desc())).alias("rn")).filter(F.col("rn") <= 3) \
            .groupBy(F.col("site"),F.col("category"),F.col("position")).agg(F.collect_list(F.struct("playNum", "title")).alias("num_title")).select("site","category","position",F.regexp_replace(F.regexp_replace(sort_udf("num_title"),",","|"),"\[|\]","").alias("title_set"))
    return df_title_set





##launcher热力图报表
def __launcher_heatmap(df_title_set,df_stat_0,df_stat_1,df_stat_7):
    """ launcher热力图 """
    #global df_stat_0,df_stat_1,df_stat_2
    df_title_set.createOrReplaceTempView("v_df_title_set")
    df_stat_0.createOrReplaceTempView("v_df_0")
    df_stat_1.createOrReplaceTempView("v_df_1")
    df_stat_7.createOrReplaceTempView("v_df_7")
    sql_0 = """select site,category,position,grouping_id() id_1,count(custom_uuid) playNum,count(distinct custom_uuid) users from v_df_0 where  position like "%,%"  group by site,category,position  grouping sets((site,category,position),(site,category)) having count(custom_uuid) >= 20"""
    sql_1 = """select site,category,position,grouping_id() id_1,count(custom_uuid) playNum,count(distinct custom_uuid) users from v_df_1 where  position like "%,%"  group by site,category,position  grouping sets((site,category,position),(site,category)) having count(custom_uuid) >= 20"""
    sql_7 = """select site,category,position,grouping_id() id_1,count(custom_uuid) playNum,count(distinct custom_uuid) users from v_df_7 where  position like "%,%"  group by site,category,position  grouping sets((site,category,position),(site,category)) having count(custom_uuid) >= 20"""
    spark.sql("show databases")
    spark.sql("use sharp")
    df_cube_0 = spark.sql(sql_0)
    spark.sql("show databases")
    spark.sql("use sharp")
    df_cube_1 = spark.sql(sql_1)
    spark.sql("show databases")
    spark.sql("use sharp")
    df_cube_7 = spark.sql(sql_7)
    ##title_top_3、天环比、周同比连接条件
    #title_top_3
    condition0=(F.coalesce(F.col("t_0.site"),F.lit("123")) == F.coalesce(F.col("t_.site"),F.lit("123")))
    condition1=(F.coalesce(F.col("t_0.category"),F.lit("123")) == F.coalesce(F.col("t_.category"),F.lit("123")))
    condition2=(F.coalesce(F.col("t_0.position"),F.lit("123")) == F.coalesce(F.col("t_.position"),F.lit("123")))
    #天环比
    condition_0=(F.coalesce(F.col("t_0.site"),F.lit("123")) == F.coalesce(F.col("t_1.site"),F.lit("123")))
    condition_1=(F.coalesce(F.col("t_0.category"),F.lit("123")) == F.coalesce(F.col("t_1.category"),F.lit("123")))
    condition_2=(F.coalesce(F.col("t_0.position"),F.lit("123")) == F.coalesce(F.col("t_1.position"),F.lit("123")))
    condition_3=(F.col("t_0.id_1") == F.col("t_1.id_1"))
    #周同比
    condition_4=(F.coalesce(F.col("t_0.site"),F.lit("123")) == F.coalesce(F.col("t_7.site"),F.lit("123")))
    condition_5=(F.coalesce(F.col("t_0.category"),F.lit("123")) == F.coalesce(F.col("t_7.category"),F.lit("123")))
    condition_6=(F.coalesce(F.col("t_0.position"),F.lit("123")) == F.coalesce(F.col("t_7.position"),F.lit("123")))
    condition_7=(F.col("t_0.id_1") == F.col("t_7.id_1"))
    ##title_top_3连接条件
    conditions_title_set = condition0 & condition1 & condition2
    ##天环比连接条件
    conditions_0_1 = condition_0 & condition_1 & condition_2 & condition_3 
    ##周同比连接条件
    conditions_0_7 = condition_4 & condition_5 & condition_6 & condition_7
    ##最终报表
    heatmap_report = df_cube_0.alias("t_0").join(df_title_set.alias("t_"),conditions_title_set,"left_outer").join(df_cube_1.alias("t_1"),conditions_0_1,"left_outer") \
                                                 .join(df_cube_7.alias("t_7"),conditions_0_7,"left_outer") \
                                                 .select(F.regexp_replace(F.lit(__str_dt_0),"-","").cast("int").alias("date"),F.col("t_0.site").alias("channelName"),F.col("t_0.category").alias("entryName"),F.col("t_0.position").alias("position"),F.col("t_.title_set").alias("titleSet"),F.col("t_0.id_1").alias("id_1"), \
                                                 F.col("t_0.playNum").alias("totalPlayCount"),F.concat(F.round((F.col("t_0.playNum")/F.col("t_1.playNum")-1)*100,2),F.lit("%")).alias("playCountCompareDay"),F.concat(F.round((F.col("t_0.playNum")/F.col("t_7.playNum")-1)*100,2),F.lit("%")).alias("playCountCompareWeek"), \
                                                 F.col("t_0.users").alias("totalUserCount"),F.concat(F.round((F.col("t_0.users")/F.col("t_1.users")-1)*100,2),F.lit("%")).alias("userCountCompareDay"),F.concat(F.round((F.col("t_0.users")/F.col("t_7.users")-1)*100,2),F.lit("%")).alias("userCountCompareWeek"))
    return heatmap_report



##数据写入mysql
def __hdfs_to_mysql(src_df,table_name,mode_type='append'):
    """ 将数据框src_df从HDFS写入MySQL表table_name """
    src_df.write.format('jdbc').options(
          url='jdbc:mysql://196.168.100.88:3306/sharpbi',
          driver='com.mysql.jdbc.Driver',
          dbtable=table_name,
          user='biadmin',
          password='bi_12345').mode(mode_type).save()


##综合函数
def main():
    """ launcher_heatmap_report报表入口 """
    #解析日志home_item_click
    df_stat_0,df_stat_1,df_stat_7 = __parse_home_item_click()
    #launcher热力图报表并写入mysql
    df_title_set = __heatmap_title_top3(df_stat=df_stat_0)
    heatmap_report = __launcher_heatmap(df_title_set=df_title_set,df_stat_0=df_stat_0,df_stat_1=df_stat_1,df_stat_7=df_stat_7)
    __hdfs_to_mysql(heatmap_report,"launcher_heatmap_report","append")
    del heatmap_report

