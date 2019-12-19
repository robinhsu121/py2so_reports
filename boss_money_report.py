#! /usr/bin/env pyspark
# -*- coding: utf-8 -*-
# endcoding:utf-8

# update:2019-03-15

"""订单报表及月度订单报表：boss_money_report & boss_money_month_report"""
######################################################################################################################
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
#import commands
#import subprocess

# 解决因为编码，导致写入数据报错(ERROR - failed to write data to stream: <open file '<stdout>', mode 'w' at)
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

######################################################################################################################
""" spark环境具体配置参数  """
spark = SparkSession.builder.master("spark://master:7077").appName("boss_money_report").enableHiveSupport().getOrCreate()
spark.conf.set("spark.master", "spark://master:7077")
# 发生shuffle时的并行度，默认是核数，太大导致GC，太小执行速度慢
#spark.conf.set("spark.default.parallelism", 190)
# spark.conf.set("spark.sql.shuffle.partitions",240) ##发生聚合操作的并行度，默认是200，太小容易导致OOM,executor丢失，任务执行时间过长,太大会导致保存的小文件过多，默认是200个小文件
# spark.conf.set("spark.sql.result.partitions",20)  ####最后的执行计划中加入一个repartition transformation。通过参数控制最终的partitions数且不影响shuffle partition的数量,减少小文件的个数
#spark.conf.set("spark.executor.memory", "3g")
#spark.conf.set("spark.executor.cores", 3)
#spark.conf.set("spark.cores.max", 72)
#spark.conf.set("spark.driver.memory", "3g")
#spark.conf.set("spark.sql.execution.arrow.enabled","true")  # spark df & pandas df性能优化，需开启
# spark.conf.set("spark.driver.maxResultSize","3g")  #一般是spark默认会限定内存，可以使用以下的方式提高
spark.conf.set("spark.yarn.executor.memoryOverhead", 2048)
spark.conf.set("spark.core.connection.ack.wait.timeout", 300)
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.debug.maxToStringFields", 300)
spark.conf.set("spark.rdd.compress", "true")
spark.conf.set("spark.sql.codegen","true")
# spark.conf.set("spark.storage.memoryFraction",0.6) #spark.executor.memory内存资源分为两部分，一部分用于缓存，缓存比例是0.6;另一部分用于任务计算，计算资源为spark.executor.memory*0.4
#spark.conf.set("")  ###


#########################################################################################################################################################

# 获得今天的日期,前1日及前7日
#today = datetime.date(2019,01,02) ##test
__today = datetime.date.today()
__day_before_0 = __today - datetime.timedelta(days=1)  # 昨天
__day_before_1 = __day_before_0 - datetime.timedelta(days=1)  # 昨天前1天
__day_before_7 = __day_before_0 - datetime.timedelta(days=7)  # 昨天前7天
__day_before_30 = __day_before_0 - datetime.timedelta(days=30)  # 昨天前30天
# 日期转化为字符串
__str_dt_0 = datetime.datetime.strftime(__day_before_0, '%Y-%m-%d')
__str_dt_1 = datetime.datetime.strftime(__day_before_1, '%Y-%m-%d')
__str_dt_7 = datetime.datetime.strftime(__day_before_7, '%Y-%m-%d')
__str_dt_30 = datetime.datetime.strftime(__day_before_30, '%Y-%m-%d')


##从boss数据库拉数据
def __mysql_to_hdfs(date):
    """ 将数据mysql导入hdfs"""
    table_name = """ (select fsk_package_id,fsk_order_provide,fsk_order_price,fsk_order_type from vip.fsk_vip_order where date_format(from_unixtime(fsk_order_delevetime/1000),'%Y-%m-%d')="{date}" and fsk_order_status=4 and fsk_order_state=1 and fsk_order_type in (2,3)) as fsk_vip_order """ .format(date=date)
    url = "jdbc:mysql://196.168.100.47:3306/vip?user=biadmin&password=bi_12345"
    jdbc_df = spark.read.format('jdbc').options(
              url=url,
              driver='com.mysql.jdbc.Driver',
              dbtable=table_name).load()
    jdbc_df.createOrReplaceTempView("v_jdbc_df")
    sql = """ select (case when lower(fsk_order_provide)="iqiyi" then "iqiyi" when lower(fsk_order_provide)="youku" then "youku" when  lower(fsk_order_provide) in ("bestv","bestv:pptv") then "bestv"  else "others" end) as channelName,
(case when lower(fsk_package_id) in ("xqla4e","tpsp2k","s2brac","8dyu7v")  then "年卡" when  lower(fsk_package_id) in ("e8t3wp","3dr88z","tpl3rd","5jef9n")  then "半年卡"
when lower(fsk_package_id) in ("42kehp","ge3c2p","8pywh2","r299ex")  then "季卡"  when   lower(fsk_package_id) in ("flk7ea","bptv01","zxx6c6","b8zenj")  then "月卡"
when lower(fsk_package_id) in ("y48685","cmjhkb","d3pzre") then "连续包月"  when fsk_order_type=3 then "单点"  end) as packageName,grouping_id() id_1,count(*) sellCount,round(sum(fsk_order_price)/100,2) moneyCount
from v_jdbc_df
group by (case when lower(fsk_order_provide)="iqiyi" then "iqiyi" when lower(fsk_order_provide)="youku" then "youku" when  lower(fsk_order_provide) in ("bestv","bestv:pptv") then "bestv"  else "others" end),
(case when lower(fsk_package_id) in ("xqla4e","tpsp2k","s2brac","8dyu7v")  then "年卡" when  lower(fsk_package_id) in ("e8t3wp","3dr88z","tpl3rd","5jef9n")  then "半年卡"
when lower(fsk_package_id) in ("42kehp","ge3c2p","8pywh2","r299ex")  then "季卡"  when   lower(fsk_package_id) in ("flk7ea","bptv01","zxx6c6","b8zenj")  then "月卡"
when lower(fsk_package_id) in ("y48685","cmjhkb","d3pzre") then "连续包月"  when fsk_order_type=3 then "单点"  end) with cube """
    spark.sql("show databases")
    spark.sql("use sharp")
    dest_df = spark.sql(sql)
    return dest_df


##boss金额报表函数
def __boss_report(df_0,df_1,df_7):
    """ boss金额报表 """
    ##天环比、周同比连接条件
    condition_0=(F.coalesce(F.col("t_0.channelName"),F.lit("123")) == F.coalesce(F.col("t_1.channelName"),F.lit("123")))
    condition_1=(F.coalesce(F.col("t_0.packageName"),F.lit("123")) == F.coalesce(F.col("t_1.packageName"),F.lit("123"))) 
    condition_2=(F.col("t_0.id_1") == F.col("t_1.id_1"))
    condition_3=(F.coalesce(F.col("t_0.channelName"),F.lit("123")) == F.coalesce(F.col("t_7.channelName"),F.lit("123")))
    condition_4=(F.coalesce(F.col("t_0.packageName"),F.lit("123")) == F.coalesce(F.col("t_7.packageName"),F.lit("123"))) 
    condition_5=(F.col("t_0.id_1") == F.col("t_7.id_1"))
    ##天环比连接条件
    conditions_0_1 = condition_0 & condition_1 & condition_2
    ##周同比连接条件
    conditions_0_7 = condition_3 & condition_4 & condition_5 
    ##最终报表
    report = df_0.alias("t_0").join(df_1.alias("t_1"),conditions_0_1,"left_outer") \
                              .join(df_7.alias("t_7"),conditions_0_7,"left_outer") \
                              .select(F.regexp_replace(F.lit(__str_dt_0),"-","").cast("int").alias("date"),F.col("t_0.channelName").alias("channelName"),F.col("t_0.packageName").alias("packageName"),F.col("t_0.id_1").alias("id_1"), \
                              F.col("t_0.sellCount").alias("sellCount"),F.concat(F.round((F.col("t_0.sellCount")/F.col("t_1.sellCount")-1)*100,2),F.lit("%")).alias("sellCountCompareDay"),F.concat(F.round((F.col("t_0.sellCount")/F.col("t_7.sellCount")-1)*100,2),F.lit("%")).alias("sellCountCompareWeek"), \
                              F.col("t_0.moneyCount").alias("moneyCount"),F.concat(F.round((F.col("t_0.moneyCount")/F.col("t_1.moneyCount")-1)*100,2),F.lit("%")).alias("moneyCountCompareDay"),F.concat(F.round((F.col("t_0.moneyCount")/F.col("t_7.moneyCount")-1)*100,2),F.lit("%")).alias("moneyCountCompareWeek"))
    return report



##数据写入MySQL函数
def __hdfs_to_mysql(src_df,table_name,mode_type="append"):
    """ 将数据src_df从HDFS写入MySQL表table_name """
    src_df.write.format('jdbc').options(
          url='jdbc:mysql://196.168.100.88:3306/sharpbi',
          driver='com.mysql.jdbc.Driver',
          dbtable=table_name,
          user='biadmin',
          password='bi_12345').mode(mode_type).save()


##boss_money_report报表
def main():
    """ boss_money_report"""
    import gc
    from time import sleep
    #昨天的数据
    df_0 = __mysql_to_hdfs(date=__str_dt_0)
    #相比昨天前1天的的数据
    df_1 = __mysql_to_hdfs(date=__str_dt_1)
    #相比昨天前7天的的数据
    df_7 = __mysql_to_hdfs(date=__str_dt_7)
    ##报表数据
    boss_money_report = __boss_report(df_0=df_0,df_1=df_1,df_7=df_7)
    ##报表数据写入MySQL
    __hdfs_to_mysql(src_df=boss_money_report,table_name="boss_money_report",mode_type="append")
    del boss_money_report

