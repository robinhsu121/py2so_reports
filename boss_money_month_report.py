#! /usr/bin/env pyspark
# -*- coding: utf-8 -*-
# endcoding:utf-8

# update:2019-06-11

"""boss_money_report，并写入mysql"""
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
spark.conf.set("spark.debug.maxToStringFields", 300)
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.rdd.compress", "true")
spark.conf.set("spark.sql.codegen","true")
# spark.conf.set("spark.storage.memoryFraction",0.6) #spark.executor.memory内存资源分为两部分，一部分用于缓存，缓存比例是0.6;另一部分用于任务计算，计算资源为spark.executor.memory*0.4
#spark.conf.set("")  ###


# 获得今天的日期,前1日
#today = datetime.date(2019,05,02) ##test
__today = datetime.date.today()
__day_before_0 = __today - datetime.timedelta(days=1)  # 昨天
# 日期转化为字符串
__str_dt_0 = datetime.datetime.strftime(__day_before_0, '%Y-%m-%d')


#从boss数据库拉取数据:第一阶段
def __boss_to_hdfs():
    """ 将数据mysql导入hdfs"""
    table_name = """ (select fsk_package_id,fsk_order_provide,fsk_order_price,fsk_order_type,fsk_order_delevetime  from vip.fsk_vip_order where date_format(from_unixtime(fsk_order_delevetime/1000),'%Y-%m-%d')="{date}" and fsk_order_status=4 and fsk_order_state=1 and fsk_order_type in (1,2,3)) as fsk_vip_order """ .format(date=__str_dt_0)
    url = "jdbc:mysql://196.168.100.47:3306/vip?user=biadmin&password=bi_12345"
    jdbc_df = spark.read.format('jdbc').options(
              url=url,
              driver='com.mysql.jdbc.Driver',
              dbtable=table_name).load()
    jdbc_df.createOrReplaceTempView("v_jdbc_df")
    sql = """ select from_unixtime(fsk_order_delevetime/1000,"yyyyMMdd") date,(case when lower(fsk_order_provide)="iqiyi" then "iqiyi" when lower(fsk_order_provide)="youku" then "youku" when  lower(fsk_order_provide) in ("bestv","bestv:pptv") then "bestv"  else "others" end) as channelName,
 (case when fsk_order_type=1 then 1 else 2 end) as vipType,
(case when lower(fsk_package_id) in ("xqla4e","tpsp2k","s2brac","8dyu7v","tpsp2k","2cet3y")  then "年卡"  when  lower(fsk_package_id) in ("e8t3wp","3dr88z","tpl3rd","5jef9n")  then "半年卡"
when lower(fsk_package_id) in ("42kehp","ge3c2p","8pywh2","r299ex")  then "季卡"  when   lower(fsk_package_id) in ("flk7ea","bptv01","zxx6c6","b8zenj")  then "月卡"
when lower(fsk_package_id) in ("y48685","cmjhkb","d3pzre") then "连续包月"  when fsk_order_type=3 then "单点"  end) as packageName,count(*) sellCount,round(sum(fsk_order_price)/100,2) moneyCount
from v_jdbc_df
group by from_unixtime(fsk_order_delevetime/1000,"yyyyMMdd"),(case when lower(fsk_order_provide)="iqiyi" then "iqiyi" when lower(fsk_order_provide)="youku" then "youku" when  lower(fsk_order_provide) in ("bestv","bestv:pptv") then "bestv"  else "others" end),
(case when fsk_order_type=1 then 1 else 2 end),
(case when lower(fsk_package_id) in ("xqla4e","tpsp2k","s2brac","8dyu7v","tpsp2k","2cet3y")  then "年卡" when  lower(fsk_package_id) in ("e8t3wp","3dr88z","tpl3rd","5jef9n")  then "半年卡"
when lower(fsk_package_id) in ("42kehp","ge3c2p","8pywh2","r299ex")  then "季卡"  when   lower(fsk_package_id) in ("flk7ea","bptv01","zxx6c6","b8zenj")  then "月卡"
when lower(fsk_package_id) in ("y48685","cmjhkb","d3pzre") then "连续包月"  when fsk_order_type=3 then "单点"  end) """
    spark.sql("show databases")
    spark.sql("use sharp")
    tmp_df = spark.sql(sql)
    return tmp_df



##从boss数据库拉取数据:第二阶段
def __mysql_to_hdfs():
    """ 将数据mysql导入hdfs"""
    table_name = """ (select date,channelName,vipType,packageName,sellCount,moneyCount from sharpbi.vipOrder_day_report) as vip_order """
    url = "jdbc:mysql://196.168.100.88:3306/sharpbi?user=biadmin&password=bi_12345"
    jdbc_df = spark.read.format('jdbc').options(
              url=url,
              driver='com.mysql.jdbc.Driver',
              dbtable=table_name).load()
    jdbc_df.createOrReplaceTempView("v_jdbc_df")
    sql = """select t.* from ( select substr(date,1,6) month,channelName,vipType,packageName,grouping_id() id_1,sum(sellCount) sellCount,round(sum(moneyCount),2) moneyCount
from v_jdbc_df
group by substr(date,1,6),channelName,vipType,packageName  with cube ) t where t.month is not null"""
    spark.sql("show databases")
    spark.sql("use sharp")
    dest_df = spark.sql(sql)
    return dest_df



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
    """ boss_money_month_report"""
    #报表数据：第一阶段
    tmp_df = __boss_to_hdfs()
    ##报表数据1写入MySQL
    __hdfs_to_mysql(src_df=tmp_df,table_name="vipOrder_day_report",mode_type="append")
    del tmp_df
    #报表数据：第二阶段
    dest_df = __mysql_to_hdfs()
    ##报表数据1写入MySQL
    __hdfs_to_mysql(src_df=dest_df,table_name="vipOrder_month_report",mode_type="overwrite")
    del dest_df

