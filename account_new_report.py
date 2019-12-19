#! /usr/bin/env pyspark
# -*- coding: utf-8 -*-
# endcoding:utf-8

# update:2019-05-20

"""新增会员account_new_report"""
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

spark = SparkSession.builder.master("spark://master:7077").appName("account_new_report").enableHiveSupport().getOrCreate()

# 发生shuffle时的并行度，默认是核数，太大导致GC，太小执行速度慢
#spark.conf.set("spark.default.parallelism", 190)
# spark.conf.set("spark.sql.shuffle.partitions",240) ##发生聚合操作的并行度，默认是200，太小容易导致OOM,executor丢失，任务执行时间过长,太大会导致保存的小文件过多，默认是200个小文件
# spark.conf.set("spark.sql.result.partitions",20)  ####最后的执行计划中加入一个repartition transformation。通过参数控制最终的partitions数且不影响shuffle partition的数量,减少小文件的个数
#spark.conf.set("spark.executor.memory", "3g")
#spark.conf.set("spark.executor.cores", 3)
#spark.conf.set("spark.cores.max", 72)
#spark.conf.set("spark.driver.memory", "3g")
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



#########################################################################################################################################################

##设定日期
__today = datetime.date.today()
__day_before_0 = __today - datetime.timedelta(days=1)  # 昨天
__day_before_1 = __day_before_0 - datetime.timedelta(days=1)  # 昨天前1天
__day_before_7 = __day_before_0 - datetime.timedelta(days=7)  # 昨天前7天
#日期转化为字符串
__str_dt_0 = datetime.datetime.strftime(__day_before_0, '%Y-%m-%d')
__str_dt_1 = datetime.datetime.strftime(__day_before_1, '%Y-%m-%d')
__str_dt_7 = datetime.datetime.strftime(__day_before_7, '%Y-%m-%d')



##当日从boss数据库拉取前一天的数据
def boss_to_hdfs():
    """ 将boss导入hdfs"""
    table_name = """ (select distinct t3.fsk_account_name,t2.fsk_order_type,t2.fsk_package_id,t1.fsk_package_provide,from_unixtime(t1.fsk_right_starttime/1000,'%Y-%m-%d') fsk_right_starttime,from_unixtime(t1.fsk_right_limitdate/1000,'%Y-%m-%d') fsk_right_limittime,from_unixtime(t2.fsk_order_addtime/1000,'%Y-%m-%d') fsk_order_addtime
from  fsk_vip_right t1
inner join fsk_vip_order t2
on (t1.fsk_order_id = t2.fsk_order_id  and from_unixtime(t2.fsk_order_addtime/1000,'%Y-%m-%d')<="{date}" and  t2.fsk_order_type in (1,2) and t2.fsk_order_status=4)
inner join fsk_user_account t3
on (t2.fsk_account_id = t3.fsk_account_id and from_unixtime(t3.fsk_account_addtime/1000,'%Y-%m-%d')<="{date}")) as account_info """.format(date=__str_dt_0)
    #boss数据库账号信息
    url = "jdbc:mysql://196.168.100.47:3306/vip?user=biadmin&password=bi_12345"
    jdbc_df = spark.read.format('jdbc').options(
              url=url,
              driver='com.mysql.jdbc.Driver',
              dbtable=table_name).load()
    jdbc_df_1= jdbc_df.select(["fsk_account_name","fsk_order_type",F.when(F.lower(F.col("fsk_package_provide")) == "iqiyi","iqiyi").when(F.lower(F.col("fsk_package_provide")) == "youku","youku").when(F.lower(F.col("fsk_package_provide")).isin("bestv","bestv:pptv"),"bestv").otherwise("others").alias("fsk_package_provide"),F.when(F.lower(F.col("fsk_package_id")).isin("xqla4e","tpsp2k","s2brac","8dyu7v"),"年卡").when(F.lower(F.col("fsk_package_id")).isin("e8t3wp","3dr88z","tpl3rd","5jef9n"),"半年卡").when(F.lower(F.col("fsk_package_id")).isin("42kehp","ge3c2p","8pywh2","r299ex"),"季卡").when(F.lower(F.col("fsk_package_id")).isin("flk7ea","bptv01","zxx6c6","b8zenj"),"月卡").when(F.lower(F.col("fsk_package_id")).isin("y48685","cmjhkb","d3pzre"),"连续包月").alias("fsk_package_id"),"fsk_right_starttime","fsk_right_limittime","fsk_order_addtime"])
    return jdbc_df_1
'''


##当日从boss数据库拉取前一天的数据
def boss_to_hdfs():
    """ 将boss导入hdfs"""
    table_name =  """(select distinct t3.fsk_account_name,t2.fsk_order_type,t2.fsk_package_id,t1.fsk_package_provide,from_unixtime(t1.fsk_right_starttime/1000,'%Y-%m-%d') fsk_right_starttime,from_unixtime(t1.fsk_right_limitdate/1000,'%Y-%m-%d') fsk_right_limittime,from_unixtime(t2.fsk_order_addtime/1000,'%Y-%m-%d %H') fsk_order_addtime
from (select a.fsk_order_id,a.fsk_package_provide,a.fsk_right_starttime,a.fsk_right_limitdate from fsk_vip_right a)  t1
inner join (select b.fsk_account_id,b.fsk_order_id,b.fsk_order_type,b.fsk_package_id,b.fsk_order_addtime from fsk_vip_order b where from_unixtime(b.fsk_order_addtime/1000,'%Y-%m-%d')<="{date}" and  b.fsk_order_type in (1,2) and b.fsk_order_status=4) t2
on t1.fsk_order_id = t2.fsk_order_id 
inner join (select c.fsk_account_id,c.fsk_account_name from fsk_user_account c where from_unixtime(c.fsk_account_addtime/1000,'%Y-%m-%d')<="{date}") t3
on t2.fsk_account_id = t3.fsk_account_id ) as account_info """.format(date=__str_dt_0)
    #boss数据库账号信息
    url = "jdbc:mysql://196.168.100.47:3306/vip?user=biadmin&password=bi_12345"
    jdbc_df = spark.read.format('jdbc').options(
              url=url,
              driver='com.mysql.jdbc.Driver',
              dbtable=table_name).load()
    jdbc_df_1= jdbc_df.select(["fsk_account_name","fsk_order_type",F.when(F.lower(F.col("fsk_package_provide")) == "iqiyi","iqiyi").when(F.lower(F.col("fsk_package_provide")) == "youku","youku").when(F.lower(F.col("fsk_package_provide")).isin("bestv","bestv:pptv"),"bestv").otherwise("others").alias("fsk_package_provide"),F.when(F.lower(F.col("fsk_package_id")).isin("xqla4e","tpsp2k","s2brac","8dyu7v"),"年卡").when(F.lower(F.col("fsk_package_id")).isin("e8t3wp","3dr88z","tpl3rd","5jef9n"),"半年卡").when(F.lower(F.col("fsk_package_id")).isin("42kehp","ge3c2p","8pywh2","r299ex"),"季卡").when(F.lower(F.col("fsk_package_id")).isin("flk7ea","bptv01","zxx6c6","b8zenj"),"月卡").when(F.lower(F.col("fsk_package_id")).isin("y48685","cmjhkb"),"连续包月").alias("fsk_package_id"),"fsk_right_starttime","fsk_right_limittime","fsk_order_addtime"])
    return jdbc_df_1

'''




##取得会员最早订单权益函数,可缓存数据
def account_regist(jdbc_df_1,date):
    """取得会员最早订单的一条数据"""
    jdbc_df_1.createOrReplaceTempView("v_jdbc")
    sql = """ select t.fsk_account_name,t.fsk_order_type,t.fsk_package_provide,t.fsk_package_id,t.fsk_right_starttime,t.fsk_right_limittime,t.registtime from 
(select t1.fsk_account_name,t1.fsk_order_type,t1.fsk_package_provide,t1.fsk_package_id,t1.fsk_right_starttime,t1.fsk_right_limittime,t1.fsk_order_addtime,min(t1.fsk_order_addtime) over(partition by fsk_account_name,fsk_order_type,fsk_package_provide) registtime
from v_jdbc t1 where t1.fsk_order_addtime <= "{date}") t 
where t.registtime = "{date}" """.format(date=date)
    spark.sql("show databases")
    spark.sql("use sharp")
    jdbc_df_2 = spark.sql(sql)
    return jdbc_df_2





##新增会员信息函数
def account_new(jdbc_df_2):
    """新增会员会员包信息函数"""
    jdbc_df_2.createOrReplaceTempView("v_df")
    #购买会员 & 赠送会员
    sql = """ select fsk_account_name,fsk_package_provide channelName,(case when fsk_order_type=1 then "赠送" else fsk_package_id  end) as packageName from v_df """
    spark.sql("show databases")
    spark.sql("use sharp")
    df_ = spark.sql(sql)
    #汇总各类型会员
    df = df_.cube(F.col("channelName"),F.col("packageName")).agg(F.grouping_id().alias("id_1"),F.countDistinct("fsk_account_name").alias("totalUserCount"))
    return df





##新增会员权益包信息报表函数:天环比、周同比
def account_new_report(report_0,report_1,report_7):
    """新增会员权益包信息报表 """
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
    report = report_0.alias("t_0").join(report_1.alias("t_1"),conditions_0_1,"left_outer") \
                              .join(report_7.alias("t_7"),conditions_0_7,"left_outer") \
                              .select(F.regexp_replace(F.lit(__str_dt_0),"-","").cast("int").alias("date"),F.col("t_0.channelName").alias("channelName"),F.col("t_0.packageName").alias("packageName"),F.col("t_0.id_1").alias("id_1"), \
                              F.col("t_0.totalUserCount").alias("totalUserCount"),F.concat(F.round((F.col("t_0.totalUserCount")/F.col("t_1.totalUserCount")-1)*100,2),F.lit("%")).alias("userCountCompareDay"),F.concat(F.round((F.col("t_0.totalUserCount")/F.col("t_7.totalUserCount")-1)*100,2),F.lit("%")).alias("userCountCompareWeek"))
                              
    return report





##数据写入MySQL函数
def hdfs_to_mysql(src_df,table_name,mode_type="append"):
    """ 将数据src_df从HDFS写入MySQL表table_name """
    src_df.write.format('jdbc').options(
          url='jdbc:mysql://196.168.100.88:3306/sharpbi',
          driver='com.mysql.jdbc.Driver',
          dbtable=table_name,
          user='biadmin',
          password='bi_12345').mode(mode_type).save()






##account_info_report报表
def main():
    """ account_info_report"""
    import gc
    from time import sleep

    #boss拉取数据
    jdbc_df_1 = boss_to_hdfs()
    ##缓存
    jdbc_df_1.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    jdbc_df_2_0 = account_regist(jdbc_df_1=jdbc_df_1,date=__str_dt_0)
    jdbc_df_2_1 = account_regist(jdbc_df_1=jdbc_df_1,date=__str_dt_1)
    jdbc_df_2_7 = account_regist(jdbc_df_1=jdbc_df_1,date=__str_dt_7)

    ##新增会员数据
    report_0 = account_new(jdbc_df_2=jdbc_df_2_0)
    report_1 = account_new(jdbc_df_2=jdbc_df_2_1)
    report_7 = account_new(jdbc_df_2=jdbc_df_2_7)
    ##新增会员报表数据
    report =  account_new_report(report_0=report_0,report_1=report_1,report_7=report_7)
    ##报表数据写入MySQL
    hdfs_to_mysql(src_df=report,table_name="account_new_report",mode_type="append")
    
    #取消缓存
    jdbc_df_1.unpersist()

    

