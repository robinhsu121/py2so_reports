#! /usr/bin/env pyspark
# -*- coding: utf-8 -*-
# endcoding:utf-8

# update:2019-10-24

"""视频播放量部分报表/开机时长报表/页面曝光报表/launcher热力图报表/屏保报表/付费影片报表/影片搜索报表/专题点击报表/注册登录用户报表/影片吸金报表"""
######################################################################################################################
# load libraries

from __future__ import absolute_import, division, print_function
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
#import numpy as np
#import pandas as pd
import os
import datetime
#import getopt
import argparse
import commands
import subprocess


# 解决因为编码，导致写入数据报错(ERROR - failed to write data to stream: <open file '<stdout>', mode 'w' at)
import sys
sys.path.insert(0,'/home/hadoop/build')
import video_play_count_report
import power_on_time_report
import launcher_exposure_report
import launcher_heatmap_report
import screensaver_click_report
import buy_entrance_report
import vod_search_report
import subject_show_count_report
import record_login_register_report
import movies_profit_report
reload(sys)
sys.setdefaultencoding('utf-8')


######################################################################################################################

""" spark环境具体配置参数  """

spark = SparkSession.builder.master("spark://master:7077").appName("movie_profit_report").enableHiveSupport().getOrCreate()

spark.conf.set("spark.master", "spark://master:7077")
# 发生shuffle时的并行度，默认是核数，太大导致GC，太小执行速度慢
spark.conf.set("spark.default.parallelism", 190)
# spark.conf.set("spark.sql.shuffle.partitions",240) ##发生聚合操作的并行度，默认是200，太小容易导致OOM,executor丢失，任务执行时间过长,太大会导致保存的小文件过多，默认是200个小文件
# spark.conf.set("spark.sql.result.partitions",20)  ####最后的执行计划中加入一个repartition transformation。通过参数控制最终的partitions数且不影响shuffle partition的数量,减少小文件的个数
spark.conf.set("spark.executor.memory", "3g")
spark.conf.set("spark.executor.cores", 3)
spark.conf.set("spark.cores.max", 72)
spark.conf.set("spark.driver.memory", "3g")
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

if  __name__ == "__main__":
    """  报表入口 """
    import time
    #video_play_count_report.main()
    time.sleep(11)
    #power_on_time_report.main()
    time.sleep(11)
    #launcher_exposure_report.main()
    time.sleep(11)
    launcher_heatmap_report.main()
    time.sleep(11)
    screensaver_click_report.main()
    time.sleep(11)
    #buy_entrance_report.main()
    time.sleep(11)
    #vod_search_report.main()
    time.sleep(11)
    #subject_show_count_report.main()
    time.sleep(11)
    #record_login_register_report.main()
    time.sleep(11)
    #movies_profit_report.main()
    #stop!!
    spark.stop()
 

 

