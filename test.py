# #coding=utf-8
# import re
# from string import punctuation,digits,letters,whitespace
# import sys
# import myuntils
#
# """字符串过滤"""
# temp = "想做/ 兼_职/学生_/ 的 、加,我Q：  1 5.  8 0. ！！？？  8 6 。0.  2。 3     有,惊,喜,哦"
# temp2 = temp.decode("utf8")
# string = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), "".decode("utf8"),temp2)
# string = string.encode('utf-8')
# string.translate(None,punctuation)
# string.translate(None,digits)
# string.translate(None,letters)
# string.translate(None,whitespace)
# print(type(temp))
# print(type(string))
# print string
#
# string = '中国'
# s1 = string.decode('utf-8')
# print(type(s1))
# print(sys.getdefaultencoding())
#
# string = '1111sfsdfs所发生的11 ：南京航空航天大学: ,11sdfsdf王栋1111'
# string = myuntils.handleAndCut(string)
# print(type(string))
# string = string.encode('utf-8')
# print(type(string))
#
# l = ['111','222']
# print(l)
from django.shortcuts import render
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.ml.feature import HashingTF,IDF,Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def convertDfToList(DataFrame):
    """将 DataFrame 转换为 List"""
    l = DataFrame.take(DataFrame.count())
    myList = []
    for row in l:
        dic = row.asDict()
        myList.append(dic)
    return myList

"""1.系统首页，显示各个数据源下前几条新闻"""
conf = SparkConf().setAppName('index').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
rawNews = sc.textFile(name='data/roll_news_sina_com_cn.csv')
parts = rawNews.map(lambda line:line.split(','))
titleNews = parts.map(lambda p:Row(label=p[0],title=p[1],url=p[2],time=p[3]))
dfTitleNews = sqlContext.createDataFrame(titleNews)
partions = dfTitleNews.orderBy('time',ascending=0).limit(10).select("*")
sinaNewsList = convertDfToList(partions)
print(sinaNewsList)
sc.stop()