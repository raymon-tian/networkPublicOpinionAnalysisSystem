#coding=utf-8
"""工具函数文件"""
import jieba
import re
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row

def createSinaNewsDF():
    conf = SparkConf().setAppName('createSinaNewsDF').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    return sqlContext.read.parquet('roll_news_sina_com_cn.parquet')

def showFirstNnews(DataFrame):
    """显示数据源下的前10条新闻，按时间排序"""
    temp = DataFrame.orderBy('time',ascending=1).limit(10).select("*")
    return convertDfToList(temp)
    
def showNewsByCategory(DataFrame):
    """某一种类下的所有新闻，按时间排序"""
    temp = DataFrame.orderBy('time',ascending=1).where(DataFrame['label']==u'科技').select("*")
    return convertDfToList(temp)

def convertDfToList(DataFrame):
    l = DataFrame.take(DataFrame.count())
    myList = []
    for row in l:
        dic = row.asDict()
        myList.append(dic)
    return myList

conf = SparkConf().setAppName('tfidf').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

rawNews = sc.textFile(name='roll_news_sina_com_cn.csv')
parts = rawNews.map(lambda line:line.split(','))
#titleNews 为 unicode
titleNews = parts.map(lambda p:Row(label=p[0],title=p[1],url=p[2],time=p[3]))
dfTitleNews = sqlContext.createDataFrame(titleNews)
dfTitleNews.show
dfTitleNews.printSchema()
dfTitleNews.select(u'label').orderBy('time').show()
dfTitleNews.orderBy('time').show()
dfTitleNews.filter(dfTitleNews['label']==u'体育').show
dfTitleNews.groupBy(u'label').count().show()
dfTitleNews.drop('title').show()
dfTitleNews.freqItems(('label','title')).show()
dfTitleNews.filter(dfTitleNews['label']=='科技').limit(3).show()
dfTitleNews.orderBy('time',ascending=0).show()
dfTitleNews.orderBy('time',ascending=1).show()
print(dfTitleNews.dtypes)
print(showNewsByCategory(dfTitleNews))

sc.stop()