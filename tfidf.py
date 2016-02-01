# -*- coding: utf-8 -*-
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row,DataFrame
from pyspark.ml.feature import HashingTF,IDF,Tokenizer
from string import punctuation

conf = SparkConf().setAppName('tfidf').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

rawNews = sc.textFile(name='roll_news_sina_com_cn.csv')
parts = rawNews.map(lambda line:line.split(','))
schemaNews = parts.map(lambda p:Row(category=p[0],title=p[1],url=p[2],time=p[3]))
titleNews = parts.map(lambda p:Row(title=p[1]))
dfNews = sqlContext.createDataFrame(schemaNews)
dfTitleNews = sqlContext.createDataFrame(titleNews)
#dfNews.show()
dfTitleNews.show()
tokenizer = Tokenizer(inputCol='title',outputCol='words')
dfNewsWords = tokenizer.transform(dfNews)
#dfNewsWords.show()
sc.stop()
