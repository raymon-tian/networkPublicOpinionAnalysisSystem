# -*- coding: utf-8 -*-
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row,DataFrame
from pyspark.ml.feature import HashingTF,IDF,Tokenizer

conf = SparkConf().setAppName('tfidf').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

dfTitles = sqlContext.read.parquet('roll_news_sina_com_cn.parquet')
print(dfTitles.dtypes)
tokenizer = Tokenizer(inputCol="title", outputCol="words")
wordsData = tokenizer.transform(dfTitles)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
rescaledData.show()
for features_label in rescaledData.select("features", "rawFeatures").take(3):
    print(features_label)

sc.stop()
