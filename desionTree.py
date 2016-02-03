# -*- coding: utf-8 -*-
"""将原始数据tfidf特征向量化后，使用desiontree"""

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row,DataFrame
from pyspark.ml.feature import HashingTF,IDF,Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def convertDfToList(DataFrame):
    l = DataFrame.take(DataFrame.count())
    myList = []
    for row in l:
        dic = row.asDict()
        myList.append(dic)
    return myList

def showNewsByCategory(DataFrame):
    """某一种类下的所有新闻，按时间排序"""
    temp = DataFrame.orderBy('time',ascending=1).where(DataFrame['label']==u'科技').select("*")
    return convertDfToList(temp)

def predictLabel(label,title,model):
    """预测新闻的标签"""
    sentenceData = sqlContext.createDataFrame([
        (label,title),
    ],['label',"title"])
    tokenizer = Tokenizer(inputCol="title", outputCol="words")
    wordsData = tokenizer.transform(sentenceData)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData)
    rescaledData = idfModel.transform(featurizedData)
    myprediction = model.transform(rescaledData)
    return myprediction


"""连接master"""
conf = SparkConf().setAppName('tfidf').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
"""处理数据集，生成特征向量"""
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
"""决策树模型培训"""
# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(rescaledData)
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(rescaledData)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = rescaledData.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)
print("---------------------------")
print(type(model))
"""模型测试"""
# Make predictions.
predictions = model.transform(testData)
predictions.show()
predictions.select("label","indexedLabel").show(100)
# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)
"""自主测试，单个新闻测试"""
sentenceData = sqlContext.createDataFrame([
    (u'科技',u"电脑 手机 集群 机器 数据 科技 云计算 大数据"),
    (u'体育',u"足球 篮球 冠军 成功 比赛 冠军 比分"),
],['label',"title"])
tokenizer = Tokenizer(inputCol="title", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
rescaledData = idfModel.transform(featurizedData)
myprediction = model.transform(rescaledData)
print("==================================================")
myprediction.show()

myprediction = predictLabel(u'体育',u'足球 篮球 冠军 成功 比赛 冠军 比分',model)
print(type(myprediction))
print(convertDfToList(myprediction))

"""模型评估"""
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)

sc.stop()


    
