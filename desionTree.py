# -*- coding: utf-8 -*-
"""将原始数据tfidf特征向量化后，使用desiontree"""

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row,DataFrame
from pyspark.ml.feature import HashingTF,IDF,Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


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

# Make predictions.
predictions = model.transform(testData)
predictions.show()
predictions.select("label","indexedLabel").show(100)
# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)
sc.stop()
