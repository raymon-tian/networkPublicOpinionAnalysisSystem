#coding=utf-8
from django.shortcuts import render
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.ml.feature import HashingTF,IDF,Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Create your views here.

def convertDfToList(DataFrame):
    """将 DataFrame 转换为 List"""
    l = DataFrame.take(DataFrame.count())
    myList = []
    for row in l:
        dic = row.asDict()
        myList.append(dic)
    return myList

def index(request):
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
    return render(request,'system/index.html',{'sinaNewsList':sinaNewsList})

def search(request):
    """2.按查询条件返回结果"""
    label = request.POST['label']
    kw = request.POST['kw']
    time = request.POST['time']

    conf = SparkConf().setAppName('search').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    rawNews = sc.textFile(name='data/roll_news_sina_com_cn.csv')
    parts = rawNews.map(lambda line:line.split(','))
    titleNews = parts.map(lambda p:Row(label=p[0],title=p[1],url=p[2],time=p[3]))
    dfTitleNews = sqlContext.createDataFrame(titleNews)
    dfResult = dfTitleNews.where(dfTitleNews['label']==label).where(dfTitleNews['time']==time)\
    .where(dfTitleNews['title']==kw).select("*")
    resultList = convertDfToList(dfResult)

    sc.stop()

    return render(request,'system/search.html',{'resultList':resultList})

def showAsLabel(request):
    """3.按标签显示"""
    label = request.GET['myLabel']

    conf = SparkConf().setAppName('showAsLabel').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    rawNews = sc.textFile(name='data/roll_news_sina_com_cn.csv')
    parts = rawNews.map(lambda line:line.split(','))
    titleNews = parts.map(lambda p:Row(label=p[0],title=p[1],url=p[2],time=p[3]))
    dfTitleNews = sqlContext.createDataFrame(titleNews)
    dfResult = dfTitleNews.where(dfTitleNews['label'] == label).select("*")
    resultList = convertDfToList(dfResult)
    print(resultList)
    sc.stop()

    return render(request,'system/showAsLabel.html',{'resultList':resultList})

def showWebsitesCrawled(request):
    """4.显示抓取的所有网站"""

    conf = SparkConf().setAppName('showWebsitesCrawled').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    rawNews = sc.textFile(name='data/websites_crawled.csv')
    parts = rawNews.map(lambda line:line.split(','))
    temp = parts.map(lambda p:Row(title=p[0],url=p[1]))
    dfWebsites = sqlContext.createDataFrame(temp)
    resultList = convertDfToList(dfWebsites)

    sc.stop()

    return render(request,{'resultList':resultList})

def dataUpdate(request):
    """5.开启爬虫，实时抓取数据，更新数据源"""
    conf = SparkConf().setAppName('textPredict').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sc.stop()


def textPredict(request):
    """6.文本聚类，热度预测"""
    label = request.POST['label']
    title = request.POST['title']

    conf = SparkConf().setAppName('textPredict').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    """处理数据集，生成特征向量"""
    dfTitles = sqlContext.read.parquet('data/roll_news_sina_com_cn.parquet')
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
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(rescaledData)
    featureIndexer =\
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(rescaledData)
    (trainingData, testData) = rescaledData.randomSplit([0.7, 0.3])
    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])
    model = pipeline.fit(trainingData)
    """模型测试"""
    predictions = model.transform(testData)
    predictions.show()
    predictions.select("prediction", "indexedLabel", "features").show(5)
    """用户数据测试，单个新闻测试"""
    sentenceData = sqlContext.createDataFrame([
        (label,title),
    ],['label',"title"])
    tokenizer = Tokenizer(inputCol="title", outputCol="words")
    wordsData = tokenizer.transform(sentenceData)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData)
    rescaledData = idfModel.transform(featurizedData)
    myprediction = model.transform(rescaledData)
    print("==================================================")
    myprediction.show()
    resultList = convertDfToList(myprediction)

    """模型评估"""
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g " % (1.0 - accuracy))

    treeModel = model.stages[2]
    print(treeModel)

    sc.stop()
    return render(request,{'resultList':resultList})

def getSensitiveNews(request):
    """7.查询敏感信息"""
    conf = SparkConf().setAppName('textPredict').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sc.stop()

def predictTextHotDegree(request):
    """8.文本热度预测"""
    conf = SparkConf().setAppName('textPredict').setMaster('spark://HP-Pavilion:7077')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sc.stop()





