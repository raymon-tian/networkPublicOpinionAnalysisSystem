#coding=utf-8
from django.shortcuts import render
from django.http import HttpResponse
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row
import csv
import StringIO
from networkPublicOpinionAnalysisSystem import settings

# Create your views here.

def index(request):
    string  = u'template显示字符串变量'
    list = ['第一','第二','第三']
    tuple = ('q','w','e','r','t')
    dict = {'a':1,'b':2,'c':3,'d':4}
    conf = SparkConf().setAppName("djangotest").setMaster("spark://HP-Pavilion:7077")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    url='jdbc:mysql://127.0.0.1:3306?user=root&password=raymon'
    dbtable='networkPublicOpinionAnalysisSystem.test'
    df = sqlContext.read.format('jdbc').options(url=url,dbtable=dbtable).load()
    lines = sc.textFile(settings.BASE_DIR+'/system/data/roll_news_sina_com_cn.csv')
    parts = lines.map(lambda l:l.split(','))
    schemaNews = parts.map(lambda p : Row(category=p[0],title=p[1],url=p[2],time=p[3]))
    news = sqlContext.createDataFrame(schemaNews)
    # news.registerTempTable('test')
    # dbtable = 'networkPublicOpinionAnalysisSystem.test'
    # news.write.format('jdbc').options(url=url).insertInto(tableName=dbtable)
    # string = news.count()
    row = news.first()
    a = Row()
    print(type(news))
    print(type(row))
    # print(type(a))
    # dict = row.asDict()
    # string = dict['title']

    # news.write.jdbc(url,table=dbtable)
    return render(request,'index.html',{'string':string,'list':list,'tuple':tuple,'dict':dict})

def loadRecord(line):
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=['category_id','title','url','time'])
    return reader.next()
def test(request):
    return HttpResponse(u'使用HttpResponse传递变量')
