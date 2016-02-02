#coding=utf-8
"""将原始新闻中的title数据抽取出来就行处理，再将处理结果存储为parquet类型"""
import jieba
import re
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row,DataFrame
from string import punctuation,digits,letters,whitespace


def handleAndCut(string):
    """字符串处理，去标点符号，中文分词，return:unicode"""
    #string = string.decode('utf-8')
    string = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！：，。？、~@#￥%……&*（）]+".decode("utf-8"), "".decode("utf-8"),string)
    string = string.encode('utf-8')
    string = string.translate(None,punctuation+digits+letters+whitespace)
    seg = jieba.cut(string)
    return ''.join(seg)


#seg_list = jieba.cut("我来到北京清华大学", cut_all=True)
#print "Full Mode:", " ".join(seg_list)  # 全模式
#seg_list = jieba.cut("我来到北京清华大学", cut_all=False)
#print "Default Mode:", " ".join(seg_list)  # 精确模式
#seg_list = jieba.cut("他来到了网易杭研大厦")  # 默认是精确模式
#print " ".join(seg_list)
#seg_list = jieba.cut_for_search("小明硕士毕业于中国科学院计算所，后在日本京都大学深造")  # 搜索引擎模式
#print " ".join(seg_list)

conf = SparkConf().setAppName('tfidf').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

rawNews = sc.textFile(name='roll_news_sina_com_cn.csv')
parts = rawNews.map(lambda line:line.split(','))
#titleNews 为 unicode
titleNews = parts.map(lambda p:[p[0],p[1]])
titleNewsHandled = titleNews.map(lambda line:[line[0],handleAndCut(line[1])])
temp = titleNewsHandled.map(lambda line:Row(label=line[0],title=line[1]))
dfTitleNews = sqlContext.createDataFrame(temp)
dfTitleNews.show()
print(dfTitleNews.dtypes)
dfTitleNews.write.save('roll_news_sina_com_cn.parquet')
sc.stop()