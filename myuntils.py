#coding=utf-8
from string import punctuation,digits,letters,whitespace
import jieba
import re

def handleAndCut(string):
    """字符串处理，去标点符号，中文分词"""
    #string = string.decode('utf-8')
    string = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！：，。？、~@#￥%……&*（）]+".decode("utf-8"), "".decode("utf-8"),string)
    string = string.encode('utf-8')
    string = string.translate(None,punctuation+digits+letters+whitespace)
    seg = jieba.cut(string)
    return ' '.join(seg)
print('ddddddddddddd')
    
    