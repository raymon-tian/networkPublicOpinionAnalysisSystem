#coding=utf-8
import re
from string import punctuation,digits,letters,whitespace
import sys
import myuntils

"""字符串过滤"""
temp = "想做/ 兼_职/学生_/ 的 、加,我Q：  1 5.  8 0. ！！？？  8 6 。0.  2。 3     有,惊,喜,哦"
temp2 = temp.decode("utf8")
string = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), "".decode("utf8"),temp2)
string = string.encode('utf-8')
string.translate(None,punctuation)
string.translate(None,digits)
string.translate(None,letters)
string.translate(None,whitespace)
print(type(temp))
print(type(string))
print string

string = '中国'
s1 = string.decode('utf-8')
print(type(s1))
print(sys.getdefaultencoding())

string = '1111sfsdfs所发生的11 ：南京航空航天大学: ,11sdfsdf王栋1111'
string = myuntils.handleAndCut(string)
print(type(string))
string = string.encode('utf-8')
print(type(string))


