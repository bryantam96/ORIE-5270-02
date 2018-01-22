__author__ = 'Bryan'

import re
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile('soc-data.txt')

def separator(line):
    pairs = re.split('\t',line)
    pairs[1] = re.split(',',pairs[1])

    return pairs

pairs = lines.map(separator)
users = pairs.map(lambda x: x[0])
frens = pairs.map(lambda x: x[1])


numfr = pairs.cartesian(pairs)

"""
def quantifier(elem1):
    def check(elem2):
        if(elem1[0] == elem2[0]):
            return ((elem1[0], elem2[0]), 0)
        elif(elem1[0] in elem2[1]):
            return ((elem1[0], elem2[0]), 1)
        else:
            return ((elem1[0], elem2[0]), 0)

    ret = tempp.map(check)
    return ret
"""

#"""
def quantifier(elem1):
    if(elem1[0][0] in elem1[1][1] and elem1[0][0] != elem1[1][0]):
        val = 1
    else:
        val = -1
    return ((elem1[0][0], elem1[1][0]),(elem1[0][1], elem1[1][1]), val)
#"""

numfr = numfr.map(quantifier)

def agg(elem1):
    return (sum(list(map(lambda x: int(x in elem1[1][1]),elem1[1][0]))),elem1[0],elem1[2])

#def rec(elem1, elem2):
#    return (elem1[0]+elem2[0], elem1[1], elem1[2])

def fin(elem1):
    return (elem1[1][0], (elem1[0]*elem1[2], elem1[1][1]))

numag = numfr.map(agg)
#numag = numag.reduce(rec)
numag = numag.map(fin)

def top5(self, elem1):
    self.list = []
    for p in elem1:
        self.list.append(p[1])
    ret = []
    for i in range(5):
        ret.append(sorted(self.list)[::-1][i])
    return (elem1[0], ret)

recos = numag.reduceByKey(top5)

#colls = lines.map(lambda l: re.split('\t', l))
#users = lines.map(lambda l: re.split('\t', l)[0])
#frens = colls.map(lambda l: re.split(',', l[1]))

#words = lines.flatMap(lambda l: re.split(r'[^\w]+', l))
#pairs = words.map(lambda w: (w, 1))
#counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)

"""

users[1] = users[1].flatMap(lambda l: re.split(',', l))
pairs = users[1].map(lambda w: (w, 1))
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)
"""

pairs.saveAsTextFile('output1')
users.saveAsTextFile('output2')
frens.saveAsTextFile('output3')
numfr.saveAsTextFile('output4')
numag.saveAsTextFile('output5')
recos.saveAsTextFile('output6')

sc.stop()