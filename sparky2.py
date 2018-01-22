# python sparky2.py

import re
import sys
from pyspark import SparkConf, SparkContext

SparkContext.setSystemProperty('spark.executor.memory', '8g')
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile('soc-data3.txt')

def separator(line):
    pairs = re.split('\t',line)
    pairs[1] = re.split(',',pairs[1])

    return pairs

pairs = lines.map(separator)
users = pairs.map(lambda x: x[0])
frens = pairs.map(lambda x: x[1])


numpa = pairs.cartesian(pairs)

def quantifier(elem1):
    if(elem1[0][0] in elem1[1][1] or elem1[0][0] == elem1[1][0]):
        val = -1
    else:
        val = len([x for x in elem1[0][1] if x in elem1[1][1]])
    return (elem1[0][0], [(val, elem1[1][0])])

numpa = numpa.map(quantifier)

def gather(elem1, elem2):
    return elem1+elem2

numpa = numpa.reduceByKey(gather)

def top5(elem1):
    return (elem1[0], sorted(elem1[1])[::-1][:10])

recos = numpa.map(top5)

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
numpa.saveAsTextFile('output5')
#numga.saveAsTextFile('output5')
recos.saveAsTextFile('output6')

sc.stop()