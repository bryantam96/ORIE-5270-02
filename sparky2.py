# python sparky2.py

import re
from pyspark import SparkConf, SparkContext

SparkContext.setSystemProperty('spark.executor.memory', '8g')
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile('soc-data.txt')

def separator(line):
    pairs = re.split('\t',line)
    pairs[1] = re.split(',',pairs[1])

    return pairs

pairs = lines.map(separator)

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

pairs.saveAsTextFile('output1')
numpa.saveAsTextFile('output2')
recos.saveAsTextFile('output3')

sc.stop()