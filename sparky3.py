# python sparky2.py

import re
from pyspark import SparkConf, SparkContext

SparkContext.setSystemProperty('spark.executor.memory', '8g')
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile('soc-data.txt')

def separator(line):
    try:
        pairs = re.split('\t',line)
        pairs[1] = re.split(',',pairs[1])
        result = [pairs[0]]
        result += pairs[1]
    except:
        #print("error in separator")
        result = []

    return result

pairs = lines.map(separator)

def quantifier(elem1):
    try:
        l = len(elem1)
        result = []
    except:
        #print("error in quantifier part 0")
        pass

    for j in range(1, l):
        try:
            if(int(elem1[0]) < int(elem1[j])):
                result.append(((elem1[0], elem1[j]), -10000))
            else:
                result.append(((elem1[j], elem1[0]), -10000))
        except:
            #print("error in quantifier part 1")
            pass

    for i in range(1, l-1):
        for j in range(i+1, l):
            try:
                if(int(elem1[i]) < int(elem1[j])):
                    result.append(((elem1[i], elem1[j]), 1))
                else:
                    result.append(((elem1[j], elem1[i]), 1))
            except:
                #print("error in quantifier part 2")
                pass

    try:
        return result
    except:
        return []


numpa = pairs.flatMap(quantifier)
numga = numpa.reduceByKey(lambda x, y: x+y)
alter = numga.map(lambda x: (x[0][0],(x[1],x[0][1])))
recos = alter.groupByKey().map(lambda x: (x[0],sorted(list(x[1]))[::-1][:10]))

#pairs.saveAsTextFile('output1')
#numpa.saveAsTextFile('output2')
#numga.saveAsTextFile('output3')
#alter.saveAsTextFile('output4')
recos.saveAsTextFile('output5')

sc.stop()