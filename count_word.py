__author__ = 'Bryan'

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

import operator

# https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do (what is the yield keyword)

class MRWordCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]

    def mapper1(self, key, line):
        # word in each line

        for word in line.split():
            yield word, 1

    def reducer1(self, word, counts):
        # count words

        total = sum(counts)

        yield None,(total,word)
        #yield None,sorted((total, word))[::-1][:100]

    def reducer2(self, _, pairs):
        # max 100 words
        self.list = []
        for p in pairs:
            self.list.append(p)
        for i in range(100):
            yield sorted(self.list)[::-1][i]

if __name__ == '__main__':

    # run: python count_word.py -o 'output dir' --no-output 'location input file or files'
    # e.g. python count_word.py -o 'results count word' --no-output

    st = time.time()
    MRWordCounter.run()
    end = time.time()
    print(end-st)
