__author__ = 'Bryan'

from mrjob.job import MRJob
from mrjob.step import MRStep
import time, string

import operator

# https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do (what is the yield keyword)

class MRWordCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,reducer=self.reducer1),
            #MRStep(reducer=self.reducer2)
        ]

    def mapper1(self, key, line):
        # word in each line

        for word in line.split():
            yield word[0].lower(), 1

    def reducer1(self, word, counts):
        # count words
        alphabet = list(string.ascii_lowercase)
        for char in word:
            if(char in alphabet):
                yield char,sum(counts)

if __name__ == '__main__':

    # run: python count_word.py -o 'output dir' --no-output 'location input file or files'
    # e.g. python count_word.py -o 'results count word' --no-output

    st = time.time()
    MRWordCounter.run()
    end = time.time()
    print(end-st)
