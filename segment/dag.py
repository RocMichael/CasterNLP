# encoding=utf-8
from base.spark_base import spark_context, sql_context, spark_session
from config import data_path
from stop_words import stop_words

import pickle
import json


class SparkDAGSegger:
    def __init__(self, context=None, session=None):
        self.context = context
        self.session = session
        self.word_dict = None  # (word:num) dataframe
        self.raw_data = None  # raw data rdd

    def read(self, filename):
        filepath = data_path(filename)
        self.raw_data = self.context.textFile(filepath)

    def update(self):
        # build word_dict by word_count
        word_count = self.raw_data.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))\
            .reduceByKey(lambda x, y: x + y)
        self.word_dict = word_count.map(lambda x: (x[0], int(x[1]))).toDF(['word', 'num'])

    def load(self, filename="words.txt"):
        filepath = data_path(filename)
        data = self.context.textFile(filepath)
        self.word_dict = data.map(lambda x: tuple(x.split(' ')))\
            .map( lambda x: (x[0], int(x[1])) ).toDF(['word', 'num'])

    def search(self, word):
        return self.word_dict.filter(lambda item: word == item[0]).first()[1]

    def build_dag(self, sentence):
        # `[ sentence[t[0]:t[1]]) for t in init_data]` is a set of all sentence substring
        # init_data = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        init_data = [(start, stop) for start in range(len(sentence)) for stop in range(start+1, len(sentence)+1)]
        init_rdd = self.context.parallelize(init_data)

        fragment_df = init_rdd.map(lambda t: (sentence[t[0]:t[1]], t[0], t[1])).toDF(['word', 'start', 'stop'])
        # word_df = [word, start, stop, num], only word in dict
        word_df = fragment_df.join(self.word_dict, 'word')

        # add default: [(char, 1, 2, num=1), (3,4)]
        default_data = [(sentence[i:i+1], i, i+1, 1) for i in range(len(sentence))]
        default_rdd = self.context.parallelize(default_data).toDF(['word', 'start', 'stop', 'num']).rdd
        dag_rdd = word_df.rdd.union(default_rdd).sortBy(lambda x: x.start)

        return dag_rdd

    def predict(self, sentence):
        Len = len(sentence)
        route = [(0, 0)] * Len
        dag_rdd = self.build_dag(sentence)
        for i in range(Len - 1, -1, -1):
            route[i] = dag_rdd.filter(lambda x: x.start == i).max(lambda x: x.num).stop
        return route

    def cut(self, sentence):
        route = self.predict(sentence)
        next = 0
        cut_str = ""
        i = 0
        while i < len(sentence):
            next = route[i]
            cut_str += sentence[i:next] + '/'
            i = next
        return cut_str[:-1]

if __name__ == '__main__':
    s = SparkDAGSegger(spark_context)
    s.load()
    print(s.cut("我来到北京清华大学"))
