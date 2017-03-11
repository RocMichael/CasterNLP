# encoding=utf-8
from config import data_path
from stop_words import stop_words

import pickle
import json


class DAGSegger:
    def __init__(self):
        self.word_dict = {}
        self.data = None

    def load_data(self, filename):
        self.data = open(data_path(filename), "r", encoding="utf-8")

    def update(self):
        # build word_dict
        for line in self.data:
            words = line.split(" ")
            for word in words:
                if word in stop_words:
                    continue
                if self.word_dict.get(word):
                    self.word_dict[word] += 1
                else:
                    self.word_dict[word] = 1

    def save(self, filename="words.txt", code="txt"):
        filename = data_path(filename)
        fw = open(filename, 'w', encoding="utf-8")
        data = {
            "word_dict": self.word_dict
        }

        # encode and write
        if code == "json":
            txt = json.dumps(data)
            fw.write(txt)
        elif code == "pickle":
            pickle.dump(data, fw)
        if code == 'txt':
            for key in self.word_dict:
                tmp = "%s %d\n" % (key, self.word_dict[key])
                fw.write(tmp)

    def load(self, filename="words.txt", code="txt"):
        filename = data_path(filename)
        fr = open(filename, 'rb')

        # load model
        model = {}
        if code == "json":
            model = json.loads(fr.read())
        elif code == "pickle":
            model = pickle.load(fr)
        elif code == 'txt':
            word_dict = {}
            for line in fr:
                line = line.decode("utf-8")
                tmp = line.split(" ")
                if len(tmp) < 2:
                    continue
                word_dict[tmp[0]] = int(tmp[1])
            model = {"word_dict": word_dict}

        # update word dict
        word_dict = model["word_dict"]
        for key in word_dict:
            key2 = key  # to unicode
            if self.word_dict.get(key):
                self.word_dict[key2] += word_dict[key]
            else:
                self.word_dict[key2] = word_dict[key]

    def build_dag(self, sentence):
        dag = {}
        for start in range(len(sentence)):
            unique = [start + 1]
            tmp = [(start + 1, 1)]
            for stop in range(start+1, len(sentence)+1):
                fragment = sentence[start:stop]
                num = self.word_dict.get(fragment, 0)
                if num > 0 and (stop not in unique):
                    tmp.append((stop, num))
                    unique.append(stop)
            dag[start] = tmp
        return dag

    def predict(self, sentence):
        Len = len(sentence)
        route = [0] * Len
        dag = self.build_dag(sentence)  # {i: (stop, num)}

        for i in range(0, Len):
            route[i] = max(dag[i], key=lambda x: x[1])[0]
        return route

    def cut(self, sentence):
        route = self.predict(sentence)
        next = 0
        word_list = []
        i = 0
        while i < len(sentence):
            next = route[i]
            word_list.append(sentence[i:next])
            i = next
        return word_list

    def test(self):
        cases = [
            "我来到北京清华大学",
            "长春市长春节讲话",
            "我们去野生动物园玩",
            "我只是做了一些微小的工作",
            "国庆节我在研究中文分词"
        ]
        for case in cases:
            result = self.cut(case)
            for word in result:
                print(word)
            print('')


def test():
    s = DAGSegger()
    s.load()
    # s.load_data("trisolars.txt")
    # s.update()
    # s.save("trisolars.words.txt")
    s.test()

def cut_text():
    s = DAGSegger()
    s.load()
    with open(data_path("trisolars.raw.txt"), "r", encoding="utf-8") as f:
        for line in f:
            r = s.cut(line)
            print(r)


if __name__ == '__main__':
    cut_text()
