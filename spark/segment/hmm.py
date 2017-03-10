from base.spark_base import spark_context, sql_context, spark_session, Row
from config import data_path
from stop_words import stop_words

import pickle
import json

STATES = {'B', 'M', 'E', 'S'}


def get_tags(src):
    tags = []
    if len(src) == 1:
        tags = ['S']
    elif len(src) == 2:
        tags = ['B', 'E']
    else:
        m_num = len(src) - 2
        tags.append('B')
        tags.extend(['M'] * m_num)
        tags.append('S')
    return tags


def cut_sent(src, tags):
    cut_str = ""
    start = -1
    started = False

    if len(tags) != len(src):
        return None

    if tags[-1] not in {'S', 'E'}:
        if tags[-2] in {'S', 'E'}:
            tags[-1] = 'S'  # for tags: r".*(S|E)(B|M)"
        else:
            tags[-1] = 'E'  # for tags: r".*(B|M)(B|M)"

    for i in range(len(tags)):
        if tags[i] == 'S':
            if started:
                started = False
                cut_str += src[start:i] + '/'  # for tags: r"BM*S"
            cut_str += src[i] + '/'
        elif tags[i] == 'B':
            if started:
                cut_str += src[start:i] + '/'  # for tags: r"BM*B"
            start = i
            started = True
        elif tags[i] == 'E':
            started = False
            word = src[start:i+1]
            cut_str += word + '/'
        elif tags[i] == 'M':
            continue
    return cut_str


class SparkHMMSegger:
    def __init__(self, context=None, session=None):
        self.context = context
        self.session = session
        self.trans_mat = None  # {start, target: num} dict
        self.emit_mat = None  # (state, observe, num) dataframe
        self.init_vec = None  # {state: num} dict
        self.state_count = None  # {state: num} dict
        self.data = None

    def load(self, filename="hmm.json"):
        filename = data_path(filename)
        fr = open(filename, 'r')
        txt = fr.read()
        model = json.loads(txt)

        # build emit mat
        mat = model['emit_mat']
        data = []
        for state in STATES:
            for observe in mat[state]:
                data.append((state, observe, mat[state][observe]))
        self.emit_mat = self.context.parallelize(data)

        # build others
        self.trans_mat = model['trans_mat']
        self.init_vec = model['init_vec']
        self.state_count = model['state_count']

    def get_prob(self):
        init_vec = {}
        trans_mat = {}
        emit_mat = {}

        # convert init_vec from num to prob
        for key in self.init_vec:
            init_vec[key] = float(self.init_vec[key]) / self.state_count[key]

        # convert trans_mat from num to prob
        for key1 in self.trans_mat:
            trans_mat[key1] = {}
            for key2 in self.trans_mat[key1]:
                trans_mat[key1][key2] = float(self.trans_mat[key1][key2]) / self.state_count[key1]

        # convert emit_mat from num to prob
        state_count = self.state_count  # unbound with self, especially self.context
        emit_mat = self.emit_mat.map(lambda x: (x[0], x[1], x[2] / state_count[x[0]]))\
            .toDF(['state', 'observe', 'prob'])

        return init_vec, trans_mat, emit_mat

    def predict(self, sentence):
        tab = [{}]
        path = {}
        init_vec, trans_mat, emit_mat = self.get_prob()

        # init
        rows = emit_mat.rdd.filter(lambda x: x.observe == sentence[0]).map(lambda x: (x.state, init_vec[x.state] * x.prob))\
            .map(lambda x: Row(state=x[0], prob=x[1])).collect()
        for row in rows:
            tab[0][row.state] = row.prob
            path[row.state] = [row.state]

        # build dynamic search table
        for t in range(1, len(sentence)):
            tab.append({})
            new_path = {}
            for state1 in STATES:
                items = []
                tmp_emit = emit_mat.rdd.filter(lambda row: row.state == state1)
                for state2 in STATES:
                    if tab[t - 1][state2] == 0:
                        continue
                    emit_prob = tmp_emit.filter(lambda row: row.observe == sentence[t]).first().prob
                    prob = tab[t - 1][state2] * trans_mat[state2].get(state1, 0) * emit_prob
                    items.append((prob, state2))
                best = max(items)  # best: (prob, state)
                tab[t][state1] = best[0]
                new_path[state1] = path[best[1]] + [state1]
            path = new_path

        # search best path
        prob, state = max([(tab[len(sentence) - 1][state], state) for state in STATES])
        return path[state]

    def cut(self, sentence):
        tags = self.predict(sentence)
        return cut_sent(sentence, tags)


if __name__ == '__main__':
    num = spark_context.parallelize([1, 2, 3, 4]).count()
    s = SparkHMMSegger(spark_context)
    s.load()
    print(s.cut("我来到北京清华大学"))
