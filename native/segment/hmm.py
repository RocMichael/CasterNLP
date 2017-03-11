# encoding=utf-8
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
    word_list = []
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
                word_list.append(src[start:i])  # for tags: r"BM*S"
            word_list.append(src[i])
        elif tags[i] == 'B':
            if started:
                word_list.append(src[start:i])  # for tags: r"BM*B"
            start = i
            started = True
        elif tags[i] == 'E':
            started = False
            word = src[start:i+1]
            word_list.append(word)
        elif tags[i] == 'M':
            continue
    return word_list


class HMMSegger:
    def __init__(self):
        self.trans_mat = {}  # trans_mat[status][status] = int
        self.emit_mat = {}  # emit_mat[status][observe] = int
        self.init_vec = {}  # init_vec[status] = int
        self.state_count = {}  # state_count[status] = int
        self.word_set = set()
        self.inited = False
        self.line_num = 0
        self.data = None

    def setup(self):
        for state in STATES:
            # build trans_mat
            self.trans_mat[state] = {}
            for target in STATES:
                self.trans_mat[state][target] = 0.0
            # build emit_mat
            self.emit_mat[state] = {}
            # build init_vec
            self.init_vec[state] = 0
            # build state_count
            self.state_count[state] = 0
        self.word_set = set()
        self.line_num = 0
        self.inited = True

    def load_data(self, filename):
        self.data = open(data_path(filename), 'r', encoding="utf-8")

    def save(self, filename="hmm.json", code="json"):
        filename = data_path(filename)
        fw = open(filename, 'w')
        data = {
            "trans_mat": self.trans_mat,
            "emit_mat": self.emit_mat,
            "init_vec": self.init_vec,
            "state_count": self.state_count
        }
        if code == "json":
            txt = json.dumps(data)
            fw.write(txt)
        elif code == "pickle":
            pickle.dump(data, fw)

    def load(self, filename="hmm.json", code="json"):
        filename = data_path(filename)
        fr = open(filename, 'r')
        if code == "json":
            txt = fr.read()
            model = json.loads(txt)
        elif code == "pickle":
            model = pickle.load(fr)
        self.trans_mat = model["trans_mat"]
        self.emit_mat = model["emit_mat"]
        self.init_vec = model["init_vec"]
        self.state_count = model["state_count"]
        self.inited = True

    def train(self):
        if not self.inited:
            self.setup()
        for line in self.data:
            # pre processing
            line = line.strip()
            if not line:
                continue
            self.line_num += 1

            # update word_set
            word_list = []
            for i in range(len(line)):
                if line[i] == " ":
                    continue
                word_list.append(line[i])
            self.word_set = self.word_set | set(word_list)

            # get tags
            words = line.split(" ")  # spilt word by whitespace
            line_tags = []
            for word in words:
                if word in stop_words:
                    continue
                line_tags.extend(get_tags(word))

            # update model params
            for i in range(len(line_tags)):
                if i == 0:
                    self.init_vec[line_tags[0]] += 1
                    self.state_count[line_tags[0]] += 1
                else:
                    self.trans_mat[line_tags[i-1]][line_tags[i]] += 1
                    self.state_count[line_tags[i]] += 1
                    if word_list[i] not in self.emit_mat[line_tags[i]]:
                        self.emit_mat[line_tags[i]][word_list[i]] = 0.0
                    else:
                        self.emit_mat[line_tags[i]][word_list[i]] += 1

    def get_prob(self):
        init_vec = {}
        trans_mat = {}
        emit_mat = {}
        # convert init_vec to prob
        for key in self.init_vec:
            init_vec[key] = float(self.init_vec[key]) / self.state_count[key]
        # convert trans_mat to prob
        for key1 in self.trans_mat:
            trans_mat[key1] = {}
            for key2 in self.trans_mat[key1]:
                trans_mat[key1][key2] = float(self.trans_mat[key1][key2]) / self.state_count[key1]
        # convert emit_mat to prob
        for key1 in self.emit_mat:
            emit_mat[key1] = {}
            for key2 in self.emit_mat[key1]:
                emit_mat[key1][key2] = float(self.emit_mat[key1][key2]) / self.state_count[key1]
        return init_vec, trans_mat, emit_mat

    def predict(self, sentence):
        tab = [{}]
        path = {}
        init_vec, trans_mat, emit_mat = self.get_prob()

        # init
        for state in STATES:
            tab[0][state] = init_vec[state] * emit_mat[state].get(sentence[0], 0)
            path[state] = [state]

        # build dynamic search table
        for t in range(1, len(sentence)):
            tab.append({})
            new_path = {}
            for state1 in STATES:
                items = []
                for state2 in STATES:
                    if tab[t - 1][state2] == 0:
                        continue
                    prob = tab[t - 1][state2] * trans_mat[state2].get(state1, 0) * emit_mat[state1].get(sentence[t], 0)
                    items.append((prob, state2))
                best = max(items)  # best: (prob, state)
                tab[t][state1] = best[0]
                new_path[state1] = path[best[1]] + [state1]
            path = new_path

        # search best path
        prob, state = max([(tab[len(sentence) - 1][state], state) for state in STATES])
        return path[state]

    def cut(self, sentence):
        try:
            tags = self.predict(sentence)
            return cut_sent(sentence, tags)
        except:
            return sentence

    def test(self):
        cases = [
            "我来到北京清华大学",
            "长春市长春节讲话",
            "我们去野生动物园玩",
            "我只是做了一些微小的工作",
        ]
        for case in cases:
            result = self.cut(case)
            for word in result:
                print(word)
            print('')

if __name__ == '__main__':
    segger = HMMSegger()
    # segger.load_data("people_daily.txt")
    # segger.train()
    # segger.save()
    segger.load()
    segger.test()

