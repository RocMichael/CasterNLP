import os

data_dir = 'data'


def data_path(filename):
    return os.path.join(os.path.dirname(__file__), "%s/%s" % (data_dir, filename))

