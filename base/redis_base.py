from redis import Redis

from hashlib import md5
from random import randint

import json
import time


def get_time():
    return int(time.time() % 1e6 )


class RedisSession:
    def __init__(self, table):
        self.session = Redis()
        self.table = table

    def get(self, key):
        redis_val = self.session.hget(self.table, key)
        if redis_val is None:
            return None
        else:
            return json.loads(redis_val.decode('utf-8'))

    def set(self, key, val):
        redis_val = json.dumps(val)
        self.session.hset(self.table, key, redis_val)

    def pop(self, key):
        self.session.hdel(self.table, key)

    def __iter__(self):
        for key in self.session.hkeys(self.table):
            yield key


def make_token(name):
    return name + str(get_time()) + str(randint(12768, 25535))

redis_session = RedisSession('NaaS')


# def cache_result(name):
#     def decorator(func):
#         def wrapper(*args):
#             token = make_token(name)
#             result = func(*args)
#             redis_session.set(token, result)
#             return token
#         return wrapper
#     return decorator()


def cache_result(name, result):
    token = make_token(name)
    redis_session.set(token, result)
    return token


def get_result_reserve(token):
    return redis_session.get(token)


def get_result_destroy(token):
    result = redis_session.get(token)
    redis_session.pop(token)
    return result

get_result = get_result_reserve

if __name__ == '__main__':
    token = cache_result("add", 2)
    print(get_result(token))
