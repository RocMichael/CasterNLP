from base.redis_base import redis_session
from celery import Celery

from SaberNLP.segment import cut
from SaberNLP.recognize import tag


app = Celery('task', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/1')


@app.task(ignore_result=False)
def async_add(token, x, y):
    result = x + y
    redis_session.set(token, result)


@app.task(ignore_result=False)
def async_seg(token, text):
    result = cut(text)
    redis_session.set(token, result)


@app.task(ignore_result=False)
def async_tag(token, text):
    result = tag(text)
    redis_session.set(token, result)


