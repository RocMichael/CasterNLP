from base.redis_base import redis_session
from celery import Celery

from SaberNLP.segment import cut

app = Celery('task', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/1')


@app.task(ignore_result=False)
def async_add(token, x, y):
    result = x + y
    redis_session.set(token, result)


@app.task(ignore_result=False)
def async_seg(token, text):
    result = cut(text)
    redis_session.set(token, result)


if __name__ == '__main__':
    import time
    time.sleep(1)
    result = async_add.delay('add71572316353', 1, 2)
    print(result.status)
