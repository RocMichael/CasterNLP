import requests
import json, time

host = 'http://localhost:8003'


def test_sync_add():
    params = {
        'x': 1,
        'y': 2
    }
    response = requests.post(host + '/sync/add/', data=params)
    print(response.text)


def test_submit_add():
    params = {
        'x': 1,
        'y': 2
    }
    response = requests.post(host + '/submit/add/', data=params)
    print(response.text)
    result = json.loads(response.text)
    return result['token']


def test_result(token):
    params = {
        'token': token
    }
    response = requests.post(host + '/result/', data=params)
    print(response.text.encode('utf-8').decode('unicode_escape'))


def test_add():
    test_sync_add()
    token = test_submit_add()
    test_result(token)


def test_sync_seg():
    params = {
        'text': '我只是做了一些微小的工作'
    }
    response = requests.post(host + '/sync/seg/', data=params)
    print(response.text.encode('utf-8').decode('unicode_escape'))


def test_submit_seg():
    params = {
        'text': '我只是做了一些微小的工作'
    }
    response = requests.post(host + '/submit/seg/', data=params)
    print(response.text)
    result = json.loads(response.text)
    return result['token']


def test_seg():
    test_sync_seg()
    token = test_submit_seg()
    test_result(token)


if __name__ == '__main__':
    test_seg()
