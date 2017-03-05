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


def test_result_add(token):
    params = {
        'token': token
    }
    response = requests.post(host + '/result/add/', data=params)
    print(response.text)


def test_add():
    test_sync_add()
    token = test_submit_add()
    test_result_add(token)


if __name__ == '__main__':
    test_add()
