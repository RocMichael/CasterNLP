from flask import request, render_template
from flask import Flask

from form import AddForm, ResultForm
from base.redis_base import make_token, get_result
import task

import json

app = Flask('NaaS')


@app.route('/')
@app.route('/index/')
def index():
    return "Welcome to NaaS"


@app.route('/sync/add/', methods=['POST'])
def sync_add():
    response = {}
    form = AddForm(request.form)
    if not form.validate():
        response['status'] = 'ERROR_INVALID_FORM'
        return response
    data = form.data

    x = data['x']
    y = data['y']
    result = x + y

    response['status'] = 'ok'
    response['result'] = result
    return json.dumps(response)


@app.route('/submit/add/', methods=['POST'])
def submit_add():
    response = {}
    form = AddForm(request.form)
    if not form.validate():
        response['status'] = 'ERROR_INVALID_FORM'
        return response
    data = form.data

    x = data['x']
    y = data['y']
    token = make_token('add')
    task.async_add.delay(token, x, y)

    response['status'] = 'ok'
    response['token'] = token
    return json.dumps(response)


@app.route('/result/add/', methods=['POST'])
def result_add():
    response = {}
    form = ResultForm(request.form)
    if not form.validate():
        response['status'] = 'ERROR_INVALID_FORM'
        return response
    data = form.data

    token = data['token']
    result = get_result(token)
    response['status'] = 'ok'
    response['result'] = result
    return json.dumps(response)


