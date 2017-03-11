from flask import request, render_template
from flask import Flask

from native.segment import cut

from form import AddForm, ResultForm, TextForm
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


@app.route('/result/', methods=['POST'])
def get_result():
    response = {}
    form = ResultForm(request.form)
    if not form.validate():
        response['status'] = 'ERROR_INVALID_FORM'
        return response
    data = form.data

    token = data['token']
    result = get_result(token)
    if result is None:
        response['status'] = 'pending'
    else:
        response['status'] = 'ok'
        response['result'] = result
    return json.dumps(response)


@app.route('/sync/seg/', methods=['POST'])
def sync_seg():
    response = {}
    form = TextForm(request.form)
    if not form.validate():
        response['status'] = 'ERROR_INVALID_FORM'
        return response
    data = form.data

    text = data['text']
    result = cut(text)

    response['status'] = 'ok'
    response['result'] = result
    return json.dumps(response)


@app.route('/submit/seg/', methods=['POST'])
def submit_seg():
    response = {}
    form = TextForm(request.form)
    if not form.validate():
        response['status'] = 'ERROR_INVALID_FORM'
        return response
    data = form.data

    text = data['text']
    token = make_token('seg')
    task.async_seg.delay(token, text)

    response['status'] = 'ok'
    response['token'] = token
    return json.dumps(response)

