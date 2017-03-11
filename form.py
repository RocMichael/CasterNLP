from wtforms import Form, validators
from wtforms import StringField, FloatField


class AddForm(Form):
    x = FloatField('x', [validators.required()])
    y = FloatField('y', [validators.required()])


class ResultForm(Form):
    token = StringField('token', [validators.required()])


class TextForm(Form):
    text = StringField('text', [validators.required()])
