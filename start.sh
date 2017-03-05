#!/usr/bin/env bash
pkill python
pkill redis-server
redis-server &
celery -A task worker -l info -P threads

