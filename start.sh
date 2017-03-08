#!/usr/bin/env bash
redis-server &
celery -A task worker -l info --pool solo

