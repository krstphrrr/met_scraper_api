from celery import Celery

cel = Celery()

cel.config_from_object('src.celeryconfig')
