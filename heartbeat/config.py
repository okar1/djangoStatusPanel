# -*- coding: utf-8 -*-
from django.apps import AppConfig


class App(AppConfig):

    name = 'heartbeat'
    # in lowercase because app label is used in DB table names
    label = 'heartbeat'
    verbose_name = "Heartbeat"
    policycheck_name="Проверка оповещений"
