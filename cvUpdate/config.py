# -*- coding: utf-8 -*-
# import sys
# sys.path.append('..')

from django.apps import AppConfig

class App(AppConfig):
	name = 'cvUpdate'
	#in lowercase because app label is used in DB table names
	label = 'cvupdate' 
	verbose_name = "Обновление ChannelView"


