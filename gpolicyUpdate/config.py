# -*- coding: utf-8 -*-
# import sys
# sys.path.append('..')

from django.apps import AppConfig


class App(AppConfig):
	name = 'gpolicyUpdate'
	#in lowercase because app label is used in DB table names
	label = 'gpolicyupdate' 
	verbose_name = "Обновление групповых политик"
	# warning! sometimes it executes 2 or more times at startup!
	#def ready(self):
	#	defaults.initDefaultSettings()


