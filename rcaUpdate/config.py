# -*- coding: utf-8 -*-
# import sys
# sys.path.append('..')

from django.apps import AppConfig

class App(AppConfig):
	name = 'rcaUpdate'
	#in lowercase because app label is used in DB table names
	label = 'rcaupdate' 
	verbose_name = "Обновление RCA"
	# warning! sometimes it executes 2 or more times at startup!
	#def ready(self):
	#	defaults.initDefaultSettings()


