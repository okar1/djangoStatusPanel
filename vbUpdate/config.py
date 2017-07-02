# -*- coding: utf-8 -*-
# import sys
# sys.path.append('..')

from django.apps import AppConfig

class App(AppConfig):
	name = 'vbUpdate'
	#in lowercase because app label is used in DB table names
	label = 'vbupdate' 
	verbose_name = "Обновление каналов VB"
	# warning! sometimes it executes 2 or more times at startup!
	#def ready(self):
	#	defaults.initDefaultSettings()


