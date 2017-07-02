# -*- coding: utf-8 -*-
# import sys
# sys.path.append('..')

from django.apps import AppConfig

class App(AppConfig):
	name = 'qsettings'
	mainView=''
	#in lowercase because app label is used in DB table names
	label = 'qsettings' 
	verbose_name = "Qsettings main"

