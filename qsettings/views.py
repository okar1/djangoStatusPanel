# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.views.generic.edit import FormView
from django import forms
from . import urls



class  MainForm(forms.Form):
	headString="Qsettings main menu"
	annotationString="Установленные модули:"
	showResetButton=False
	autoRefreshInterval=0
	required_css_class = 'bootstrap3-req'
	result={}

	# Set this to allow tests to work properly in Django 1.10+
	# More information, see issue #337
	use_required_attribute = False
	
# *******************************************
# *******************************************
# *******************************************
# *******************************************

class MainMenuView(FormView):
	template_name = 'base.html'
	success_url='/' 
	form_class=MainForm

	# def appname(request):
	# 	return {'appname': resolve(request.path).app_name}

	# def appName(self):
	# 	return (self.__module__).split('.')[0]


	def get_context_data(self, **kwargs):
		context = super().get_context_data(**kwargs)
		context['headString'] = self.form_class.headString
		context['annotationString'] = self.form_class.annotationString.replace('\n', '<br />')
		context['mainMenu']=urls.mainMenu
		context['thisClass']=self.__class__.__module__+'.'+self.__class__.__name__
		# context['thisMenuItem']=self.appName()
		# context['thisMenuItem']=self.appName()
		# context['rrr']=self.request.path[1:]
		# print(resolve(self.request.path).func.__module__)

		
		# context['p1']=self.request.path
		# tmp=resolve(self.request.path).func
		# context['p1']=tmp.__module__+'.'+tmp.__name__
		

		return context



