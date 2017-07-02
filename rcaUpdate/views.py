# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.views.generic.edit import FormView
from django import forms
# from qsettings.rcaDataSource import query
from .controller import query2viewList,pageGen,incPageIndex,getPageData,savePageData
from django.http import HttpResponse
import json,time,random

class BaseDialogForm(forms.Form):
	headString="Title example"
	annotationString="Annotation example"
	showResetButton=False
	autoRefreshInterval=0
	required_css_class = 'bootstrap3-req'
	result={}

	# Set this to allow tests to work properly in Django 1.10+
	# More information, see issue #337
	use_required_attribute = False

	def backButtonClick(self):
		# print("back button pressed"+str(dialogGenerator.pageIndex))
		incPageIndex(-1)


	def fwButtonClick(self):
		# print("fw button pressed "+str(dialogGenerator.pageIndex))
		incPageIndex(1)

	buttons=[['<-- Назад','backButtonClick'],['Вперед -->','fwButtonClick']]

	# проверка правильности заполнения формы
	def clean(self):
		self.result = super().clean()
		if not super().is_valid():
			return self.result
		# print(self.request())
		# print("valid ",self.form_valid)
		# raise forms.ValidationError('no change.')
		# raise forms.ValidationError("init message")        
		return self.result


	# если в форме нет ошибок
	# или если пользователь перезагрузил форму с ошибкой
	def is_valid(self):
		res=super().is_valid()
		if res==False: 
			return False
		self.formIsOk()
		self.doButtonAction()
		return res

	#для переопределения в child class
	#тут у child класса есть доступ к cleaned_data 
	def formIsOk(self):    
		pass

	
	def doButtonAction(self):
		if super().is_valid():
			for i in range(len(self.buttons)):
				checkName="button"+str(i)
				if checkName in self.data.keys():
					#вызываем обработчик кнопки по имени.
					#Если будем вызывать ссылку на обработчик а не имя, тогда
					#дочерние классы будут вызывать "родительские" обработчики вместо своих
					getattr(self, self.buttons[i][1])()   
					break

		
# *******************************************
# *******************************************
# *******************************************
# *******************************************
class WelcomeDialogForm(BaseDialogForm):
	headString="Preved"
	annotationString="Welcome message"

# *******************************************
# *******************************************
# *******************************************
# *******************************************
class SelectRuleDetailDialogForm(BaseDialogForm):
	headString="Preved"
	annotationString="rule detail"
	ch=(
			('1', 'Все БК'),
			('2', 'Один БК'),
			('3', 'Канал'),
			('4', 'Модуль'),
		)
	detail = forms.ChoiceField(choices=ch, widget=forms.RadioSelect, label='')
	
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.initial=getPageData()

	def formIsOk(self):
		savePageData(self.cleaned_data)
		
# *******************************************
# *******************************************
# *******************************************
# *******************************************
class WaitDialogForm(BaseDialogForm):
	headString="Please wait"
	annotationString="doing a thing"
	autoRefreshInterval=5
	buttons=[]

# *******************************************
# *******************************************
# *******************************************
# *******************************************
class UnauthorizedDialogForm(BaseDialogForm):
	headString="Unauthorized"
	annotationString=""
	buttons=[]

# *******************************************
# *******************************************
# *******************************************
# *******************************************
class SelectRuleItemsDialogForm(BaseDialogForm):
	headString="select items"
	annotationString="select items and next to continue\nEmpty select means over"
	
	selectBK = forms.MultipleChoiceField(
		label="Блоки контроля",
		required=False,
	)
	selectChannel = forms.MultipleChoiceField(
		label="Каналы",
		required=False
	)
	selectModule = forms.MultipleChoiceField(
		label="Модули сбора данных",
		required=False
	)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		choices=getPageData(-1)

		if not "agent" in choices:
			pass
			# self.agentList=query(resultType=("agent",)) 
		else:
			self.agentList=choices['agent']

		if not "channel" in choices:
			# self.channelList=query(resultType=("channel",)) 
			pass
		else:
			self.channelList=choices['channel']

		if not "module" in choices:
			# self.moduleList=query(resultType=("module",))
			pass
		else:
			self.moduleList=choices['module']

		self.fields['selectBK'].choices=query2viewList(self.agentList)
		self.fields['selectBK'].help_text=str(len(self.agentList))+" шт"
		self.fields['selectChannel'].choices=query2viewList(self.channelList)
		self.fields['selectChannel'].help_text=str(len(self.channelList))+" шт"
		self.fields['selectModule'].choices=query2viewList(self.moduleList)
		self.fields['selectModule'].help_text=str(len(self.moduleList))+" шт"

	def formIsOk(self):
		data=self.cleaned_data
		agentData=data['selectBK']
		channelData=data['selectChannel']
		moduleData=data['selectModule']
		if len(agentData)+len(channelData)+len(moduleData)==0:
			pageGen.selectDone=True

		newAgentList=[]
		for index in agentData:
			newAgentList.append(self.agentList[int(index)])
		newChannelList=[]
		for index in channelData:
			newChannelList.append(self.channelList[int(index)])
		newModuleList=[]
		for index in moduleData:
			newModuleList.append(self.moduleList[int(index)])

		newData=''
		# newData={'agent':query(resultType=("agent",),agentList=newAgentList, channelList=newChannelList,moduleList=newModuleList),
			# 'channel':query(resultType=("channel",),agentList=newAgentList, channelList=newChannelList,moduleList=newModuleList),
			# 'module':query(resultType=("module",),agentList=newAgentList, channelList=newChannelList,moduleList=newModuleList)}
		savePageData(newData)
# *******************************************
# *******************************************
# *******************************************
# *******************************************
class WeAreReadyToProcess(BaseDialogForm):
	headString="We are ready to process"
	annotationString='press "next" button to generate files'

# *******************************************
# *******************************************
# *******************************************
# *******************************************
def static(setValue=None):
	# if not hasattr(static, "l"):
	if setValue!=None:
		static.l = setValue
	if not hasattr(static, "l"):
		static.l = setValue
	return static.l



import os

from multiprocessing import Pool

import functools
import urllib.request
from concurrent import futures




class UpdateRouter():
	def worker(self,task):
		# print("start ",task)
		time.sleep(random.random()*10)
		print("end ",task) 
		del UpdateRouter.taskDict[task]
		# print (UpdateRouter.taskDict)
		# input("enter ")
		return task

	def __init__(self, taskList):
		UpdateRouter.taskDict={s:s for s in taskList}
		print("created router for taskList:",UpdateRouter.taskDict)

		with futures.ThreadPoolExecutor(40) as executor:
			UpdateRouter.futureDict = dict((executor.submit(self.worker, task), task) for task in UpdateRouter.taskDict.keys())

		print("exiting")

			# for future in futures.as_completed(UpdateRouter.futureDict):
			# 	url = UpdateRouter.futureDict[future]
			# 	if future.exception() is not None:
			# 		print('%r generated an exception: %s' % (url,future.exception()))
			# 	else:
			# 		print("f result=",future.result())
			


	def status(self):
		return UpdateRouter.taskDict











class DialogFormView(FormView):
	# template_name = 'dialogForm.html'
	# success_url='rca' 
	template_name = 'cwForm.html'


	def post(self, request, *args, **kwargs):
		if not request.user.is_authenticated():
			return HttpResponse("")

		if request.is_ajax():
			req = json.loads(request.body.decode('utf-8'))
			print("start req")
			if "startUpdate" in req.keys():
				print ("create router")
				DialogFormView.router=UpdateRouter(req['data'])
				print ("done")
			print("1")
			# elif "continueUpdate" in req.keys():	
				# pass
			# sleepTime=random.random()*10
			# sleepTime=10
			# print("req received, sleeping ",sleepTime)
			# time.sleep(sleepTime)
			# print("send reply after",sleepTime)
			if hasattr(DialogFormView, "router"):
				print(DialogFormView.router.status())
			print("2")				
			return(HttpResponse(json.dumps({"data:":"111"})))


	def get_form(self, form_class=None):
		if self.form_class is None:
			sk=self.request.session.session_key
			if sk==None:
				self.request.session.flush()
				self.request.session.cycle_key()  
			sk=self.request.session.session_key
			self.form_class = pageGen(sk)
		return super().get_form(form_class)

	def get_context_data(self, **kwargs):
		context_data = super().get_context_data(**kwargs)
		context_data['headString'] = self.form_class.headString
		context_data['annotationString'] = self.form_class.annotationString.replace('\n', '<br />')
		buttonLayout=""
		for i in range(len(self.form_class.buttons)):
			btName=self.form_class.buttons[i][0]
			buttonLayout+="""
			<button type="submit" name="button{0}" class="btn btn-default">
			  {1}
			</button>""".format(i,btName)
		if self.form_class.showResetButton:
			buttonLayout+="""
			<button type="reset" class="btn btn-default">
			   Сброс
			</button>"""
		context_data['buttonLayout']=buttonLayout
		autoRefresh="" if self.form_class.autoRefreshInterval==0 else \
			'<meta http-equiv="refresh" content="{0}" >'.format(str(self.form_class.autoRefreshInterval))
		context_data['autoRefresh']=autoRefresh    
		return context_data





