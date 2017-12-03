# -*- coding: utf-8 -*-
from django import forms
from django.http import HttpResponse, HttpResponseNotAllowed
import json,re
from concurrent import futures
from qsettings.views import MainMenuView

from django.contrib.auth.mixins import LoginRequiredMixin

class BoxForm(forms.Form):
	headString="BoxForm header"
	annotationString="BoxForm annotation"
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

class BoxFormView(LoginRequiredMixin,MainMenuView):
	template_name = 'BoxForm.html'
	# success_url='/' 
	form_class=BoxForm
	buttons=[{"name":"Применить","onclick":None},
			{"name":"Применить все","onclick":None}]

	# for override 
	def getGroups(self):
		return []

	# for override 
	def getBoxesForGroup(self,groupID):
		return []

	# for override 
	def getRecordsForBox(self,boxID):
		return []

	# for override  - return base timestamp for timer value calculation.
	# 0 means hiding timer and no autoupdate
	def getProgresUpdatedAgoSec(self):
		return -1

	# for override - return time for timer become red
	def getTimerDangerousTimeSec(self):
		# return int(time.time())
		return 3

	def __init__(self,  **kwargs):
		if not hasattr(self.__class__,"_progressArray"):
			self.__class__._progressArray=[]
			self.__class__.buttonTaskWorking=False
		super().__init__()

	@property
	def progressArray(self):
		return self.__class__._progressArray

	@progressArray.setter
	def progressArray(self,v):
		self.__class__._progressArray=v


	def get_context_data(self, **kwargs):
		context = super().get_context_data(**kwargs)
		context['headString'] = self.form_class.headString
		context['annotationString'] = self.form_class.annotationString.replace('\n', '<br />')
		context['hasGroups']=(len(self.getGroups())!=0)
		context['timerDangerousTimeSec']=self.getTimerDangerousTimeSec()
	
		buttonLayout=""
		for i,bt in enumerate(self.buttons):
			buttonLayout+="""<td style="padding: 6px;"><button type="submit" id="button{0}" class="btn btn-default">{1}</button></td>\n""".format(i,bt['name'])
		context['buttonLayout']=buttonLayout
		return context

	def get(self, request, *args, **kwargs):
		if request.user.is_authenticated():
			return super().get(self, request, *args, **kwargs)
		else:
			return (HttpResponse("Пользователь не авторизован "))



	def post(self, request, *args, **kwargs):
		if not request.user.is_authenticated():
			return HttpResponseNotAllowed("")

		if request.is_ajax():
			req = json.loads(request.body.decode('utf-8'))

			if "mode" in req.keys() and "data" in req.keys():
				# mode: "group" "boxrec" "btclick"
				# data: {'selectedGroup': ,"selectedBox": , "pressedButton":})
				mode=req['mode']
				data=req['data']
				
				# responce to client
				resultMessage=None
				resultGroups=None
				resultBoxes=None
				resultRecords=None				

				# print("mode ",mode)
				# print("data ",data)

				if mode=="group":
					resultGroups=self.getGroups()
				elif mode=="boxrec" :
					resultBoxes=[]
					selectedGroup=data.get('selectedGroup',None)
					resultBoxes=self.getBoxesForGroup(selectedGroup)
					
					selectedBox=data.get('selectedBox',None)
					if selectedBox is not None:
						resultRecords={'selectedBox':selectedBox,'records':self.getRecordsForBox(selectedBox)}
						
				elif mode=="btclick" and self.__class__.buttonTaskWorking==False and "pressedButton" in data.keys():
					self.__class__.buttonTaskWorking=True
					name=data['pressedButton']
					selectedBox=data.get('selectedBox',None)
					r=re.match("(button)(\d*)",name)
					if len(r.groups())==2:
						buttonId=int(r.group(2))
						if buttonId in range(len(self.buttons)):
							handler=self.buttons[buttonId]['onclick']
							if handler!=None:
								# start processing button operation
								self.__class__._progressArray=[1]
								try:
									resultMessage=handler(self,buttonId,selectedBox)
								except Exception as e:
									resultMessage=str(e)
								if resultMessage==None:
									resultMessage=""
								
								if selectedBox!=None:
									resultRecords={'selectedBox':selectedBox,'records':self.getRecordsForBox(selectedBox)}

								# end processing button operation
					self.__class__.buttonTaskWorking=False
				#endif mode										

				if self.__class__._progressArray!=None:
					#sometimes progress object can contain additional fields with big amount of data
					#trim data before send to js with only "id","progress","error" fields
					def trimTA(p):
						if type(p)==dict:
							res={k:str(v) for k,v in p.items() if k in ["progress","error","enabled"]}
							res["id"]=p["id"]
							return res
						return p
					self.__class__._progressArray=[trimTA(v) for v in self.__class__._progressArray]
				#end if progress format
				
				# prepare responce to client	
				response={"mode":mode}
				if resultMessage is not None:
					response['message']=resultMessage
				if resultGroups is not None:
					response['groups']=resultGroups
				if resultBoxes is not None:
					response['boxes']=resultBoxes
				if resultRecords is not None:
					response['boxrec']=resultRecords
				if self.__class__._progressArray is not None:
					response['progress']=self.__class__._progressArray
				progressAgo=self.getProgresUpdatedAgoSec()
				if progressAgo!=-1:
					response['progressupdatedagosec']=progressAgo
				# print ("return ",response)
				return(HttpResponse(json.dumps(response)))

			#end if mode & data present
		#end if is ajax

		# print ("return not allowed")
		return HttpResponseNotAllowed("")

	def startJob(self,progressArray,workerFunction,*args,**kwargs):
		self.__class__._progressArray=progressArray
		
		futureDict = {futures.ThreadPoolExecutor(20).submit(workerFunction, data,*args, **kwargs): data
					for data in progressArray}
		workerResult=[]
		for future in futures.as_completed(futureDict):
			if future.exception() is not None:
				print('%r generated an exception: %s' % (futureDict[future],future.exception()))
			else:
				workerResult+=[future.result()]
		# all job done, clear result for successful records 
		# for result in workerResult:
		# 	if "error" not in result.keys():
		# 			_progressArray.remove(result)

		

