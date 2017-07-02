# -*- coding: utf-8 -*-
from shared.views import BoxForm,BoxFormView
from .threadPoll import threadPoll
from .models import Servers,ServerGroups,Options
import time

class MainViewForm(BoxForm):
	pass
	headString="heartbeat"
	# annotationString="Выберите пользователя из списка, затем щелкните по виджету для отображения списка задач"

# group format [{"id":1,"name":groupName1},{"id":2,"name":groupName2}]

# boxesForGroup format [{"id":boxId,"name":boxName,"error":someErrorText}]
# (if "error" is not present - box is green else red with message)

# recordsForBox format [{"id":recId,"name":recName,"style":"add/rem"}]
# style is optional. Use default style if key is not present 

class MainView(BoxFormView):
	form_class=MainViewForm
	buttons=[]

	def getGroups(self):
		return list(ServerGroups.objects.all().order_by('name').values('id','name'))

	# return base timestamp for timer value calculation
	def getProgresUpdatedAgoSec(self):
		return int(time.time())-threadPoll.pollTimeStamp

	# return time for timer become red
	def getTimerDangerousTimeSec(self):
		return 3*Options.getOptionsObject()['pollingPeriodSec']

	def getBoxesForGroup(self,groupID):
		filterBoxFields=lambda d : {k:v for k,v in d.items() if k in ['id','name','error','progress']}
		
		if groupID is None:
			res=[filterBoxFields(v) for v in threadPoll.pollResult]
		else:
			serverList=list(Servers.objects.filter(servergroups__id=int(groupID)).order_by('name').values_list('name',flat=True))
			res=[filterBoxFields(v) for v in threadPoll.pollResult if v['pollServer'] in serverList]
		self.progressArray=res
		return res

	def getRecordsForBox(self,boxID):
		# print(boxID)
		for item in threadPoll.pollResult:
			if item['id']==boxID:
				return item['data']
		return []