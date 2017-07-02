# -*- coding: utf-8 -*-
from shared.views import BoxForm,BoxFormView
import time
import random

class TestBoxForm(BoxForm):
	headString="Обновление виджетов ChannelView"
	annotationString="Выберите пользователя из списка, затем щелкните по виджету для отображения списка задач"

# group format [{"id":1,"name":groupName1},{"id":2,"name":groupName2}]
# boxesForGroup format [{"id":boxId,"name":boxName,"error":someErrorText}]
# (if "error" is not present - box is green else red with message)
# recordsForBox format [{"id":recId,"name":recName,"style":"add/rem"}]
# style is optional. Use default style if key is not present 
	
class TestBoxView(BoxFormView):
	form_class=TestBoxForm

	def worker(self,currentTask,*args,**kwargs):
		print("start ",currentTask)
		workTime=random.random()*10
		step=workTime/20

		simulateFailPercent=0
		simulateFail=random.random()*10<2
		if simulateFail: 
			simulateFailPercent=int(random.random()*20)*5

		for i in range(0,105,5):
			currentTask['progress']=i #if i!=100 else 99
			if simulateFail and i==simulateFailPercent:
			# if i==100:
				currentTask['error']="ERROR: something is wrong"
				break

			time.sleep(step)

		print("end ",currentTask) 
		# input("enter ")
		return currentTask

	groups=[{"id":"g1","name":"set 1"},{"id":"g2","name":"set 2"},{"id":"g3","name":"set 3"},
		{"id":"g12","name":"set 12"},{"id":"g23","name":"set 23"},{"id":"g123","name":"set 123"},
		{"id":"ge","name":"empty"}]

	def getGroups(self):
		return self.groups

	# return base timestamp for timer value calculation
	def getProgresUpdatedAgoSec(self):
		return int(time.time())%120
		# return -1

	# return time for timer become red
	def getTimerDangerousTimeSec(self):
		# return int(time.time())
		return 60


	def getBoxesForGroup(self,groupId):
		if groupId!=None:
			self.__class__.lastSelectedGroup=groupId
		res=[]
		if groupId.find('1')!=-1:
			res=[ {"id":"b1-"+str(i),"name":"set 1 - " + str(i),  } for i in range (1,11) ]
		if groupId.find('2')!=-1:
			res+=[ {"id":"b2-"+str(i),"name":"set 2 - " + str(i), } for i in range (1,16) ] # "error":"preved"
		if groupId.find('3')!=-1:
			res+=[ {"id":"b3-"+str(i),"name":"set 3 - " + str(i), } for i in range (1,21) ]
		# self.__class__.taskArray=res
		return res



	def getRecordsForBox(self,boxID):
		return [ {"id":"-t{0}-{1}".format(boxID,str(i)),
				 "name":"task {0} for box {1}".format(str(i),boxID),
				 "style":random.choice(['add','rem','none'])
				 }
				 	for i in range (1,21)  ]


	def test1(self,buttonId,selectedBox):
		self.startJob([{"id":selectedBox}],self.worker)

	def test2(self,buttonId,selectedBox):
		if hasattr(self.__class__,"lastSelectedGroup") and self.__class__.lastSelectedGroup is not None :
			self.startJob(self.getBoxesForGroup(self.__class__.lastSelectedGroup),self.worker,taskByTask=False)		
			return "Обновление завершено"
		else:
			return "Выберите группу"

	def test3(self,buttonId,selectedBox):
		self.startJob(self.getBoxesForGroup('g123'),self.worker,taskByTask=False)		
		return "Обновление завершено"

	def test4(self,buttonId,selectedBox):
		self.worker({})
		return "Обновление завершено"

	def test5(self,buttonId,selectedBox):
		def randomStatus(box):
			box.pop('error',None)
			if random.random()>0.5:
				box['error']='some random records form group 2 are marked as errors'
			return box

		boxes=self.getBoxesForGroup('g2')
		status=[randomStatus(box) for box in boxes]
		# self.__class__._progressArray=status
		self.progressArray=status



		

	buttons=[{"name":"test selected","onclick":test1},
			{"name":"test this screen","onclick":test2},
			{"name":"test all","onclick":test3},
			{"name":"no interactive mode","onclick":test4},
			{"name":"update random","onclick":test5},
			{"name":"none","onclick":None},
			]

