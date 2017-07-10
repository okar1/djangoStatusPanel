# -*- coding: utf-8 -*-
from . import qosMq
from . import qosDb
from .models import Servers,Options
import time

# pollResult like 
# [{"id":boxId, "name":BoxName, "error":someErrorText,"pollServer": ,data:boxTasks}]
#boxTasks like: [{"id":recId,"name":recName,"style":"add/rem"}]
def threadPoll():

	def formatErrors(errors,serverName,pollName):
		return [{'id':serverName+'.'+pollName+'.'+str(i), 'name':pollName+": "+text, 'style':'rem'} for i,text in enumerate(errors)]
		

	if not hasattr(threadPoll,"pollResult"):
		threadPoll.pollResult=[]
		threadPoll.oldTasks={}
		threadPoll.pollTimeStamp=int(time.time())
		threadPoll.appStartTimeStamp=int(time.time())
	else:
		return
	print('started Heartbeat thread')
	appStartTimeStamp=threadPoll.appStartTimeStamp

	# print("heartbeat started")
	while True:
		opt=Options.getOptionsObject()
		pollingPeriodSec=opt['pollingPeriodSec']
		pollResult=[]
		pollStartTimeStamp=int(time.time())

		for server in Servers.objects.all():
			serverConfig=server.getConfigObject()
			serverDbConfig=None
			serverErrors=[]
			tasksToPoll=None
			#oldtsks are used to get timestamp if it absent in current taskstopoll
			oldTasks=threadPoll.oldTasks.get(server.name,{})

			#******************************************************
			#******** begin poll modules call *********
			# (tasksToPoll,server,serverConfig,errors)
			
			#poll database
			pollName="Database"
			errors=[]
			
			for dbConf in serverConfig['db']:
				e,tmp=qosDb.getTasks(dbConf)
				if e is None:
					tasksToPoll=tmp
					serverDbConfig=dbConf
				else:
					errors+=[e]
			#endfor
			serverErrors+=formatErrors(errors,server.name,pollName)
			errors=None

			#polling RabbitMQ
			if tasksToPoll is not None:

				pollName="RabbitMQ"
				errors=[]
				
				#now tasksToPoll like {taskKey: {"agentKey":"aaa", "period":10} }} 
				qosMq.pollRabbitMQ(errors=errors, tasks=tasksToPoll, rabbits=serverConfig['mq'], opt=opt, oldTasks=oldTasks)
				#now tasksToPoll like {taskKey: {"agentKey":"aaa", "period":10,"idleTime":timedelta} }} 

				serverErrors+=formatErrors(errors,server.name,pollName)
				errors=None

			#******** end poll modules call *********
			#**********************************************

			#mark some tasks as errors
			if tasksToPoll is not None:
				for taskKey,task in tasksToPoll.items():
					task['taskError']=False
					if "idleTime" not in task.keys():
						task['displayname']="{0} ({1}) : Данные не получены".format(taskKey,task['displayname'])
						if (pollStartTimeStamp-appStartTimeStamp)>3*max(task['period'],pollingPeriodSec):
						# if (pollStartTimeStamp-appStartTimeStamp)>15:
							task['taskError']=True
					else:
						idleTime=task['idleTime'].days*86400 + task['idleTime'].seconds
						task['displayname']="{0} ({1}) : {2} сек назад".format(taskKey,task['displayname'],idleTime)
						if abs(idleTime) > 3*max(task['period'],pollingPeriodSec):
							task['taskError']=True
			# endif mark errors

			#dublicate errors "no task data" to qos server gui
			if tasksToPoll is not None and server.qosguialarm and serverDbConfig:
				pollName="QosGuiAlarm"
				errors=[]

				#get originatorID - service integer number to send alarm
				e,originatorId=qosDb.getOriginatorIdForAlertType(dbConf=serverDbConfig,alertType=opt['qosAlertType'])
				if e is None:
					#send alarm to qos gui
					qosMq.sendQosGuiAlarms(errors=errors,tasksToPoll=tasksToPoll,rabbits=serverConfig['mq'],opt=opt,originatorId=originatorId)
				else:
					errors+=[e]
				
				serverErrors+=formatErrors(errors,server.name,pollName)
				errors=None
				pollName=None
			#endif qosguialarm	

			#create box for every agent (controlblock)
			#set box error if any of its tasks has error
			agents={}
			agentHasErrors=set()
			if tasksToPoll is not None:
				for taskKey,task in tasksToPoll.items():
					agentKey=task['agentKey']		

					if agentKey in agents.keys():
						curTasks=agents[agentKey]
					else:
						curTasks=[]
						agents[agentKey]=curTasks

					taskData={"id":server.name+'.'+taskKey,"name":task['displayname']}
					
					#processing errors for tasks without timestamp and tasks with old timestamp
					if task['taskError']:
						taskData.update({"style":"rem"})
						agentHasErrors.add(agentKey)

					curTasks+=[taskData]
 			#endif create boxes

			threadPoll.oldTasks[server.name]=tasksToPoll
			tasksToPoll=None #not need anymore

			#add server errors to pollresult
			if len(serverErrors)>0:
				pollResult+=[{"id":server.name, "name":server.name, "pollServer":server.name,
						 "error":"Ошибки при опросе сервера "+server.name,"data":serverErrors,
						 }]

			#sort taskdata for every agent
			for taskData in agents.values():
				taskData.sort(key=lambda t: \
					 ("0" if "style" in t.keys() else "1")+t['name'] )

			#add agents with errors to pollresult
			resultWithErrors=[{"id":server.name+'.'+key,"name":key, 
				"error":key+" : Ошибки в одной или нескольких задачах", 
				"data":taskData, "pollServer":server.name}
				for key,taskData in agents.items() if key in agentHasErrors]
			pollResult+=resultWithErrors

			#at last add agents without errors to pollresult
			resultNoErrors=[{"id":server.name+'.'+key,"name":key, 
				"data":taskData, "pollServer":server.name}
				for key,taskData in agents.items() if key not in agentHasErrors]
			pollResult+=resultNoErrors

		#end for server

		#sort pollresults
		pollResult.sort(key=lambda v: \
			("0" if "error" in v.keys() else "1")+v['name'] )

		#calc errors percent in pollresult
		for poll in pollResult:
			if "error" in poll:
				count=len(poll['data'])
				if count>0:
					errCount=sum([record.get("style",None)=="rem" for record in poll['data']])
					# print (errCount)
					poll['progress']=int(100*errCount/count)
					if poll['pollServer']==poll['name']:
						errHead=poll['pollServer']
					else:
						errHead=poll['pollServer']+" : "+poll['name']
					poll['error']="{0} : Обнаружено ошибок: {1} из {2}".format(errHead,errCount,count)

		threadPoll.pollResult=pollResult
		# print ("sleeping for ",pollingPeriodSec)
		threadPoll.pollTimeStamp=int(time.time())
		# time.sleep(5)
		time.sleep(pollingPeriodSec)

	#end while true 
	# print("heartbeat end")
	# print(pollResult)

# module initialization

import threading
import sys

arg=sys.argv
startThread=True
if len(arg)==2 and arg[0]=='manage.py' and arg[1]!='runserver':
	startThread=False

if startThread:
	t = threading.Thread(target=threadPoll)
	t.start()	



	