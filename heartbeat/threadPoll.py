# -*- coding: utf-8 -*-
from .pollRabbitMQ import pollRabbitMQ
from .dbGetTasks import dbGetTasks
from .models import Servers,Options
import time

# pollResult like 
# [{"id":boxId, "name":BoxName, "error":someErrorText,"pollServer": ,data:boxTasks}]
#boxTasks like: [{"id":recId,"name":recName,"style":"add/rem"}]
def threadPoll():

	def formatErrors(errors,formattedErrors,serverName,pollName,pollResult):
		formattedErrors+=[{'id':serverName+'.'+pollName+'.'+str(i), 'name':pollName+": "+text, 'style':'rem'} for i,text in enumerate(errors)]
		del errors[:]

	if not hasattr(threadPoll,"pollResult"):
		threadPoll.pollResult=[]
		threadPoll.oldTasksToPoll={}
		threadPoll.pollTimeStamp=int(time.time())
		threadPoll.appStartTimeStamp=int(time.time())
	else:
		return
	
	print('started Heartbeat thread')
	opt=Options.getOptionsObject()
	pollingPeriodSec=opt['pollingPeriodSec']
	appStartTimeStamp=threadPoll.appStartTimeStamp

	# print("heartbeat started")
	while True:
		# print ("polling")
		pollResult=[]
		pollStartTimeStamp=int(time.time())

		for server in Servers.objects.all():
			config=server.getConfigObject()
			
			errors=[]
			formattedErrors=[]
			pollName="Database"
			tasksToPoll=None
			for dbConf in config['db']:
				e,tmp=dbGetTasks(dbHost=dbConf['server'],dbPort=dbConf['port'],dbUser=dbConf['user'],dbPassword=dbConf['pwd'])
				if e is None:
					tasksToPoll=tmp
				else:
					errors+=[e]
			#endfor
			formatErrors(errors,formattedErrors,server.name,pollName,pollResult)

			#polling RabbitMQ
			if tasksToPoll is not None:

				pollName="RabbitMQ"
				oldTasksToPoll=threadPoll.oldTasksToPoll.get(server.name,{})

				#now tasks like {taskKey: {"agentKey":"aaa", "period":10} }} 
				pollRabbitMQ(errors=errors, tasks=tasksToPoll, rabbits=config['mq'], opt=opt, oldTasks=oldTasksToPoll)
				#now tasks like {taskKey: {"agentKey":"aaa", "period":10,"idleTime":timedalta} }} 

				threadPoll.oldTasksToPoll[server.name]=tasksToPoll
				formatErrors(errors,formattedErrors,server.name,pollName,pollResult)

			if len(formattedErrors)>0:
				pollResult+=[{"id":server.name, "name":server.name, "pollServer":server.name,
						 "error":"Ошибки при опросе сервера "+server.name,"data":formattedErrors,
						 }]

			#create box for every agent (controlblock)
			#set box error if any of its tasks has error
			if tasksToPoll is not None:
				agents={}
				agentHasErrors=set()
				for taskKey,task in tasksToPoll.items():

					agentKey=task['agentKey']		
					taskPeriod=task['period']
					taskDisplayName=task['displayname']

					if agentKey in agents.keys():
						curTasks=agents[agentKey]
					else:
						curTasks=[]
						agents[agentKey]=curTasks

					taskError=False
					if "idleTime" not in task.keys():
						taskText="{0} ({1}) : Данные не получены". \
							format(taskKey,taskDisplayName)
						if (pollStartTimeStamp-appStartTimeStamp)>3*max(taskPeriod,pollingPeriodSec):
							taskError=True
					else:
						idleTime=task['idleTime'].days*86400 + task['idleTime'].seconds
						taskText="{0} ({1}) : {2} сек назад". \
							format(taskKey,taskDisplayName,idleTime)
						if abs(idleTime) > 3*max(taskPeriod,pollingPeriodSec):
							taskError=True
							
					taskData={"id":server.name+'.'+pollName+'.'+taskKey,"name":taskText}
					if taskError:
						taskData.update({"style":"rem"})
						agentHasErrors.add(agentKey)

					curTasks+=[taskData]
				#endfor
 
				#sort taskdata for every agent
				for taskData in agents.values():
					taskData.sort(key=lambda t: \
						 ("0" if "style" in t.keys() else "1")+t['name'] )


				resultWithErrors=[{"id":server.name+'.'+pollName+'.'+key,"name":key, 
					"error":key+" : Ошибки в одной или нескольких задачах", 
					"data":taskData, "pollServer":server.name}
					for key,taskData in agents.items() if key in agentHasErrors]
				pollResult+=resultWithErrors

				resultNoErrors=[{"id":server.name+'.'+pollName+'.'+key,"name":key, 
					"data":taskData, "pollServer":server.name}
					for key,taskData in agents.items() if key not in agentHasErrors]
				pollResult+=resultNoErrors
			#endif tasks not none
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



	