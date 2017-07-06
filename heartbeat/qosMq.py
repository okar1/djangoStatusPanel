# -*- coding: utf-8 -*-
import requests,json
from datetime import datetime
import pika

# poll all rabbitmq instances for one cluster
# calculate idletime for each task in tasks
# store result in tasks[taskKey]["idleTime"]
def pollRabbitMQ(
	#errors - some additional messages to display for user.
	errors=[],
	tasks={"taskKey":{} } , 
	rabbits=[{
	 	"server":"",
	 	"port":"guest",
	 	"user":"guest",
	 	"pwd":"heartbeat"
 		}],
	opt={
		"maxMsgTotal":50000,
	},
	# if current poll not contain timestamp - then use timestamp from prev. poll
	oldTasks={"taskKey":{} } , 
	):

	messagesPerRequest=50 #for http method only
	heartbeatQueue='heartbeat'
	amqp=1
	http=2

	amqpPort=5672
	# [http] - get msg cont and msg data via http (it is not recommended way to get data)
	# [amqp,http] - get msg count via http and msg data via amqp
	# [amqp] - get msg data via amqp. No msg count available
	mqProto=[amqp,http]

	def updateTaskWithIdleTime(taskData,resultDateTimeString):
		# "20170610151013"
		resultDateTime=datetime.strptime(resultDateTimeString,"%Y%m%d%H%M%S")
		utcNowDateTime=datetime.utcnow()
		idleTime=utcNowDateTime-resultDateTime
		
		if "idleTime" not in taskData.keys():
			taskData["idleTime"]=idleTime
		else:
			if taskData["idleTime"]>idleTime:
				taskData["idleTime"]=idleTime

	#poll availablity of all rabbits and choose which one to poll
	rabbitToPoll=None
	msgTotal=0
	for rab in rabbits:
		try:
			if http in mqProto:
				req = requests.get('http://{0}:{1}/api/overview'.format(rab["server"],rab["port"]), auth=(rab["user"], rab["pwd"]))
				msgTotal=req.json()['queue_totals']['messages']
			if amqp in mqProto:
				amqpLink=pika.BlockingConnection(
					pika.ConnectionParameters(rab["server"],amqpPort,'/',pika.PlainCredentials(rab["user"], rab["pwd"])))

		except Exception as e:
			errors+=[str(e)]
		else:
			rabbitToPoll=rab
	#endfor

	if rabbitToPoll is None:
		return

	if msgTotal>opt["maxMsgTotal"]:
		errors+=["Необработанных сообщений на RabbitMQ : " + str(msgTotal)]

	#poll rabbitMQ heartbeat queue
	if amqp in mqProto:
		amqpLink=pika.BlockingConnection(pika.ConnectionParameters(rabbitToPoll["server"],amqpPort,'/',pika.PlainCredentials(rab["user"], rab["pwd"]))).channel()
	else:
		httpLink="http://{0}:{1}/api/queues/%2f/{2}/get".format(rabbitToPoll["server"],rabbitToPoll["port"],heartbeatQueue)
	
	mqMessages=[""]
	while len(mqMessages)>0:

		if amqp in mqProto:
			#connect and check errors (amqp)
			getOk=None
			try:
				getOk, *mqMessages = amqpLink.basic_get(heartbeatQueue,no_ack=True)
			except Exception as e:
				errors+=[str(e)]
				return

			if getOk:
				if mqMessages[0].content_type!='application/json':
					errors+=[ "Неверный тип данных " + mqMessages[0].content_type]
					return
				mqMessages=[mqMessages]
			else:
				mqMessages=[]

		else:
			#connect and check errors (http)
			try:
				req=requests.post(httpLink, auth=(rabbitToPoll["user"], rabbitToPoll["pwd"]),
					json = {"count":messagesPerRequest,"requeue":False,"encoding":"auto"})
			except Exception as e:
				errors+=[str(e)]
				return

			if req.status_code!=200:
				errors+=[ "Ошибка http " + str(req.status_code)]
				return

			if req.headers['content-type']!='application/json':
				errors+=[ "Неверный тип данных " + req.headers['content-type']]
				return

			mqMessages=req.json()

			if type(mqMessages)!=list:
				errors+=[ "Не могу обработать данные: " + str(type(mqMessages))]
				return


		#now we have list of mqMessages
		for msg in mqMessages:
			
			msgType=""
			try:
				if amqp in mqProto:
					msgType=msg[0].headers['__TypeId__']
				else:
					msgType=msg['properties']['headers']['__TypeId__']

			except Exception as e:
				errStr="Ошибка обработки сообщения: нет информации о типе."
				if errStr not in errors:
					errors+=[errStr]
				continue

			#parse message payload
			try:
				if amqp in mqProto:
					mData=json.loads((msg[1]).decode('utf-8'))
				else:
					mData=json.loads(msg['payload'])

				taskKey=mData['taskKey']
				if taskKey not in tasks.keys():
					errors+=[ "Задача " + taskKey + " не зарегистрирована в БД"]
					continue

				if msgType=='com.tecomgroup.qos.communication.message.ResultMessage':
				
					taskResults=mData['results']
					for tr in taskResults:
						#if result has any parameters - store min task idletime in tasks
						if len(tr['parameters'].keys())>0:
							updateTaskWithIdleTime(tasks[taskKey],tr['resultDateTime'])

				elif msgType=='com.tecomgroup.qos.communication.message.TSStructureResultMessage':
					if len(mData['TSStructure'])>0:
						updateTaskWithIdleTime(tasks[taskKey],mData['timestamp'])
				else:
					errStr="Неизвестный тип сообщения: "+ msgType
					if errStr not in errors:
						errors+=[errStr]									

			except Exception as e:
				errors+=[str(e)]
				return
		#endfor messages in current request
		# break
	#endwhile messages in rabbit queue

	#add timestamp from prev poll if absent
	for taskKey,task in tasks.items():
		if 'idleTime' not in task.keys():
			if (taskKey in oldTasks.keys()) and ('idleTime' in oldTasks[taskKey].keys()):
				task['idleTime']=oldTasks[taskKey]['idleTime']



# send alarms to main GUI interface of server
def sendQosGuiAlarms(errors,tasks,rabbits,opt,originatorId):
	pass
# taskPeriod=task['period']

# 						if (pollStartTimeStamp-appStartTimeStamp)>3*max(taskPeriod,pollingPeriodSec):
# 							taskError=True
# pollingPeriodSec=opt['pollingPeriodSec']	