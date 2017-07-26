# -*- coding: utf-8 -*-
import requests
import json
import pika
import time
from datetime import datetime
import random

amqpPort = 5672
heartbeatQueue = 'heartbeat'
timeStampFormat="%Y%m%d%H%M%S"
hbOwnExchange='heartbeatOwnTasks'
hbOwnQueue='heartbeatOwnTasks'

# mqconfig --> (msgTotal, mqConnection)
def getMqConnection(mqConf):

    # try to connect via http api
    req = requests.get('http://{0}:{1}/api/overview'.format(
        mqConf["server"], mqConf["port"]), auth=(mqConf["user"], mqConf["pwd"]))

    # get total number of messages on rabbitMQ
    msgTotal = req.json()['queue_totals']['messages']

    # try to connect via amqp
    amqpLink = pika.BlockingConnection(
        pika.ConnectionParameters(
            mqConf["server"],
            amqpPort,
            '/',
            pika.PlainCredentials(mqConf["user"], mqConf["pwd"])))
    return (msgTotal, amqpLink)


# poll all rabbitmq instances for one cluster
# calculate idletime for each task in tasks
# store result in tasks[taskKey]["timestamp"]
def pollTimeStamp(mqAmqpConnection, vErrors, vTasksToPoll):

    amqpLink = mqAmqpConnection.channel()
    mqMessages = [""]
    while len(mqMessages) > 0:

        # connect and check errors (amqp)
        getOk = None
        try:
            getOk, *mqMessages = amqpLink.basic_get(heartbeatQueue, no_ack=True)
        except Exception as e:
            vErrors += [str(e)]
            return

        if getOk:
            if mqMessages[0].content_type != 'application/json':
                vErrors += ["Неверный тип данных " + mqMessages[0].content_type]
                return
            mqMessages = [mqMessages]
        else:
            mqMessages = []

        # now we have list of mqMessages
        for msg in mqMessages:

            msgType = ""
            try:
                msgType = msg[0].headers['__TypeId__']
            except Exception as e:
                errStr = "Ошибка обработки сообщения: нет информации о типе."
                if errStr not in vErrors:
                    vErrors += [errStr]
                continue

            # parse message payload
            try:
                mData = json.loads((msg[1]).decode('utf-8'))
                taskKey = mData['taskKey']
                if taskKey not in vTasksToPoll.keys():
                    vErrors += ["Задача " + taskKey + " не зарегистрирована в БД"]
                    continue

                if msgType == 'com.tecomgroup.qos.communication.message.ResultMessage':

                    taskResults = mData['results']
                    for tr in taskResults:
                        # if result has any parameters - store min task
                        # idletime in vTasksToPoll
                        if len(tr['parameters'].keys()) > 0:
                            vTasksToPoll[taskKey]['timeStamp'] = tr['resultDateTime']

                elif msgType == 'com.tecomgroup.qos.communication.message.TSStructureResultMessage':
                    if len(mData['TSStructure']) > 0:
                        vTasksToPoll[taskKey]['timeStamp'] = mData['timestamp']
                else:
                    errStr = "Неизвестный тип сообщения: " + msgType
                    if errStr not in vErrors:
                        vErrors += [errStr]

            except Exception as e:
                vErrors += [str(e)]
                return
        # endfor messages in current request
    # endwhile messages in rabbit queue


def sendHeartBeatTasks(mqAmqpConnection,tasksToPoll):
    if not mqAmqpConnection:
        return ['Соединение с RabbitMQ не установлено']
    
    errors=[]
    nowDateTime=(datetime.utcnow()).strftime(timeStampFormat)

    channel = mqAmqpConnection.channel()

    try:
        channel.exchange_declare(exchange=hbOwnExchange, exchange_type='topic')
    except Exception as e:
        errors+= [str(e)]
        return errors

    for taskKey, task in tasksToPoll.items():
        
        # process only heartbeat tasks
        if task.get('module',None)!='heartbeat':
            continue

        # process only enabled tasks
        if not task.get('enabled',False):
            continue

        msgRoutingKey=task['agentKey']
        msgBody=(json.dumps(task['config'])).encode('UTF-8')
        
        msgHeaders={'key':taskKey,'type':task['type'],'timestamp':nowDateTime}
        channel.basic_publish(
            exchange=hbOwnExchange,
            routing_key=msgRoutingKey,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json',
                content_encoding='UTF-8',
                priority=0,
                expiration="86400000",
                headers=msgHeaders),
            body=msgBody
        )
    
    return errors


def receiveHeartBeatTasks(mqAmqpConnection,tasksToPoll):
    if not mqAmqpConnection:
        return ['Соединение с RabbitMQ не установлено']
    vErrors=[]

    amqpLink = mqAmqpConnection.channel()
    mqMessages = [""]
    while len(mqMessages) > 0:

        # connect and check errors (amqp)
        getOk = None
        try:
            getOk, *mqMessages = amqpLink.basic_get(hbOwnQueue, no_ack=True)
        except Exception as e:
            vErrors += [str(e)]
            return vErrors

        if getOk:
            if mqMessages[0].content_type != 'application/json':
                vErrors += ["Неверный тип данных " + mqMessages[0].content_type]
                return vErrors
            mqMessages = [mqMessages]
        else:
            mqMessages = []

        # now we have list of mqMessages
        for msg in mqMessages:

            try:
                headers = msg[0].headers
                taskKey=headers['key']
                taskTimeStamp=headers['timestamp']
            except Exception as e:
                errStr = "Ошибка обработки сообщения: неверный заголовок."
                if errStr not in vErrors:
                    vErrors += [errStr]
                continue

            # parse message payload
            try:
                mData = json.loads((msg[1]).decode('utf-8'))
                a=random.random()
                if a>0.3:
                    continue
                mData.update({'value':random.choice(([0,1,2,3,4,5,6,7,8,9]))})
                taskValue=mData['value']
            except Exception as e:
                vErrors += ['Ошибка обработки сообщения: неверное содержимое.']
                return vErrors

            if taskKey not in tasksToPoll.keys():
                vErrors += ['Ошибка обработки сообщения: неверный ключ.']
                return vErrors
            
            tasksToPoll[taskKey]['timeStamp']=taskTimeStamp
            tasksToPoll[taskKey]['value']=taskValue
                        
        # endfor messages in current request
    # endwhile messages in rabbit queue
    return vErrors


# send alarms to main GUI interface of server
# originatorID is the service integer number to send alarm
# agents like
# {akentkey:[{"id":task_serv_id,"taskKey":taskKey,"name":taskText,"style":"rem"}]}
def sendQosGuiAlarms(errors, tasksToPoll, mqAmqpConnection, opt, originatorId):

    channel = mqAmqpConnection.channel()
    for taskKey, task in tasksToPoll.items():
        
        if task.get('module',None)=='heartbeat':
            continue

        action = "ACTIVATE" if task.get('style', None) == 'rem' else "CLEAR"

        # 0 - not published
        # 1 - published activate
        # 2 - published clear
        alarmPublishStatus = tasksToPoll[taskKey].get('alarmPublishStatus', 0)
        needPublish = False

        if action == "ACTIVATE":
            if alarmPublishStatus != 1:
                needPublish = True
                alarmPublishStatus = 1

        if action == "CLEAR":
            if alarmPublishStatus != 2:
                needPublish = True
                alarmPublishStatus = 2

        if needPublish:
            tasksToPoll[taskKey]['alarmPublishStatus'] = alarmPublishStatus
            # print('publish alarm')
            channel.basic_publish(
                exchange='qos.alert',
                routing_key='',
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json',
                    content_encoding='UTF-8',
                    priority=0,
                    expiration="86400000",
                    headers={
                        '__TypeId__': 'com.tecomgroup.qos.communication.message.AlertMessage'}),
                body="""{
                    "originName":null,
                    "action":"{action}",
                    "alert":{
                        "alertType":{
                            "name":"{alerttype}",
                            "probableCause":null,
                            "displayName":null,
                            "displayTemplate":null,
                            "description":null},
                        "perceivedSeverity":"{ceverity}",
                        "specificReason":"NONE",
                        "settings":"",
                        "context":null,
                        "indicationType":null,
                        "dateTime":{time},
                        "source":{"displayName":null,
                            "key":"{taskkey}",
                            "type":"TASK"},
                        "originatorID":{originatorid},
                        "originatorName":"123",
                        "detectionValue":0,
                        "thresholdValue":0
                    }}"""
                .replace("{action}", action)
                .replace("{alerttype}", opt['qosAlertType'])
                .replace("{ceverity}", opt['qosSeverity'])
                .replace("{time}", str(int(time.time()) * 1000))
                .replace("{taskkey}", taskKey)
                .replace("{originatorid}", str(int(originatorId)))
            )
