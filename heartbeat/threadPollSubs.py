# -*- coding: utf-8 -*-
import json
import pika
import time
from datetime import datetime
from . import qosDb
from . import heartbeatAgent
from .threadMqConsumers import MqConsumers

timeStampFormat="%Y%m%d%H%M%S"

def formatErrors(errors, serverName, pollName):
    return [{
        'id': serverName + '.' + pollName + '.' + str(i),
        'name': pollName + ": " + text,
        'style': 'rem'}
        for i, text in enumerate(errors)]


# dbConfig -> (dbConnection,tasksToPoll)
# get db connection for later use and query list of tasks from db
def pollDb(dbConfig, serverName, vServerErrors):
    # poll database
    pollName = "Database"
    errors = []
    dbConnections = []

    # check all db in dbconf
    for dbConf in dbConfig:
        try:
            curConnection = qosDb.getDbConnection(dbConf)
        except Exception as e:
            errors += [str(e)]
            continue
        else:
            dbConnections += [curConnection]

    tasksToPoll = None
    dbConnection = None

    if dbConnections:
        # close all connections but first
        for i in range(1, len(dbConnections)):
            dbConnections[i].close()
        # use first connection in later tasks
        dbConnection = dbConnections[0]

        e, tmp = qosDb.getTasks(dbConnection)
        if e is None:
            tasksToPoll = tmp
        else:
            errors += [e]
    # endif
    vServerErrors += formatErrors(errors, serverName, pollName)
    tasksToPoll = {} if tasksToPoll is None else tasksToPoll
    return (dbConnection, tasksToPoll)


# test all rabbitMQ configs and return first working to mqconf
# mqConfig=[mqConf] -> mqConf (select first working mqconf)
def getMqConf(mqConfig,serverName,maxMsgTotal,vServerErrors):
    pollName = "CheckRabbitMq"
    errors = []

    # mqAmqpConnection if first working mq was found and res is corresponding mqConf
    res=None
    for mqConf in mqConfig:
        try:
            con = pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
        except Exception as e:
            errors += [str(e)]
        else:
            if res is None:
                # use first working connection in later tasks
                res=mqConf
            #close amqplink
            if con.is_open:
                con.close()
    # endfor

    vServerErrors += formatErrors(errors, serverName, pollName)
    return res


def pollMQ(serverName, mqConsumerId, vServerErrors, vTasksToPoll):
    pollName = "RabbitMQ"
    errors = set()

    try:
        mqMessages = MqConsumers.popConsumerMessages(mqConsumerId)
    except Exception as e:
        errors.add(str(e))
        vServerErrors += formatErrors(errors, serverName, pollName)
        return

    # print("**************************",mqConsumerId)
    # print(mqMessages)

    # now we have list of mqMessages
    for msg in mqMessages:
        if type(msg)!=tuple or len(msg)!=3:
            errors.add("RabbitMQ вернул недопустимые данные")
            continue

        # unpack tuple
        mMetaData, mProperties, mData=msg

        # example:

        # --- metadata ---

        # [(<Basic.Deliver(['consumer_tag=ctag1.3f91b60b79f7431a8d4b66a8906eaed3',
        # 'delivery_tag=1',
        # 'exchange=',
        # 'redelivered=False',
        # 'routing_key=test'])>,

        # --- properties ---

        # <BasicProperties(['content_encoding=UTF-8',
        # 'content_type=application/json',
        # 'delivery_mode=2',
        # 'expiration=604800000',
        # "headers={'x-received-from': [{'uri': 'amqp://192.168.116.60/test', 'exchange': 'qos.result', 'redelivered': False}], '__TypeId__': 'com.tecomgroup.qos.communication.message.TSStructureResultMessage', 'version': '1.0'}"])>,

        # --- data ---

        # b'{
        #     "TSStructure": {
        #         "ServiceList": [{
        #             "Bitrate": 2449.9933304775377,
        #             "PESList": [{
        #                 "Bitrate": 2323.102415867024,
        #                 "PESProperties": [.......]
        #                 "Pid": 256,
        #                 "StreamType": "27 (ITU-T Rec. H.264 and ISO/IEC 14496-10 (lower bit-rate video))",
        #                 "Type": "Video"
        #             },
        #             .....
        #             ],
        #             "ServiceID": "1",
        #             "ServiceProperties": [{
        #                 "Name": "Name",
        #                 "Value": "Service01"
        #             },
        #             .....
        #             ]
        #         }],
        #         "TSID": 1,
        #         "TSProperties": [{
        #             "Name": "Bitrate",
        #             "Value": 2522.838114790981
        #         },
        #         .....
        #         ]
        #     },
        #     "originName": "BC_Test_All_Signal",
        #     "taskKey": "BC_Test_All_Signal.TSStructure.71020",
        #     "timestamp": "20170901055905"
        # }'),

        # next message ...
        # ]        

        try:
            mHeaders=mProperties.headers
            if mProperties.content_type != 'application/json':
                errors.add("Неверный тип данных в сообщении RabbitMQ" + mProperties)
                continue
        except Exception as e:
            errors.add(e)
            continue

        msgType = ""
        try:
            msgType = mHeaders['__TypeId__']
        except Exception as e:
            errors.add("Ошибка обработки сообщения: нет информации о типе.")
            continue

        # parse message payload
        try:
            mData = json.loads((mData).decode('utf-8'))
            taskKey = mData['taskKey']

            if taskKey not in vTasksToPoll.keys():
                errors.add("Задача " + taskKey + " не зарегистрирована в БД")
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
                errors.add("Неизвестный тип сообщения: " + msgType)
                continue

        except Exception as e:
            errors.add(str(e))
            continue

    # endfor

    vServerErrors += formatErrors(errors, serverName, pollName)


    # *** old not-async mq method bellow ***

    # # mqAmqpConnection if first working mq was found and res is corresponding mqConf
    # res=None
    # mqAmqpConnection=None
    # for mqConf in mqConfig:
    #     try:
    #         con = pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
    #     except Exception as e:
    #         errors += [str(e)]
    #     else:
    #         mqConnections += [mqConf]
    #         if res is None:
    #             res=mqConf
    #             # use first working connection in later tasks
    #             mqAmqpConnection=con
    #         else:
    #             # close all amqplink but first
    #             if con.is_open:
    #                 con.close()
    # # endfor

    # if (res is None) or (not vTasksToPoll):
    #     return res

    # # poll all rabbitmq instances for one cluster
    # # calculate idletime for each task in tasks
    # # store result in tasks[taskKey]["timestamp"]
    # amqpLink = mqAmqpConnection.channel()
    # mqMessages = [""]

    # try:
    #     amqpLink.queue_declare(queue=res['heartbeatQueue'], durable=True,arguments={'x-message-ttl':1800000})
    # except Exception as e:
    #     errors+= [str(e)]
    #     vServerErrors += formatErrors(errors, serverName, pollName)
    #     if mqAmqpConnection.is_open:
    #         mqAmqpConnection.colse()
    #     return res

    # while len(mqMessages) > 0:

    #     # connect and check errors (amqp)
    #     getOk = None
    #     try:
    #         getOk, *mqMessages = amqpLink.basic_get(res['heartbeatQueue'], no_ack=True)
    #     except Exception as e:
    #         errors += [str(e)]
    #         break

    #     if getOk:
    #         if mqMessages[0].content_type != 'application/json':
    #             errors += ["Неверный тип данных " + mqMessages[0].content_type]
    #             break
    #         mqMessages = [mqMessages]
    #     else:
    #         mqMessages = []

    #     # now we have list of mqMessages
    #     for msg in mqMessages:

    #         msgType = ""
    #         try:
    #             msgType = msg[0].headers['__TypeId__']
    #         except Exception as e:
    #             errStr = "Ошибка обработки сообщения: нет информации о типе."
    #             if errStr not in errors:
    #                 errors += [errStr]
    #             continue

    #         # parse message payload
    #         try:
    #             mData = json.loads((msg[1]).decode('utf-8'))
    #             taskKey = mData['taskKey']
    #             if taskKey not in vTasksToPoll.keys():
    #                 errors += ["Задача " + taskKey + " не зарегистрирована в БД"]
    #                 continue

    #             if msgType == 'com.tecomgroup.qos.communication.message.ResultMessage':

    #                 taskResults = mData['results']
    #                 for tr in taskResults:
    #                     # if result has any parameters - store min task
    #                     # idletime in vTasksToPoll
    #                     if len(tr['parameters'].keys()) > 0:
    #                         vTasksToPoll[taskKey]['timeStamp'] = tr['resultDateTime']

    #             elif msgType == 'com.tecomgroup.qos.communication.message.TSStructureResultMessage':
    #                 if len(mData['TSStructure']) > 0:
    #                     vTasksToPoll[taskKey]['timeStamp'] = mData['timestamp']
    #             else:
    #                 errStr = "Неизвестный тип сообщения: " + msgType
    #                 if errStr not in errors:
    #                     errors += [errStr]

    #         except Exception as e:
    #             errors += [str(e)]
    #             vServerErrors += formatErrors(errors, serverName, pollName)
    #             if mqAmqpConnection.is_open:
    #                 mqAmqpConnection.colse()
    #             return res

    #     # endfor messages in current request
    # # endwhile messages in rabbit queue

    # vServerErrors += formatErrors(errors, serverName, pollName)
    # if mqAmqpConnection.is_open:
    #     mqAmqpConnection.close()
    # return res


# send heartbeat tasks request to rabbitmq exchange
def sendHeartBeatTasks(mqConf,serverName,tasksToPoll,serverErrors):
    con=pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
    errors=heartbeatAgent.sendHeartBeatTasks(con,tasksToPoll,mqConf['heartbeatAgentRequest'],True)
    serverErrors += formatErrors(errors, serverName, "hbSender")
    con.close()


# receive heartbeat tasks request from rabbitmq queue
def receiveHeartBeatTasks(mqConf,serverName,tasksToPoll,serverErrors):
    con=pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
    errors=heartbeatAgent.receiveHeartBeatTasks(con,tasksToPoll,mqConf['heartbeatAgentReply'],True)
    serverErrors += formatErrors(errors, serverName, "hbReceiver")
    con.close()


def useOldParameters(vTasksToPoll, oldTasks):
    # use some parameters from oldTasks if it absent in taskstopoll
    for taskKey, task in vTasksToPoll.items():
        if taskKey in oldTasks.keys():
            if 'timeStamp' not in task.keys() and ('timeStamp' in oldTasks[taskKey].keys()):
                task['timeStamp'] = oldTasks[taskKey]['timeStamp']
            if 'value' not in task.keys() and ('value' in oldTasks[taskKey].keys()):
                task['value'] = oldTasks[taskKey]['value']


def calcIddleTime(vTasksToPoll):
    # calculate iddle time
    for taskKey, task in vTasksToPoll.items():
        if 'timeStamp' in task.keys():
            # "20170610151013"
            resultDateTime = datetime.strptime(
                task['timeStamp'], timeStampFormat)
            utcNowDateTime = datetime.utcnow()
            task["idleTime"] = utcNowDateTime - resultDateTime


def markTasks(tasksToPoll, oldTasks, pollStartTimeStamp, appStartTimeStamp, pollingPeriodSec):
    for taskKey, task in tasksToPoll.items():
        task.pop('style', None)
        
        if not task.get('enabled',True):
            task['displayname'] = "{0} ({1}) : Задача отключена".format(
                taskKey, task['displayname'])
            task['style'] = 'ign'
        else:
            if "idleTime" not in task.keys():
                task['displayname'] = "{0} ({1}) : Данные не получены".format(
                    taskKey, task['displayname'])
                # if task is absent in previous poll - then not mark it as error
                if taskKey in oldTasks.keys():
                    task['style'] = 'rem'
                else:
                    task['style'] = 'ign'
            else:
                idleTime = task['idleTime'].days * 86400 + task['idleTime'].seconds
                task['displayname'] = "{0} ({1}) : {2} сек назад".format(
                    taskKey, task['displayname'], idleTime)
                if abs(idleTime) > \
                        3 * max(task['period'], pollingPeriodSec):
                    task['style'] = 'rem'

                if 'unit' in task.keys() and 'value' in task.keys():
                    unit=task['unit']
                    value=task['value']
                    task['displayname']+=" получено значение "+str(value)+" "+unit


# create box for every agent (controlblock)
# agents like {agent:[tasks]}
# taskstopoll -> pollResult
def makePollResult(tasksToPoll, serverName, serverErrors, vPollResult):
    agents = {}
    agentHasErrors = set()
    if tasksToPoll is not None:
        for taskKey, task in tasksToPoll.items():
            # agentName is used for box caption
            agentName = serverName + " " + task['agentName']
            # truncate agentName>25 symbols
            if len(agentName)>25:
                agentName=agentName[:25]+"..."

            if agentName in agents.keys():
                curTasks = agents[agentName]
            else:
                curTasks = []
                agents[agentName] = curTasks

            taskData = {"id": serverName + '.' +
                        taskKey, "name": task['displayname']}

            # processing errors for tasks without timestamp and tasks with old timestamp
            taskStyle = task.get('style', None)
            if taskStyle is not None:
                taskData.update({"style": taskStyle})
                if taskStyle == 'rem':
                    agentHasErrors.add(agentName)

            curTasks += [taskData]

    # add box with server errors to vPollresult
    if len(serverErrors) > 0:
        vPollResult += [{
            "id": serverName,
            "name": serverName,
            "pollServer": serverName,
            "error": "Ошибки при опросе сервера " + serverName,
            "data": serverErrors,
            }]

    # sort taskdata for every agent
    def sortTasks(t):
        taskStyle = t.get('style', None)
        if taskStyle == 'rem':
            res = '0'
        elif taskStyle == "ign":
            res = '1'
        else:
            res = '2'
        return res+t['name']

    for taskData in agents.values():
        taskData.sort(key=sortTasks)

    # add agents with errors to vPollresult
    resultWithErrors = [{
        "id": serverName + '.' + key,
        "name": key,
        "error": key + " : Ошибки в одной или нескольких задачах",
        "data": taskData,
        "pollServer": serverName}
        for key, taskData in agents.items()
        if key in agentHasErrors]
    vPollResult += resultWithErrors

    # add agents without errors to vPollresult
    resultNoErrors = [{"id": serverName + '.' + key,
                       "name": key,
                       "data": taskData,
                       "pollServer": serverName}
                      for key, taskData in agents.items()
                      if key not in agentHasErrors]
    vPollResult += resultNoErrors


def pollResultSort(vPollResult):
    # sort pollresults, make boxes with errors first
    vPollResult.sort(key=lambda v: ("0" if "error" in v.keys() else "1") + v['name'])


def pollResultCalcProgress(vPollResult):
    # calc errors percent in vPollresult (visual errorcount)
    for poll in vPollResult:
        if "error" in poll:
            count = len(poll['data'])
            if count > 0:
                errCount = sum([record.get("style", None) ==
                                "rem" for record in poll['data']])
                # print (errCount)
                poll['progress'] = int(100 * errCount / count)
                if poll['pollServer'] == poll['name']:
                    errHead = poll['pollServer']
                else:
                    errHead = poll['pollServer'] + " : " + poll['name']
                poll['error'] = "{0} : Обнаружено ошибок: {1} из {2}".format(
                    errHead, errCount, count)


def qosGuiAlarm(tasksToPoll, oldTasks, serverName, serverDb, mqConf, opt, vServerErrors):
    # dublicate errors "no task data" to qos server gui
    pollName = "QosGuiAlarm"
    errors = []

    # add alarmPublishStatus from prev poll if absent
    for taskKey, task in tasksToPoll.items():
        if taskKey in oldTasks.keys():
            if ('alarmPublishStatus' not in tasksToPoll.keys()) and \
                    ('alarmPublishStatus' in oldTasks[taskKey].keys()):
                task['alarmPublishStatus'] = oldTasks[taskKey]['alarmPublishStatus']

    # get originatorID - service integer number to send alarm
    e, originatorId = qosDb.getOriginatorIdForAlertType(
        dbConnection=serverDb, alertType=opt['qosAlertType'])
    if e is None:
        # send alarms to main GUI interface of server
        # originatorID is the service integer number to send alarm
        # agents like
        # {akentkey:[{"id":task_serv_id,"taskKey":taskKey,"name":taskText,"style":"rem"}]}  
        con=pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
        channel = con.channel()
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
        con.close()
        #endif e is none
    else:
        errors += [e]

    vServerErrors += formatErrors(errors, serverName, pollName)
