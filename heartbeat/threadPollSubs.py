# -*- coding: utf-8 -*-
import json
import pika
import time
from datetime import datetime
from . import qosDb
from . import heartbeatAgent
from .threadMqConsumers import MqConsumers
from . import timeDB
import re

timeStampFormat="%Y%m%d%H%M%S"
matchTimeStampFormat="%Y-%m-%dT%H:%M:%S.%fZ"
agentProtocolVersion=2
# messages with this routing key are not actually send to rabbitMQ
# they are processed locally in server context
localRoutingKey="local"

# send message "ServerStarted" to "qos.service" queue
# (no exception handling)
# routingKeys is list [routingKey]
# routingKey "agent" causes re-registration of all agents
# routingKey "agent-someAgentKey" causes re-registration of single agent
def sendRegisterMessage(server,routingKeys):
    
    exchangeName="qos.service"
    queueName="heartbeatService"
    msgHeaders={"__TypeId__":"com.tecomgroup.qos.communication.message.ServerStarted"}
    msgBody={"originName":None,"serverName":""}
    
    serverConfig = server.getConfigObject()
    errors=[]
    mqConf = getMqConf(serverConfig['mq'], server.name, errors)

    # raise exception only if all mq's are down, so message sending is impossible
    if mqConf is None:
        raise Exception("sendRegisterMessage error: " + str(errors))

    connection=pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
    channel = connection.channel()

    channel.exchange_declare(exchange=exchangeName, exchange_type='topic', durable=True)
    channel.queue_declare(queue=queueName, durable=True,arguments={'x-message-ttl':1800000})
    channel.queue_bind(queue=queueName, exchange=exchangeName, routing_key="server.agent.register")

    for key in routingKeys:
        channel.basic_publish(
            exchange=exchangeName,
            routing_key=key,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json',
                content_encoding='UTF-8',
                priority=0,
                expiration="86400000",
                headers=msgHeaders),
            body=json.dumps(msgBody).encode('UTF-8')
        )
    connection.close()


def formatErrors(errors, serverName, pollName,delayedError=False):
    # dellayed error will not be shown as error in current pollresult
    # it appears only if repeated in next pollresult
    return {serverName + '.' + pollName + '.' + str(i) :
                {
                'name': pollName + ": " + text,
                'error': text,
                # 'style': 'ign' if delayedError else 'rem',
                'enabled':not delayedError,
                # this is error message from server, not an actual task
                'servertask':True
                }
                for i, text in enumerate(errors)
            }


# dbConfig -> (dbConnection,tasksToPoll)
# get db connection for later use and query list of tasks from db
def pollDb(dbConfig, serverName, vServerErrors,oldTasks,pollingPeriodSec):
    # poll database
    pollName = "Database"
    errors = set()
    dbConnections = []

    # check all db in dbconf
    for dbConf in dbConfig:
        try:
            if dbConf.get('server','')!='':
                curConnection = qosDb.getDbConnection(dbConf)
            else:
                continue

        except Exception as e:
            errors.add(str(e))
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

        e, tmp = qosDb.getTasks(dbConnection, pollingPeriodSec)
        if e is None:
            tasksToPoll = tmp
        else:
            errors.add(e)
    # endif
    vServerErrors.update(formatErrors(errors, serverName, pollName))
    
    # if connection to qos db is lost - use last received from qos db tasks
    # heartbeat tasks are excluded, because they are loaded from local db, not qos db. 
    if tasksToPoll is None:
        tasksToPoll = {k:v for k,v in oldTasks.items() if v.get("module","")!='heartbeat'}

    return (dbConnection, tasksToPoll)


# test all rabbitMQ configs and return first working to mqconf
# mqConfig=[mqConf] -> mqConf (select first working mqconf)
def getMqConf(mqConfig,serverName,vServerErrors):
    pollName = "CheckRabbitMq"
    errors = set()

    # mqAmqpConnection if first working mq was found and res is corresponding mqConf
    res=None
    for mqConf in mqConfig:
        try:
            con = pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
        except Exception as e:
            errors.add(str(e))
        else:
            if res is None:
                # use first working connection in later tasks
                res=mqConf
            #close amqplink
            if con.is_open:
                con.close()
    # endfor

    vServerErrors.update(formatErrors(errors, serverName, pollName))
    return res


def pollMQ(serverName, mqConsumerId, vServerErrors, vTasksToPoll):
    pollName = "RabbitMQ"
    errors = set()
    delayedErrors = set()

    try:
        mqMessages = MqConsumers.popConsumerMessages(mqConsumerId)
    except Exception as e:
        errors.add(str(e))
        vServerErrors.update(formatErrors(errors, serverName, pollName))
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
                delayedErrors.add("Задача " + taskKey + " не зарегистрирована в БД")
                continue

            if msgType == 'com.tecomgroup.qos.communication.message.ResultMessage':
                taskResults = mData['results']
                for tr in taskResults:
                    # if result has any parameters - store in timeStamp vTasksToPoll
                    if len(tr['parameters'].keys()) > 0:
                        vTasksToPoll[taskKey]['timeStamp'] = datetime.strptime(tr['resultDateTime'], timeStampFormat)

            elif msgType ==  'com.tecomgroup.qos.communication.message.MatchResultMessage':
                taskResults = mData['results']
                for tr in taskResults:
                    # if result has any parameters - current timeStamp in vTasksToPoll.
                    # timestamps in amqp message not satisfied heartbeat task
                    if len(tr['parameters'].keys()) > 0:
                        vTasksToPoll[taskKey]['timeStamp']=datetime.utcnow()

                    # timeStamp=tr.get('parameters',{}).get('match',{}).get('second_event_time',None)
                    # if timeStamp is not None:
                    #     vTasksToPoll[taskKey]['timeStamp'] = datetime.strptime(tr['resultDateTime'], matchTimeStampFormat)

            elif msgType ==  'com.tecomgroup.qos.communication.message.TaskStatus':
                pass
            elif msgType == 'com.tecomgroup.qos.communication.message.TSStructureResultMessage':
                if len(mData['TSStructure']) > 0:
                    vTasksToPoll[taskKey]['timeStamp'] = datetime.strptime(mData['timestamp'], timeStampFormat)
            else:
                errors.add("Неизвестный тип сообщения: " + msgType)
                continue

        except Exception as e:
            errors.add(str(e))
            continue

    # endfor
        
    vServerErrors.update(formatErrors(errors, serverName, pollName))
    vServerErrors.update(formatErrors(delayedErrors, serverName, 'RabbitMqTaskNotInDB',delayedError=True))


# send heartbeat tasks request to rabbitmq exchange
def subSendHeartBeatTasks(mqConf,serverName,tasksToPoll,serverErrors):
    con=pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
    errors=heartbeatAgent.sendHeartBeatTasks(con,tasksToPoll,mqConf['heartbeatAgentRequest'],True)
    serverErrors.update(formatErrors(errors, serverName, "hbSender"))
    con.close()


# receive heartbeat tasks request from rabbitmq queue
def subReceiveHeartBeatTasks(mqConf,serverName,tasksToPoll,serverErrors,oldTasks):
    con=pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
    errors=heartbeatAgent.receiveHeartBeatTasks(con,tasksToPoll,mqConf['heartbeatAgentReply'],True)
    serverErrors.update(formatErrors(errors, serverName, "hbReceiver"))
    con.close()

    # process composite heartbeat tasks decomposing
    # these tasks returns >1 values in dictionary like 
    # {key1: value1, key2: value2 ....}
    tasksToAdd={}
    tasksToRemove=set()
    for taskKey,task in tasksToPoll.items():
        if task.get("module",None)=='heartbeat' and task.get('enabled',False):

            if "value" in task.keys():
                # hb value received and this is composite task
                # decompose composite task to some simple tasks
                value=task['value']

                if type(value)==dict:

                    for resultKey,resultValue in value.items():
                        childTaskKey=taskKey+"."+resultKey
                        childTask=task.copy()
                        # tasks with "parentkey" property set are the children tasks
                        # ex. cpu load of core #1 #2 ... #24 are the children tasks of "cpu load"
                        childTask["parentkey"]=taskKey
                        childTask["value"]=resultValue

                        tasksToAdd.update({childTaskKey:childTask})
                        tasksToRemove.add(taskKey)
            else:
                # hb value not received
                # check if there are early decomposed (children) tasks in oldTasks with 
                # parentkey == current task
                oldChildrenTasks={k:v for k,v in oldTasks.items() 
                                        if v.get("parentkey",None)==taskKey}
                if oldChildrenTasks:
                    # remove this composite task without value
                    tasksToRemove.add(taskKey)
                    
                    newChildrenTasks={k:task.copy() for k in oldChildrenTasks.keys()}
                    # add some parameters from oldChildrenTass to NewChildrenTasks
                    useOldParameters(newChildrenTasks,oldChildrenTasks)
                    # add early decomposed (children) tasks instead
                    tasksToAdd.update(newChildrenTasks)
                    # tasksToAdd.update(oldChildrenTasks)

    # actually do add and remove for composite tasks
    for t2r in tasksToRemove:
        tasksToPoll.pop(t2r)
    tasksToPoll.update(tasksToAdd)
    # print(tasksToPoll)

# move some parameters from previous poll if they are absent in current poll
def useOldParameters(vTasksToPoll, oldTasks):
    for taskKey, task in vTasksToPoll.items():
        if taskKey in oldTasks.keys():
            if 'taskStartTimeStamp' not in task.keys() and ('taskStartTimeStamp' in oldTasks[taskKey].keys()):
                task['taskStartTimeStamp'] = oldTasks[taskKey]['taskStartTimeStamp']
            if 'timeStamp' not in task.keys() and ('timeStamp' in oldTasks[taskKey].keys()):
                task['timeStamp'] = oldTasks[taskKey]['timeStamp']
                
                # if results not received - then prolongate old errors, else no
                # also no prolongate errors for disabled tasks
                if 'error' not in task.keys() and ('error' in oldTasks[taskKey].keys()) and \
                        oldTasks.get('enabled',True):
                    task['error'] = oldTasks[taskKey]['error']

            if 'alarmTimeStamps' not in task.keys() and ('alarmTimeStamps' in oldTasks[taskKey].keys()):
                task['alarmTimeStamps'] = oldTasks[taskKey]['alarmTimeStamps']
            if 'value' not in task.keys() and ('value' in oldTasks[taskKey].keys()):
                task['value'] = oldTasks[taskKey]['value']
            if 'unit' not in task.keys() and ('unit' in oldTasks[taskKey].keys()):
                task['unit'] = oldTasks[taskKey]['unit']
            if 'parentkey' not in task.keys() and ('parentkey' in oldTasks[taskKey].keys()):
                task['parentkey'] = oldTasks[taskKey]['parentkey']


def markTasks(tasksToPoll, oldTasks, pollStartTimeStamp, appStartTimeStamp, pollingPeriodSec,serverDB,serverName,vServerErrors):

    # extended error check and task markup for heartbeat tasks.
    def markHeartBeatTask(taskKey, task):

        # check expected results count==actual count (if specified in settings)
        expResCount=task['config'].get('resultcount',None)
        if expResCount is not None:
            # user-specified key for item, like "cpu", "loadAverage" etc
            itemKey=task['itemKey']
            agentKey=task['agentKey']
            agentName=task['agentName']
            isTaskToCount=lambda t: \
                t.get('itemKey',None)==itemKey and \
                t.get('agentKey',None)==agentKey and \
                t.get('agentName',None)==agentName and \
                t.get('enabled',True) and \
                t.get('value',None) is not None
            factResCount=sum([1 for t in tasksToPoll.values() if isTaskToCount(t) ])
            if expResCount!=factResCount:
                task['error']="в настройках задано результатов: "+str(expResCount)+" фактически получено: "+str(factResCount)
                return
            #end if
        #end if

        # set of alarm keys that are fired in this poll
        alarmsFired=set()
        alarms=task.get('alarms',{})

        # apply alarm formatters to task value
        # alarm formatters structure is the same as result formatters
        # if after applying we got a value (not [None, False, empty dict, empty list, etc ])
        # then add mark to alarmsfired set
        if task['value'] is not None and task.get('alarms',None):
            try:
                value=task['value']
                oldValue=oldTasks.get(taskKey,{}).get('value',None)
                assert type(value) in [int, float,bool,str]

                for aKey,aData in alarms.items():

                    # check value for matching alarms patterns.
                    # if pattern is not specified - apply to all
                    if ('pattern' not in aData.keys()) or \
                            aData['pattern'].search(taskKey) is not None:
                        # remember alarm if we got something true
                        # (True, not zero, not empty string or dict etc)
                        if formatValue(value,aData,oldValue):
                            alarmsFired.update({aKey})

            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                task['error']="обработка оповещений: "+str(e) 

        # timestamps like {alarmKey:timestamp} when alarm was firstly fired
        # used for calc alarm duration
        alarmTimeStamps=task.get('alarmTimeStamps',{})

        # Doing alarmTimeStamps.keys()==alarmsFired:
        # 1. remove timeStamps for alarms that not fired
        alarmTimeStamps={k:v for k,v in alarmTimeStamps.items() if k in alarmsFired}

        # 2. add current timeStamp only if absent in timeStamps else use old
        for aKey in alarmsFired:
            if aKey not in alarmTimeStamps.keys():
                alarmTimeStamps[aKey]=pollStartTimeStamp

        # 3. save alarmTimeStamps for getting it from oldTasks in next poll
        task['alarmTimeStamps']=alarmTimeStamps        

        # error if one of alarms raised out of specified duration (show first raised alarm)
        # duration is specified as polling period count
        for aKey,aTimeStamp in alarmTimeStamps.items():
            aDuration = pollStartTimeStamp - aTimeStamp
            if aKey in alarms.keys():
                if abs(aDuration) >= alarms[aKey].get('duration',0) * pollingPeriodSec:
                    task['error']="оповестить если " + aKey
                    return
    # end sub
    
    # compose error text for task (if applicable)
    for taskKey, task in tasksToPoll.items():
        
        if task.get('enabled',True):
            if task.get('taskStartTimeStamp', None) is None:
                # when data not received - set current timestamp as start point
                # this timestamp will be used for idle time calculation for task
                # this command will be executed only once for very task
                task['taskStartTimeStamp']=datetime.utcnow()
            #endif timestamp
            
            # last data received timestamp
            timeStamp=task.get('timeStamp',False)
            
            if timeStamp:
                # task received data and has no errors
                # so, make extended check for heartbeat tasks (alarms, resultcount etc)
                if task.get('module',None)=='heartbeat' and task.get('error',None) is None:
                    # check that task received a value
                    if (task.get('value',None) is None):
                        task['error']="Значение не вычислено"
                        continue
                    markHeartBeatTask(taskKey,task)
            else:
                # data not received. Use task first run time.
                # also taskStartTimeStamp will not be shown in GUI
                timeStamp=task.get('taskStartTimeStamp',False)
            
            if task.get('error',None) is None:
                # raise alarm if data is absent for a long time
                idleTime = datetime.utcnow() - timeStamp
                idleTime = idleTime.days * 86400 + idleTime.seconds

                if abs(idleTime) > \
                        3 * max(task['period'], pollingPeriodSec):
                    task['error'] = 'задача не присылает данные длительное время'
                    continue
        else:
            # since task is disabled - remove timestamp and value to prevent including it in oldTasks
            # in this case alarm will not fired imidiately when task will became enabled in future
            t=task.pop('timeStamp',None)
            task.pop('taskStartTimeStamp',None)
            task.pop('alarmTimeStamps',None)
            task.pop('value',None)
            task.pop('error',None)

            # got data from disabled qos task
            # (check that in oldTasks it was disabled too for avoid fake alarms)
            if (task.get('module',None) not in ['heartbeat','Match'] ) and \
                    t is not None and \
                    taskKey in oldTasks.keys() and \
                    oldTasks[taskKey].get('enabled',True)==False:
                task['error']="Debug: задача отключена, но присылает данные"
    # endfor task

    # display error style in task caption
    for taskKey, task in tasksToPoll.items():
        if (task.get('error',None) is not None):
            # task has errors
            task['style'] = 'rem'
        elif not task.get('enabled',True):
            # task is disabled
            task['style'] = 'ign'
        else:
            # task is enabled and has no errors
            task.pop('style', None)


# create box for every agent (controlblock)
# agents like {agent:[tasks]}
# taskstopoll -> pollResult
def makePollResult(tasksToPoll, serverName, serverErrors,delayedServerErrors):
    pollResult=[]
    agents = {}
    agentHasErrors = set()
    if tasksToPoll is not None:
        for taskKey, task in tasksToPoll.items():
            # boxName is used for box caption
            boxName = task['agentName']

            if boxName in agents.keys():
                curTasks = agents[boxName]
            else:
                curTasks = []
                agents[boxName] = curTasks

            taskData = {"id": serverName + '.' + taskKey}

            # filter parameters to write from tasksToPoll to pollResult
            taskData.update({key:task[key] 
                for key in 
                    #agentName not included - because it already presents in box name
                    ["style","agentKey","timeStamp","enabled","unit","value","error","itemName"]
                if key in task.keys()})

            alarms=task.get('alarms',None)
            if alarms is not None:
                taskData.update({'alarms':alarms})

            if task.get('style', None) == 'rem':
                agentHasErrors.add(boxName)

            # processing errors for tasks without timestamp and tasks with old timestamp
            # taskStyle = task.get('style', None)
            # if taskStyle is not None:
            #     taskData.update({"style": taskStyle})
            #     if taskStyle == 'rem':
            #         agentHasErrors.add(boxName)

            curTasks += [taskData]

    # remove from delayedservererrors items that not in serverErrors
    delayedServerErrors &= set(serverErrors.keys())

    # add box with server errors to pollresult
    if len(serverErrors) > 0:
        def _justIsolateLocalVars():
            nonlocal delayedServerErrors

            tmp=[]
            hasNotDelayerErrors=False
            for errorId,error in serverErrors.items():
                
                # if task is disabled (delayed)
                if not error.get('enabled',True):
                    # but was added to delayed early
                    if errorId in delayedServerErrors:
                        # mark it enabled to fire server alarm
                        error['enabled']=True
                    # in any case - add to delayed to alarm was fired at next poll
                    delayedServerErrors.add(errorId)
                    
                if error.get('enabled',True):
                    hasNotDelayerErrors=True

                error.update({
                    'id':errorId,
                    'style': 'rem' if error.get('enabled',True) else 'ign',
                    })
                tmp+=[error]
            #end for
            res={
                "id": serverName,
                "name": serverName,
                "pollServer": serverName,
                "servertask":True,
                "data": tmp,
            }
            if hasNotDelayerErrors:
                res.update({"error": "Ошибки при опросе сервера " + serverName})
            return res
        # end sub

        pollResult += [_justIsolateLocalVars()]
    # endif len(serverErrors)

    # add agents with errors to pollresult
    resultWithErrors = [{
        "id": serverName + '.' + key,
        "name": key,
        "error": key + " : Ошибки в одной или нескольких задачах",
        "data": taskData,
        "pollServer": serverName}
        for key, taskData in agents.items()
        if key in agentHasErrors]
    pollResult += resultWithErrors

    # add agents without errors to pollresult
    resultNoErrors = [{"id": serverName + '.' + key,
                       "name": key,
                       "data": taskData,
                       "pollServer": serverName}
                      for key, taskData in agents.items()
                      if key not in agentHasErrors]
    pollResult += resultNoErrors

    # calc errors percent in pollresult ("no error" tasks/"error" tasks condition in percent)
    for poll in pollResult:
        if "error" in poll:
            # count of not-disabled tasks
            count = sum([record.get("style", None) != "ign"
                        for record in poll['data']])
            if count > 0:
                # count of error tasks
                errCount = sum([record.get("style", None) == "rem"
                    for record in poll['data']])
                # print (errCount)
                poll['progress'] = int(100 * errCount / count)
                if poll['pollServer'] == poll['name']:
                    # box with common server errors
                    errHead = poll['pollServer']
                else:
                    # box with some host errors and tasks
                    errHead = poll['pollServer'] + " : " + poll['name']
                poll['error'] = "{0} : Обнаружено ошибок: {1} из {2}".format(
                    errHead, errCount, count)

    return pollResult


def pollResultSort(vPollResult):
    # sort pollresults like servername & boxname, make boxes with errors first
    # also look views.py/makeBoxCaption for box caption rule
    vPollResult.sort(key=lambda v: ("0" if v.get("servertask",False) and "error" in v.keys() else
                                    "1" if "error" in v.keys() else
                                    "2") + v['pollServer']+" "+v['name'])


def qosGuiAlarm(tasksToPoll, oldTasks, serverName, serverDb, mqConf, opt, vServerErrors):
    # dublicate errors "no task data" to qos server gui
    pollName = "QosGuiAlarm"
    errors = set()

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
        errors.add(e)

    vServerErrors.update(formatErrors(errors, serverName, pollName))

# commit pollResult in external time database
# pollResult can can be changed in case of registartion error during commit
def commitPollResult(tasksToPoll, serverName, vServerErrors, timeDbConfig,vPollResult):
    pollName = "commitResult"
    errors=set()
    timeDB.commitPollResult(timeDbConfig,vPollResult,errors)

    if errors:
        # on commit execption - add error message to serverErrors
        vServerErrors.update(formatErrors(errors, serverName, pollName))
        # now vPollResult = [], but object reference is the same
        vPollResult*=0 
        # rebuild pollResult again with new serverErrors
        # use += instead of = to keep object reference
        vPollResult+=makePollResult(tasksToPoll, serverName, vServerErrors)


# if some tasksToPoll name found in alias list - rename such task to hostname, that owns alias
# in this case box of such taskToPoll will be merged into host's box.
# aliases like {server:{host:{aliases set},host2:{aliases set}},server2:...}
 # tasksToPoll like {taskKey: {"agentKey":"aaa", "itemName:" "period":10} }}
def applyHostAliases(allServerAliases,server,vTasks):
    allHostAliases=allServerAliases.get(server.name,{})
    for task in vTasks.values():

        for hostName,aliases in allHostAliases.items():
            # change "native" name in qos task to hostname that has corresponding alias
            if task['agentName'] in aliases:
                task['agentName']=hostName
                break


# qos tasks are marked as disabled in case:
# * task is presents in qos task schedule and now is paused
# * task is placed inside existing heartbeat host, that is disabled
def disableQosTasks(allServerHostEnabled,serverDB,pollingPeriodSec,pollStartTimeStamp,serverName,vTasksToPoll,vServerErrors):

    # request qos db for channel status. Which chnnels are actve now and which are not
    channelScheduledModules={}
    if serverDB:
        try:
            # zapas is seconds count after channel becomes active and before it becomes inactive
            # in "zapas" period channelSchedule still retutns active=false (in really it already=true)
            # zapas is used to prevent fake alarms at schedule intervals borders
            zapas=pollingPeriodSec
            # get channel schedule from qos db like {agent:{1:True,3:True, 5:False}}
            # where agent - agentkey
            # number - integer channel id (tasks with schedule support has taskkey like someAgentKey.CaptionsAnalyzer.869)
            # bool - is task scheduled to run in specified timestamp (true) or scheduled to be paused (false)
            channelScheduledModules=qosDb.getChannelScheduledModules(serverDB, pollStartTimeStamp, zapas)
        except Exception as e:
            vServerErrors.update(formatErrors([str(e)], serverName, "channelSchedule"))

    hostsEnabled=allServerHostEnabled.get(serverName,{})

    for taskKey,taskData in vTasksToPoll.items():
        # check that task is placed inside existing heartbeat host, that is disabled
        if hostsEnabled.get(taskData['agentName'],True)==False or \
                taskData['module'] in ['MediaRecorder', 'MediaStreamer']:
            taskData['enabled']=False
            continue

        # taskIsScheduled=False

        #check that qos task is paused by schedule 
        agentKey=taskData['agentKey']
        agentDict=channelScheduledModules.get(agentKey,False)
        # if agentDict is absent - this host is absent in schedule
        # so, treat all such tasks as continuous
        if agentDict != False:
            # check that taskkey contains int channel number like 2 in  someAgent.LoudnessR128.2
            s=re.search("\.(\w*)\.(\d*)$",taskKey)
            if s:
                moduleName=s.group(1)
                channelNumber=int(s.group(2))
                # channel present in schedule
                # else treat such task as continuous
                if channelNumber in agentDict.keys():
                    # current task's module is present in schedule and NOT allowed to run now - disable task
                    # some module can absent in agentdict if it not described in qosScheduleDeviceToModuleMapping
                    # ex. when new module types are added to qos
                    # such new modules will be treat as continuous and will raise fake errors when scheduled
                    # update qosScheduleDeviceToModuleMapping to avoid this
                    if moduleName not in agentDict[channelNumber]:
                        taskData['enabled']=False
                        continue
                        # dont forget to uncomment early taskIsScheduled when debug )
                        # taskIsScheduled=taskIsScheduled or True

        # debug check that all tasks are scheduled and there is no continuous tasks
        # if not taskIsScheduled:
        #     taskData['error']="задача отсутствует в расписании"
        # else:
        #     continue
        
        # debug disabling for all qos tasks. testing alarms
        # taskData['enabled']=False
    # endfor task


# run heartbeat agent locally in server context to process tasks with localroutingkey
# no amqp is used to sent/receive local tasks
def runAgentLocally(tasksToPoll,serverName,vServerErrors):
    localTasks={k:v for k,v in tasksToPoll.items() if v.get("agentKey",None)==localRoutingKey}
    try:
        heartbeatAgent.processHeartBeatTasks(localTasks)
    except Exception as e:
        vServerErrors.update(formatErrors({str(e)}, serverName, "localAgent"))


# format value or multivalue with formatter settings.
# returns formatted result
def formatValue(v,format,oldV):
    
    # convert v to float. Raise exception if impossible.
    # check +-inf and nan and raises exception
    def toFloat(v):
        res=float(v)
        if res!=res:
            raise Exception("NaN values not supported")
        if res==float("inf") or res==float("-inf"):
            raise Exception("infinitive values not supported")
        return res

    # value is single and format is single
    def _do1value1format(v,format):
        
        if v is None:
            return None

        if type(format) is not dict:
           raise Exception("Проверьте настройки программы")
        
        item=format.get("item",None)

        # convert to numeric value.
        # optional parameter "decimalplaces" is supported (default is 0)
        if item =="number":
            params=heartbeatAgent.checkParameters(format,{
                "decimalplaces":{"type":int,
                                 "mandatory":False}}
            )
            v=toFloat(v)
            decPlaces=params['decimalplaces']
            if decPlaces is not None:
                v=round(v,decPlaces)
        # convert to boolean value.
        # optional lists [truevalues] and [falsevalues] are supported
        # default value is returned if not found in truevalues and falsevales
        elif item=="bool":
            params=heartbeatAgent.checkParameters(format,{
                "truevalues":{"type":list,
                              "mandatory":False},
                "falsevalues":{"type":list,
                               "mandatory":False},              
                "default":{ "type":bool,
                            "mandatory":False,
                            "default":False},              
                }
            )

            default=params['default']
            trueValues=params['truevalues']
            falseValues=params['falsevalues']

            # both truevalues and falsevalues are specified
            if (trueValues is None) and (falseValues is None):
                trueValues=[1,'1',True,"true","True",'t','T',"y","Y"]
                falseValues=[0,'0',False,'false','False','f','F','n',"N"]
                if v in trueValues:
                    v=True
                elif v in falseValues:
                    v=False
                else:
                    v=default
            # only truevalues are specified. Default value will be ignored
            elif trueValues is not None:
                if v in trueValues:
                    v=True
                else:
                    v=False
            # only falsevalues are specified. Default value will be ignored
            else:
                if v in falseValues:
                    v=False
                else:
                    v=True

        # find and replace text in string value (regEx supported)
        # mandatory strings "find" and "replace" must be specified
        elif item=="replace":
            params=heartbeatAgent.checkParameters(format,{
                "find":{"type":str,
                        "mandatory":True},
                "replace":{"type":str,
                           "mandatory":True},
                "ignorecase":{"type":bool,
                           "mandatory":False,
                           "default":False},
                }
            )
            sFind=params['find']
            sReplace=params['replace']
            ignoreCase=params['ignorecase']
            
            if ignoreCase:
                pattern=re.compile(sFind,re.IGNORECASE)
            else:
                pattern=re.compile(sFind)
            if type(v)!=str:
                v=str(v)

            v=pattern.sub(sReplace,v)
        # add number to number value.
        # mandatory number "value" must be specified
        elif item=="add":
            params=heartbeatAgent.checkParameters(format,{
                "value":{"type":[int,float],
                        "mandatory":True},
                }
            )
            v=toFloat(v)+params['value']
        # multiply number to number value.
        # mandatory number "value" must be specified
        elif item=="multiply":
            params=heartbeatAgent.checkParameters(format,{
                "value":{"type":[int,float],
                        "mandatory":True},
                }
            )
            v=toFloat(v)*params['value']
        # exclude tasks if got value from list specified.
        elif item=="exclude":
            params=heartbeatAgent.checkParameters(format,{
                "values":{"type":list,
                        "mandatory":True},
                }
            )
            if v in params['values']:
                v=None
        # true if python-true value received
        elif item=="istrue":
            # params=checkParameters(format,{
            #     "values":{"type":list,
            #             "mandatory":True},
            #     }
            # )
            if v:
                v=True
            else:
                v=False
        elif item=="isfalse":
            # params=checkParameters(format,{
            #     "values":{"type":list,
            #             "mandatory":True},
            #     }
            # )
            if v:
                v=False
            else:
                v=True
        elif item==">" or item==">=" or item=="<" or item=="<=" or item=="=" or item=="!=":
            params=heartbeatAgent.checkParameters(format,{
                "value":{"type":[int,float,str],
                        "mandatory":True},
                }
            )
            v2=params['value']
            
            # "last" keyword specifies last received value 
            if v2=='last':
                v2=oldV

            if item==">":
                v=(v>v2)
            elif item==">=":
                v=(v>=v2)
            elif item=="<":
                v=(v<v2)
            elif item=="<=":
                v=(v<=v2)
            elif item=="=":
                v=(v==v2)
            elif item=="!=":
                v=(v!=v2)
        else:
            raise Exception("поле item не задано либо некорректно. Проверьте настройки программы")
            
        return v
    #end internal function

    if type(format)!=list:
        # format is {format}
        return _do1value1format(v,format)
    else:
        # format is [{format1},{format2},...]
        # let's apply formats turn by turn
        for f in format:
            v=_do1value1format(v,f)
        return v
    # end sub


# apply result formatters to tasks in tasksToPoll that contains a value
def formatTasksValues(tasksToPoll):
    for task in tasksToPoll.values():
        if task.get("module",None)=='heartbeat' and \
                task.get('enabled',True) and \
                task.get('value',None) is not None and \
                task.get("format",None) is not None:
            try:
                # assert that last value is not need when formatting, only when alerting
                task['value']=formatValue(task['value'],task['format'],None)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                task['error']="обработка результата: "+str(e)    

