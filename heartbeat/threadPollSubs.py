# -*- coding: utf-8 -*-
from . import qosDb
from . import qosMq


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


# mqConfig -> (mqAmqpConnection, mqHttpConf)
def pollMQ(mqConfig, serverName, maxMsgTotal, vServerErrors, oldTasks, vTasksToPoll):
    pollName = "RabbitMQ"
    errors = []
    mqConnections = []
    msgTotal = 0

    for mqConf in mqConfig:
        try:
            msgTotal, amqpLink = qosMq.getMqConnection(mqConf)
        except Exception as e:
            errors += [str(e)]
        else:
            mqConnections += [(mqConf, amqpLink)]
    # endfor

    mqAmqpConnection = None
    mqHttpConf = None

    if mqConnections:
        # close all connections but first
        for i in range(1, len(mqConnections)):
            mqConnections[i][1].close()

        # use first connection in later tasks
        mqHttpConf = mqConnections[0][0]
        mqAmqpConnection = mqConnections[0][1]

        # check if too many messages on rabbitMQ
        if msgTotal > maxMsgTotal:
            errors += ["Необработанных сообщений на RabbitMQ : " + str(msgTotal)]

        # add iddleTime to tasksToPoll
        qosMq.pollIddleTime(mqAmqpConnection, errors, vTasksToPoll, oldTasks)
    # endif

    vServerErrors += formatErrors(errors, serverName, pollName)
    return (mqAmqpConnection, mqHttpConf)


def markTasks(tasksToPoll, pollStartTimeStamp, appStartTimeStamp, pollingPeriodSec):
    for taskKey, task in tasksToPoll.items():
        task.pop('style', None)
        if "idleTime" not in task.keys():
            task['displayname'] = "{0} ({1}) : Данные не получены".format(
                taskKey, task['displayname'])
            if (pollStartTimeStamp - appStartTimeStamp) > \
                    3 * max(task['period'], pollingPeriodSec):
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


# create box for every agent (controlblock)
# agents like {agent:[tasks]}
# taskstopoll -> pollResult
def makePollResult(tasksToPoll, serverName, serverErrors, vPollResult):
    agents = {}
    agentHasErrors = set()
    if tasksToPoll is not None:
        for taskKey, task in tasksToPoll.items():
            agentKey = task['agentKey']

            if agentKey in agents.keys():
                curTasks = agents[agentKey]
            else:
                curTasks = []
                agents[agentKey] = curTasks

            taskData = {"id": serverName + '.' +
                        taskKey, "name": task['displayname']}

            # processing errors for tasks without timestamp and tasks with old timestamp
            taskStyle = task.get('style', None)
            if taskStyle is not None:
                taskData.update({"style": taskStyle})
                if taskStyle == 'rem':
                    agentHasErrors.add(agentKey)

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


def qosGuiAlarm(tasksToPoll, serverName, serverDb, mqAmqpConnection, opt, vServerErrors):
    # dublicate errors "no task data" to qos server gui
    pollName = "QosGuiAlarm"
    errors = []

    # get originatorID - service integer number to send alarm
    e, originatorId = qosDb.getOriginatorIdForAlertType(
        dbConnection=serverDb, alertType=opt['qosAlertType'])
    if e is None:
        # send alarm to qos gui
        qosMq.sendQosGuiAlarms(errors=errors,
                               tasksToPoll=tasksToPoll,
                               mqAmqpConnection=mqAmqpConnection,
                               opt=opt,
                               originatorId=originatorId)
    else:
        errors += [e]

    vServerErrors += formatErrors(errors, serverName, pollName)
