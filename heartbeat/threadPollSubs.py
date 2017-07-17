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
def pollDb(dbConfig, serverName, vServerErrors):
    # poll database
    pollName = "Database"
    errors = []
    dbConnection = None
    tasksToPoll = None

    for dbConf in dbConfig:
        try:
            dbConnection = qosDb.getDbConnection(dbConf)
        except Exception as e:
            errors += [str(e)]
            continue

        e, tmp = qosDb.getTasks(dbConnection)
        if e is None:
            tasksToPoll = tmp
        else:
            errors += [e]
    vServerErrors += formatErrors(errors, serverName, pollName)
    return (dbConnection, tasksToPoll)


# mqConfig -> mqConnection
def pollMQ(mqConfig, serverName, oldTasks, maxMsgTotal, vTasksToPoll, serverErrors):
    pollName = "RabbitMQ"
    errors = []

    qosMq.connectAndPollIddleTime(
        errors=errors,
        tasks=vTasksToPoll,
        rabbits=mqConfig,
        maxMsgTotal=maxMsgTotal,
        oldTasks=oldTasks)

    serverErrors += formatErrors(errors, serverName, pollName)
    return mqConnection

# create box for every agent (controlblock)
# agents like {agent:[tasks]}
# taskstopoll -> pollResult
def makeServerPollResult(tasksToPoll, serverName, serverErrors, vPollResult):
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

            # processing errors for tasks without timestamp and tasks
            # with old timestamp
            if task['taskError']:
                taskData.update({"style": "rem"})
                agentHasErrors.add(agentKey)

            curTasks += [taskData]

    # add server errors to vPollresult
    if len(serverErrors) > 0:
        vPollResult += [{
            "id": serverName,
            "name": serverName,
            "pollServer": serverName,
            "error": "Ошибки при опросе сервера " + serverName,
            "data": serverErrors,
            }]

    # sort taskdata for every agent
    for taskData in agents.values():
        taskData.sort(
            key=lambda t:
            ("0" if "style" in t.keys() else "1") + t['name'])

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

    # agents without errors to vPollresult
    resultNoErrors = [{"id": serverName + '.' + key,
                       "name": key,
                       "data": taskData,
                       "pollServer": serverName}
                      for key, taskData in agents.items()
                      if key not in agentHasErrors]
    vPollResult += resultNoErrors


def pollResultPostProcessing(vPollResult):
    # sort pollresults
    vPollResult.sort(key=lambda v: ("0" if "error" in v.keys() else "1") + v['name'])

    # calc errors percent in vPollresult
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


# mark some tasks as errors
# set task['taskError'] = True
def markErrors(pollStartTimeStamp, appStartTimeStamp, pollingPeriodSec, vTasksToPoll):
    for taskKey, task in vTasksToPoll.items():
        task['taskError'] = False
        if "idleTime" not in task.keys():
            task['displayname'] = "{0} ({1}) : Данные не получены".format(
                taskKey, task['displayname'])
            if (pollStartTimeStamp - appStartTimeStamp) > \
                    3 * max(task['period'], pollingPeriodSec):
                # if (pollStartTimeStamp-appStartTimeStamp)>15:
                task['taskError'] = True
        else:
            idleTime = task['idleTime'].days * \
                86400 + task['idleTime'].seconds
            task['displayname'] = "{0} ({1}) : {2} сек назад".format(
                taskKey, task['displayname'], idleTime)
            if abs(idleTime) > \
                    3 * max(task['period'], pollingPeriodSec):
                task['taskError'] = True


def qosGuiAlarm(tasksToPoll, serverName, serverDb, opt, vServerErrors):
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
                               rabbits=serverConfig['mq'],
                               opt=opt,
                               originatorId=originatorId)
    else:
        errors += [e]

    vServerErrors += formatErrors(errors, serverName, pollName)
