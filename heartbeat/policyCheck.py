# -*- coding: utf-8 -*-
import time
from .models import Servers, Options, TaskSets, Hosts
from . import threadPollSubs as subs
from . import qosDb

isTestEnv = False


# dbConfig -> (dbConnection,tasksToPoll)
# get db connection for later use and query list of tasks from db
def pollDb(dbConfig, serverName, vServerErrors):
    # poll database
    pollName = "Database"
    errors = set()
    dbConnections = []

    # check all db in dbconf
    for dbConf in dbConfig:
        try:
            if dbConf.get('server', '') != '':
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

        e, tmp = qosDb.getPolicyStatus(dbConnection)
        if e is None:
            tasksToPoll = tmp
        else:
            errors.add(e)
    # endif
    vServerErrors.update(formatErrors(errors, serverName, pollName))

    dbConfigured = sum([item.get('server', '') != '' for item in dbConfig])

    # if connection to qos db is lost - use last received from qos db tasks
    # heartbeat tasks are excluded, because they are loaded from local db, not
    # qos db.
    if tasksToPoll is None:
        tasksToPoll = {}

    return (dbConnection, tasksToPoll)







# heartbeat main thread
def policyCheck():
    if not hasattr(policyCheck, "pollResult"):
        policyCheck.pollResult = []
        policyCheck.pollTimeStamp = int(time.time())
    
    delayedServerErrors = {}

    opt = Options.getOptionsObject()
    # pollResult like [{"id":boxId, "name":BoxName, "error":someErrorText,
    # "pollServer": ,data:boxTasks}]
    pollResult = []
    pollStartTimeStamp = int(time.time())
    hostAliases = Hosts.getAllAliases()
    hostEnabled = Hosts.getEnabled()
    # delayedServerErrors like {server:[{delayedErrorsForThisServer}]}

    # debug
    if isTestEnv:
        print('started policyCheck sub')

    for server in Servers.objects.all():
        serverConfig = server.getConfigObject()
        serverErrors = {}
        tasksToPoll = None
        # connection to database
        serverDb = None

        # query DB. Get Db connection and list of tasks for monitoring
        # serverConfig -> (serverDb,tasksToPoll)
        # tasksToPoll like {taskKey: {"agentKey":"aaa", "itemName:"
        # "period":10} }}
        serverDb, tasksToPoll = pollDb(
            serverConfig['db'],
            server.name,
            serverErrors,
            oldTasks,
            opt['pollingPeriodSec']
        )

        # get tasks for heartbeat agent from app DB. All such tasks has
        # "module":"heartbeat"
        hbTasks = TaskSets.getHeartbeatTasks(
            server, opt['pollingPeriodSec'])

        if tasksToPoll:

            # hb tasks can be enabled or disabled in admin panel
            # qos tasks are marked as disabled in cases:
            # * task is presents in qos task schedule and now is paused
            # * task is placed inside existing disabled heartbeat host
            subs.disableQosTasks(
                hostEnabled,
                serverDb,
                opt['pollingPeriodSec'],
                pollStartTimeStamp,
                server.name,
                tasksToPoll,
                serverErrors)

            # if some tasksToPoll name found in alias list - rename
            # such task to hostname, that owns alias
            # in this case box of such taskToPoll will be merged into
            # host's box
            subs.applyHostAliases(hostAliases, server, tasksToPoll)

            # some heartbeat tasks can have "applyTo" field, wich means
            # this task takes another tasks parameters as argument
            # ex. task "MediaRecorderControl" is applying to tasks
            # "mediaRecorder"
            # here we process applyTo field from hb task settings and
            # substitute names with actual task id and parameters
            subs.fillApplyTo(tasksToPoll, hbTasks,
                             server.name, serverErrors)

        # add heartbeat tasks to taskstopoll.
        tasksToPoll.update(hbTasks)
        hbTasks = None

        # add "style" field to tasksToPoll
        # style == "rem" - error presents (red)
        # style == "ign" - error presents, but ignored (gray)
        # style not present - no error
        subs.markTasks(
            tasksToPoll,
            oldTasks,
            pollStartTimeStamp,
            policyCheck.appStartTimeStamp,
            opt['pollingPeriodSec'],
            serverDb,
            server.name,
            serverErrors)

        # tasksToPoll,serverErrors -> serverPollResult
        # grouping tasks to boxes by agentKey, also create +1 box for
        # server errors
        serverPollResult = subs.makePollResult(
            tasksToPoll,
            server.name,
            serverErrors,
            delayedServerErrors.setdefault(server.name, set())
        )

        # calc error percent for boxes
        # percent is calculated for tasks with "style":"rem" to all tasks
        subs.pollResultCalcErrorPercent(serverPollResult)

        # add this server poll result to global poll result
        pollResult += serverPollResult

        policyCheck.oldTasks[server.name] = tasksToPoll
        tasksToPoll = None
        serverErrors = None

        if serverDb:
            serverDb.close()
    # end for server

    # sort boxex, make boxes with errors first. Not affects on tasks inside
    # boxes
    subs.pollResultSort(pollResult)

    # poll completed, set pollResult accessible to others
    policyCheck.pollResult = pollResult

    # print(int(time.time()-policyCheck.pollTimeStamp), 'seconds cycle')
    policyCheck.pollTimeStamp = int(time.time())

    # debug
    if isTestEnv:
        print("--------------------------end poll (",
              policyCheck.pollTimeStamp - pollStartTimeStamp, " sec)")
        print("--------------------------------------------")

