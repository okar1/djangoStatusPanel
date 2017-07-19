# -*- coding: utf-8 -*-
import time
import threading
import sys
from .models import Servers, Options
from . import threadPollSubs as subs

# heartbeat main thread
def threadPoll():

    if not hasattr(threadPoll, "pollResult"):
        threadPoll.pollResult = []
        # oldtasks like {serverName:[tasks]}
        threadPoll.oldTasks = {}
        threadPoll.pollTimeStamp = int(time.time())
        threadPoll.appStartTimeStamp = int(time.time())
    else:
        return
    print('started Heartbeat thread')

    while True:
        opt = Options.getOptionsObject()
        # pollResult like [{"id":boxId, "name":BoxName, "error":someErrorText,
        # "pollServer": ,data:boxTasks}]
        pollResult = []
        pollStartTimeStamp = int(time.time())

        for server in Servers.objects.all():
            serverConfig = server.getConfigObject()
            serverErrors = []
            tasksToPoll = None
            # connection to database
            serverDb = None
            # amqp connection to rabbit
            mqAmqpConnection = None
            # credintials for access rabbit http api
            mqHttpConf = None

            # oldtsks are used to get timestamp if it absent in current taskstopoll
            oldTasks = threadPoll.oldTasks.get(server.name, None)
            oldTasks = {} if oldTasks is None else oldTasks

            # query DB. Get Db connection and list of tasks for monitoring
            # serverConfig -> (serverDb,tasksToPoll)
            # tasksToPoll like {taskKey: {"agentKey":"aaa", "displayname:" "period":10} }}
            serverDb, tasksToPoll = subs.pollDb(serverConfig['db'], server.name, serverErrors)

            # polling RabbitMQ. Get MQ connection. 
            # Add "idleTime" to tasksToPoll
            # mqHttpConf is one of serverConfig['mq'] for later http api connections
            mqAmqpConnection, mqHttpConf = subs.pollMQ(
                                                serverConfig['mq'],
                                                server.name,
                                                opt["maxMsgTotal"],
                                                serverErrors,
                                                oldTasks,
                                                tasksToPoll)

            # subs.sendHeartBeatTasks(tasksToPoll)
            # subs.receiveHeartBeatTasks(tasksToPoll)

            # add "style" field to tasksToPoll, modify "displayname"
            # style == "rem" - error presents (red)
            # style == "ign" - error presents, but ignored (gray)
            subs.markTasks(tasksToPoll,pollStartTimeStamp,threadPoll.appStartTimeStamp,opt['pollingPeriodSec'])

            # dublicate task alarms to qos gui
            if server.qosguialarm and serverDb and mqAmqpConnection:
                subs.qosGuiAlarm(
                    tasksToPoll,
                    server.name,
                    serverDb,
                    mqAmqpConnection,
                    opt,
                    serverErrors)

            # tasksToPoll,serverErrors -> pollResult
            # grouping tasks to boxes by agentKey, also create +1 box for server errors
            # then save this server's boxes to pollresult 
            subs.makePollResult(tasksToPoll, server.name, serverErrors, pollResult)

            threadPoll.oldTasks[server.name] = tasksToPoll
            tasksToPoll = None
            serverErrors = None

            if serverDb:
                serverDb.close()
            if mqAmqpConnection:
                mqAmqpConnection.close()
        # end for server

        subs.pollResultSort(pollResult)
        subs.pollResultCalcProgress(pollResult)
        # poll completed, set pollResult accessible to others
        threadPoll.pollResult = pollResult

        # print(int(time.time()-threadPoll.pollTimeStamp), 'seconds cycle')
        threadPoll.pollTimeStamp = int(time.time())

        # time.sleep(5)
        time.sleep(opt['pollingPeriodSec'])

    # end while true


# module initialization

arg = sys.argv
startThread = True
if len(arg) == 2 and arg[0] == 'manage.py' and arg[1] != 'runserver':
    startThread = False

if startThread:
    t = threading.Thread(target=threadPoll)
    t.start()
