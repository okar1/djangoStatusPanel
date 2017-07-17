# -*- coding: utf-8 -*-
import time
import threading
import sys
from .models import Servers, Options
from . import threadPollSubs as subs

# pollResult like
# [{"id":boxId, "name":BoxName, "error":someErrorText,
# "pollServer": ,data:boxTasks}]
# boxTasks like: [{"id":recId,"name":recName,"style":"add/rem"}]


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
        pollResult = []
        pollStartTimeStamp = int(time.time())

        for server in Servers.objects.all():
            serverConfig = server.getConfigObject()
            serverErrors = []
            tasksToPoll = None
            # oldtsks are used to get timestamp if it absent in current taskstopoll
            oldTasks = threadPoll.oldTasks.get(server.name, {})
            oldTasks = {} if oldTasks is None else oldTasks

            # query DB. Get Db connection and list of tasks for monitoring
            # serverConfig -> (serverDb,tasksToPoll)
            # tasksToPoll like {taskKey: {"agentKey":"aaa", "period":10} }}
            serverDb, tasksToPoll = subs.pollDb(serverConfig['db'], server.name, serverErrors)

            # polling RabbitMQ. Get MQ connection. Add "idleTime" to tasksToPoll
            # mqHttpConf is one of serverConfig['mq'] for later http connections
            if tasksToPoll is not None:
                print(oldTasks)
                mqAmqpConnection, mqHttpConf = subs.pollMQ(
                                                    serverConfig['mq'],
                                                    server.name,
                                                    opt["maxMsgTotal"],
                                                    serverErrors,
                                                    oldTasks,
                                                    tasksToPoll)

                # set task['taskError'] = True for some conditions
                subs.markErrors(pollStartTimeStamp,
                                threadPoll.appStartTimeStamp,
                                opt['pollingPeriodSec'],
                                tasksToPoll)

                # dublicate task alarms to qos gui
                # TODO: dublicate server alarms from serverErrors too
                if server.qosguialarm and serverDb and mqAmqpConnection:
                    subs.qosGuiAlarm(
                        tasksToPoll,
                        server.name,
                        serverDb,
                        mqAmqpConnection,
                        opt,
                        serverErrors)

            # tasksToPoll -> pollResult
            subs.makeServerPollResult(tasksToPoll, server.name, serverErrors, pollResult)
            threadPoll.oldTasks[server.name] = tasksToPoll
            tasksToPoll = None
        # end for server

        subs.pollResultPostProcessing(pollResult)

        # poll completed, set pollResult accessible to others
        threadPoll.pollResult = pollResult
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
