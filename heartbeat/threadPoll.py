# -*- coding: utf-8 -*-
import time
import threading
import sys
from .models import Servers, Options, TaskSets
from . import threadPollSubs as subs
from .threadMqConsumers import MqConsumers
# import pythoncom
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

    # pythoncom.CoInitialize()    
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
            mqConf=None

            # oldtsks are used to get timestamp if it absent in current taskstopoll
            oldTasks = threadPoll.oldTasks.get(server.name, None)
            oldTasks = {} if oldTasks is None else oldTasks

            # query DB. Get Db connection and list of tasks for monitoring
            # serverConfig -> (serverDb,tasksToPoll)
            # tasksToPoll like {taskKey: {"agentKey":"aaa", "displayname:" "period":10} }}
            serverDb, tasksToPoll = subs.pollDb(serverConfig['db'], server.name, serverErrors)

            # test all rabbitMQ configs and return first working to mqconf
            # serverConfig['mq']=[mqConf] -> mqConf (select first working mqconf)
            mqConf = subs.getMqConf(serverConfig['mq'], server.name, opt["maxMsgTotal"], serverErrors)

            if mqConf:
                # add heartbeat tasks to taskstopoll. All such tasks has "module":"heartbeat"
                tasksToPoll.update(
                    TaskSets.getHeartbeatTasks(server,opt['pollingPeriodSec'])
                )

            if mqConf and tasksToPoll:
                mqConsumerId=str(server.id)+" "+server.name

                # opens mq consumer for this server. If it alredy opened - do nothing 
                MqConsumers.createUpdateConsumers({mqConsumerId:(mqConf['amqpUrl'],mqConf['heartbeatQueue'])})

                # polling RabbitMQ (download messages from consumer), add "idleTime" to tasksToPoll
                subs.pollMQ(server.name, mqConsumerId,serverErrors,tasksToPoll)


                # send heartbeat tasks request to rabbitmq exchange
                subs.sendHeartBeatTasks(mqConf,server.name,tasksToPoll,serverErrors)
                
                # receive heartbeat tasks request from rabbitmq queue
                subs.receiveHeartBeatTasks(mqConf,server.name,tasksToPoll,serverErrors,oldTasks)

                # debug mode for heartbeatAgent: disable "sendHeartBeatTasks" and "receiveHeartBeatTasks" via amqp
                # and call "processHeartBeatTasks" directly
                # from . import heartbeatAgent
                # heartbeatAgent.processHeartBeatTasks(tasksToPoll)

            # use some parameters from oldTasks if it absent in taskstopoll
            subs.useOldParameters(tasksToPoll, oldTasks)            

            # calc timestamp-->iddleTime for tasksToPoll
            subs.calcIddleTime(tasksToPoll)

            # add "style" field to tasksToPoll, modify "displayname"
            # style == "rem" - error presents (red)
            # style == "ign" - error presents, but ignored (gray)
            subs.markTasks(
                tasksToPoll,
                oldTasks,
                pollStartTimeStamp,
                threadPoll.appStartTimeStamp,
                opt['pollingPeriodSec'])

            # dublicate task alarms to qos gui
            if server.qosguialarm and serverDb and mqConf:
                subs.qosGuiAlarm(
                    tasksToPoll,
                    oldTasks,
                    server.name,
                    serverDb,
                    mqConf,
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
        # end for server
        
        # close and delete MQ consumers wich was not updated in cycle before
        # (ex. db config was changed)
        MqConsumers.cleanupConsumers()

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
