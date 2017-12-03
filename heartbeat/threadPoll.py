# -*- coding: utf-8 -*-
import time
import threading
import sys
import platform
from .models import Servers, Options, TaskSets, Hosts
from . import threadPollSubs as subs
from .threadMqQosResultConsumers import MqQosResultConsumers

isTestEnv=False

if platform.system()=='Windows':
    import pythoncom

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

    if platform.system()=='Windows':
        pythoncom.CoInitialize()    

    print('started Heartbeat thread')
    delayedServerErrors={}
    
    while True:
        opt = Options.getOptionsObject()
        # pollResult like [{"id":boxId, "name":BoxName, "error":someErrorText,
        # "pollServer": ,data:boxTasks}]
        pollResult = []
        pollStartTimeStamp = int(time.time())
        hostAliases=Hosts.getAllAliases()
        hostEnabled=Hosts.getEnabled()
        # delayedServerErrors like {server:[{delayedErrorsForThisServer}]}

        #debug
        if isTestEnv:
            print("------------------------------------")
            print("--------------------------start poll")

        for server in Servers.objects.all():
            serverConfig = server.getConfigObject()
            serverErrors = {}
            tasksToPoll = None
            # connection to database
            serverDb = None
            mqConf=None

            # oldtsks are used to get timestamp if it absent in current taskstopoll
            oldTasks = threadPoll.oldTasks.get(server.name, {})

            # query DB. Get Db connection and list of tasks for monitoring
            # serverConfig -> (serverDb,tasksToPoll)
            # tasksToPoll like {taskKey: {"agentKey":"aaa", "itemName:" "period":10} }}
            serverDb, tasksToPoll = subs.pollDb(serverConfig['db'], server.name, serverErrors,oldTasks, opt['pollingPeriodSec'])

            # test all rabbitMQ configs and return first working to mqconf
            # serverConfig['mq']=[mqConf] -> mqConf (select first working mqconf)
            mqConf = subs.getMqConf(serverConfig['mq'], server.name, serverErrors)
            #opt["maxMsgTotal"]

            #get tasks for heartbeat agent from app DB. All such tasks has "module":"heartbeat"
            hbTasks=TaskSets.getHeartbeatTasks(server,opt['pollingPeriodSec'])

            if mqConf and (tasksToPoll or hbTasks):

                if tasksToPoll:

                    # hb tasks can be enabled or disabled in admin panel
                    # qos tasks are marked as disabled in cases:
                    # * task is presents in qos task schedule and now is paused
                    # * task is placed inside existing heartbeat host, that is disabled
                    subs.disableQosTasks(hostEnabled,serverDb,opt['pollingPeriodSec'],pollStartTimeStamp,server.name,tasksToPoll,serverErrors)

                    # if some tasksToPoll name found in alias list - rename such task to hostname, that owns alias
                    # in this case box of such taskToPoll will be merged into host's box
                    subs.applyHostAliases(hostAliases,server,tasksToPoll)


                    # some heartbeat tasks can have "applyTo" field, wich means this task takes another tasks parameters as argument
                    # ex. task "MediaRecorderControl" is applying to tasks "mediaRecorder"
                    # here we process applyTo field from hb task settings and substitute names with actual task id and parameters
                    subs.fillApplyTo(tasksToPoll,hbTasks,server.name,serverErrors)

                # add heartbeat tasks to taskstopoll.
                tasksToPoll.update(hbTasks)
                hbTasks=None

                mqConsumerId=str(server.id)+" "+server.name

                # opens mq consumer for this server. If it alredy opened - do nothing 
                MqQosResultConsumers.createUpdateConsumers({mqConsumerId:(mqConf['amqpUrl'],mqConf['heartbeatQueue'])})

                # polling RabbitMQ (download messages from consumer), add "idleTime" to tasksToPoll
                subs.pollMQ(server.name, mqConsumerId,serverErrors,tasksToPoll)

                # send heartbeat tasks request to rabbitmq exchange
                subs.subSendHeartBeatTasks(mqConf,server.name,tasksToPoll,serverErrors)

                # messages with local routing key are not actually send to rabbitMQ
                # they are processed locally in server context
                subs.runAgentLocally(tasksToPoll,server.name,serverErrors)
                
                # receive heartbeat tasks request from rabbitmq queue
                subs.subReceiveHeartBeatTasks(mqConf,server.name,tasksToPoll,serverErrors,oldTasks)

                # debug mode for heartbeatAgent: disable "sendHeartBeatTasks" and "receiveHeartBeatTasks" via amqp
                # and call "processHeartBeatTasks" directly
                # from . import heartbeatAgent
                # heartbeatAgent.processHeartBeatTasks(tasksToPoll)
                
                #apply result formatters to tasks in tasksToPoll that contains a value
                subs.formatTasksValues(tasksToPoll)
            
            # mark task status for GUI.
            # use some parameters from oldTasks if it absent in taskstopoll
            subs.useOldParameters(tasksToPoll, oldTasks)

            # add "style" field to tasksToPoll
            # style == "rem" - error presents (red)
            # style == "ign" - error presents, but ignored (gray)
            # style not present - no error
            subs.markTasks(
                tasksToPoll,
                oldTasks,
                pollStartTimeStamp,
                threadPoll.appStartTimeStamp,
                opt['pollingPeriodSec'],
                serverDb,
                server.name,
                serverErrors)

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

            # tasksToPoll,serverErrors -> serverPollResult
            # grouping tasks to boxes by agentKey, also create +1 box for server errors
            serverPollResult=subs.makePollResult(tasksToPoll, server.name, serverErrors,delayedServerErrors.setdefault(server.name,set()))
            
            # commit poll results of current server to timeDB (now it is influxDB)
            subs.commitPollResult(tasksToPoll, server.name, serverErrors, serverConfig['timeDB'], serverPollResult)

            # add this server poll result to global poll result
            pollResult+=serverPollResult

            threadPoll.oldTasks[server.name] = tasksToPoll
            tasksToPoll = None
            serverErrors = None

            if serverDb:
                serverDb.close()
        # end for server
        
        # close and delete MQ consumers wich was not updated in cycle before
        # (ex. db config was changed)
        MqQosResultConsumers.cleanupConsumers()

        # sort boxex, make boxes with errors first. Not affects on tasks inside boxes
        subs.pollResultSort(pollResult)
        
        # poll completed, set pollResult accessible to others
        threadPoll.pollResult = pollResult

        # print(pollResult)

        # print(int(time.time()-threadPoll.pollTimeStamp), 'seconds cycle')
        threadPoll.pollTimeStamp = int(time.time())

        #debug
        if isTestEnv:
            print("--------------------------end poll (",threadPoll.pollTimeStamp-pollStartTimeStamp, " sec)")
            print("--------------------------------------------")
        
        time.sleep(opt['pollingPeriodSec'])

    # end while true


# module initialization

arg = sys.argv
startThread = True
if len(arg) == 2 and arg[0] == 'manage.py':
    # testing environment - migrate mode
    if arg[1] != 'runserver':
        startThread = False
    # testing environment - run mode
    else:
        isTestEnv=True
   

if startThread:
    t = threading.Thread(target=threadPoll)
    t.start()
