# # -*- coding: utf-8 -*-
import requests
import pika
import json
from datetime import datetime

mqConf={
    "server":'demo.tecom.nnov.ru',
    "port":"15672",
    "user":"guest",
    "pwd":"guest",
    "vhost":"/"
    }

logFile='heartbeatAgent.log'
receiveFromQueue='heartbeatAgentRequest'
sendToExchange='heartbeatAgentReply'
maxMsgTotal=50000
amqpPort = 5672
timeStampFormat="%Y%m%d%H%M%S"


# mqconfig --> (msgTotal, mqConnection)
def getMqConnection(mqConf,vErrors,maxMsgTotal):

    # try to connect via http api
    req = requests.get('http://{0}:{1}/api/overview'.format(
        mqConf["server"], mqConf["port"]), auth=(mqConf["user"], mqConf["pwd"]))

    # get total number of messages on rabbitMQ
    msgTotal = req.json()['queue_totals']['messages']

    # check if too many messages on rabbitMQ
    if msgTotal > maxMsgTotal:
        vErrors += ["Необработанных сообщений на RabbitMQ : " + str(msgTotal)]

    # try to connect via amqp
    amqpLink = pika.BlockingConnection(
        pika.ConnectionParameters(
            mqConf["server"],
            amqpPort,
            mqConf.get("vhost",'/'),
            pika.PlainCredentials(mqConf["user"], mqConf["pwd"])))
    return amqpLink

def sendHeartBeatTasks(mqAmqpConnection,tasksToPoll,sendToExchange,serverMode=False):
    if not mqAmqpConnection:
        return ['Соединение с RabbitMQ не установлено']
    
    errors=[]

    channel = mqAmqpConnection.channel()

    try:
        channel.exchange_declare(exchange=sendToExchange, exchange_type='topic')
    except Exception as e:
        errors+= [str(e)]
        return errors

    for taskKey, task in tasksToPoll.items():
        
        msgRoutingKey=task['agentKey']        
        if serverMode:
            # process only heartbeat tasks
            if task.get('module',None)!='heartbeat':
                continue
            # process only enabled tasks
            if not task.get('enabled',False):
                continue
            msgBody=task['config']
        else:
            msgBody=task['value']

        msgHeaders={'key':taskKey,'type':task['type'],'timestamp':task['timeStamp']}

        channel.basic_publish(
            exchange=sendToExchange,
            routing_key=msgRoutingKey,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json',
                content_encoding='UTF-8',
                priority=0,
                expiration="86400000",
                headers=msgHeaders),
            body=json.dumps(msgBody).encode('UTF-8')
        )
    return errors


def receiveHeartBeatTasks(mqAmqpConnection,tasksToPoll,receiveFromQueue,serverMode=False):
    if not mqAmqpConnection:
        return ['Соединение с RabbitMQ не установлено']
    vErrors=[]

    channel = mqAmqpConnection.channel()

    try:
        channel.queue_declare(queue=receiveFromQueue, durable=True,arguments={'x-message-ttl':1800000})
    except Exception as e:
        vErrors+= [str(e)]
        return vErrors

    mqMessages = [""]
    while len(mqMessages) > 0:

        # connect and check errors (amqp)
        getOk = None
        try:
            getOk, *mqMessages = channel.basic_get(receiveFromQueue, no_ack=True)
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
                taskType=headers['type']
                taskTimeStamp=headers['timestamp']
            except Exception as e:
                errStr = "Ошибка обработки сообщения: неверный заголовок."
                if errStr not in vErrors:
                    vErrors += [errStr]
                continue

            # parse message payload
            try:
                msgBody = json.loads((msg[1]).decode('utf-8'))
                # msgBody['value']=777
            except Exception as e:
                vErrors += ['Ошибка обработки сообщения: неверное содержимое.']
                return vErrors
            
            if serverMode:            
                if taskKey not in tasksToPoll.keys():
                    vErrors += ['Ошибка обработки сообщения: неверный ключ.']
                    return vErrors
               
                tasksToPoll[taskKey]['value']=msgBody
                tasksToPoll[taskKey]['timeStamp']=taskTimeStamp
            else:
                tasksToPoll[taskKey]={'module':'heartbeat','type':taskType,'agentKey':getOk.routing_key,'config':msgBody}

            

        # endfor messages in current request
    # endwhile messages in rabbit queue
    return vErrors


# taskstoPoll like {Trikolor_NN.Heartbeat.1:{"module":"heartbeat",'type': 'qtype1', 'agentKey': 'Trikolor_NN', 'config': {'header': 'task1'}}}
# keys in every heartbeat task : 
#   module - always == "heartbeat"
#   type - task type from settings
#   config - task config from settings
#   agentKey == key for agent == msg routing key
# As result of work - must set keys for every task:
#   value - string or number with result
#   timeStamp - value timestamp

def processHeartBeatTasks(tasksToPoll):
    for taskKey,task in tasksToPoll.items():
        if task.get('module',None)!='heartbeat':
            continue

        # print("got task",taskKey,task)
        task['value']="preved!!!"
        
        #calc current timestamp after end of collecting results        
        nowDateTime=(datetime.utcnow()).strftime(timeStampFormat)
        task['timeStamp']=nowDateTime


if __name__ == '__main__':
    print("agent start")
    errors=[]
    tasksToPoll={}
    mqAmqpConnection= getMqConnection(mqConf,errors,maxMsgTotal)
    errors+=receiveHeartBeatTasks(mqAmqpConnection,tasksToPoll,receiveFromQueue)
    processHeartBeatTasks(tasksToPoll)
    sendHeartBeatTasks(mqAmqpConnection,tasksToPoll,sendToExchange)
    if errors:
        for e in errors:
            print(e)
    print("agent end")
