# # -*- coding: utf-8 -*-
import requests
import pika
import json
from datetime import datetime
from collections import OrderedDict

from pysnmp import hlapi as snmp
from pysnmp.proto.rfc1905 import NoSuchObject # wrong OID
from pysnmp.proto.rfc1902 import  TimeTicks #TimeTicks
from pysnmp.smi.rfc1902 import  ObjectIdentity # OID
from pysnmp.proto.rfc1902 import Integer
from pysnmp.proto.rfc1902 import  Integer32 # integer
from pysnmp.proto.rfc1902 import  Gauge32 # gauge
from pysnmp.proto.rfc1902 import  Counter32 # Counter32
from pysnmp.proto.rfc1902 import Counter64
from pysnmp.proto.rfc1902 import Unsigned32
from pysnmp.proto.rfc1902 import  IpAddress # IpAddres (некорректно в str)
from pysnmp.proto.rfc1902 import OctetString #OctetString
from pyasn1.type.univ import ObjectIdentifier

# import wmi

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
            timeStamp=(datetime.utcnow()).strftime(timeStampFormat)
            msgBody=task['config']
        else:
            msgBody=task['value']
            timeStamp=task['timeStamp']

        msgHeaders={'key':taskKey,'type':task['type'],'timestamp':timeStamp,'unit':task['unit']}

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
                taskUnit=headers['unit']
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
                tasksToPoll[taskKey]['unit']=taskUnit
            else:
                tasksToPoll[taskKey]={'module':'heartbeat','type':taskType,'agentKey':getOk.routing_key,
                                      'unit':taskUnit,'config':msgBody}

            

        # endfor messages in current request
    # endwhile messages in rabbit queue
    return vErrors


def _snmpFormatValue(v):
    _type=type(v)
    if _type is NoSuchObject:
        res="Указан неверный OID"
    elif sum([issubclass(_type,baseType) for baseType in [TimeTicks,Integer,Integer32,Gauge32,Counter32,Counter64,Unsigned32]]):
        res=int(v)
    elif sum([issubclass(_type,baseType) for baseType in [ObjectIdentity,ObjectIdentifier,IpAddress,OctetString]]):
        res=str(v.prettyPrint())
    else:
        res="Неизвестный тип данных "+str(_type)
    return res


def taskSnmp(oid=".0.0.0.0", host="127.0.0.1",port=161, readcommunity='public'):
    # sample oids of windows snmp service for debugging
    #NoSuchObject
    #oid=".0.0.0.0.0.0.0.0"
    #TimeTicks
    #oid=".1.3.6.1.2.1.1.3.0"
    #OID
    #oid=".1.3.6.1.2.1.1.2.0"
    #integer
    #oid=".1.3.6.1.2.1.1.7.0"
    #gauge
    #oid=".1.3.6.1.2.1.2.2.1.5.1"
    #Counter32
    #oid=".1.3.6.1.2.1.2.2.1.10.1"
    #IpAddres
    #oid=".1.3.6.1.2.1.4.20.1.3.127.0.0.1"
    #OctetString
    #oid=".1.3.6.1.2.1.1.1.0"
    #OctetString with MAC addres
    #oid=".1.3.6.1.2.1.2.2.1.6.6"
    try:
        req = snmp.getCmd(snmp.SnmpEngine(),
                    snmp.CommunityData(readcommunity),
                    snmp.UdpTransportTarget((host, port)),
                    snmp.ContextData(),
                    snmp.ObjectType(snmp.ObjectIdentity(oid)))
        reply=next(req)
    except Exception as e:
        return str(e)

    if reply[0] is None:
        # ok result example(None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.3.0')), TimeTicks(1245987))])
        varBind=reply[3][0]
        
        if oid[0] == ".":
            prefix="."
        else:
            prefix=""
        oidInResult=prefix+str(varBind[0])

        if oidInResult!=oid:
            return ("Указан неверный OID", str(oidInResult),oid)
        else:
            value=varBind[1]
            return _snmpFormatValue(value)
    else:
        # error example (RequestTimedOut('No SNMP response received before timeout',), 0, 0, [])
        res=str(reply[0])
    return res


# get snmp table like OrderedDict {indexRow1:[col1,col2,col3],...}
# no error handling
def _snmpGetTable(mainoid,host,port,readcommunity):
    # sample oid for table
    # oid=".1.3.6.1.2.1.4.20"
    if mainoid[0]=='.':
        mainoid=mainoid[1:]
    res=OrderedDict()

    req = snmp.nextCmd(snmp.SnmpEngine(),
                snmp.CommunityData(readcommunity),
                snmp.UdpTransportTarget((host, port)),
                snmp.ContextData(),
                snmp.ObjectType(snmp.ObjectIdentity(mainoid)))

    while True:
        reply=next(req)

        if reply[0] is None:
            # ok result example(None, 0, 0, [ObjectType(ObjectIdentity(ObjectName('1.3.6.1.2.1.1.3.0')), TimeTicks(1245987))])
            varBind=reply[3][0]
            oid=str(varBind[0])

            # oid like 1.3.6.1.2.1.4.20.1.2.127.0.0.1
            # where 1.3.6.1.2.1.4.20 - main (table) oid
            # 1 - entry (hardcode to always=1)
            # 2 - column index (starts from 1)
            # 127.0.0.1 - index row value
            entryIndex=1
            prefix=mainoid+'.'+str(entryIndex)+"."

            if not oid.startswith(prefix):
                break

            oid=oid[len(prefix):]
            # now oid like 2.127.0.0.1

            # remove entry from oid
            indexValue='.'.join(oid.split('.')[1:])
            value=_snmpFormatValue(varBind[1])

            if indexValue not in res.keys():
                res[indexValue]=[]
            res[indexValue]+=[value] 

        else:
            # error example (RequestTimedOut('No SNMP response received before timeout',), 0, 0, [])
            raise Exception(str(reply[0]))
    return res


def taskSnmpTableValue(oid=".0.0.0.0", host="127.0.0.1",port=161, readcommunity='public', indexcol=1, indexcolvalue="", datacol=2):
    if not hasattr(taskSnmpTableValue,"tableDict"):
        taskSnmpTableValue.tableDict={}
    tableDict=taskSnmpTableValue.tableDict

    tableId=oid+"&"+host+"&"+str(port)+"&"+readcommunity
    if tableId not in tableDict.keys():
        try:
            table=_snmpGetTable(oid,host,port,readcommunity)
        except Exception as e:
            return str(e)
        tableDict[tableId]=table
    else:
        table=tableDict[tableId]


    for row in table.values():
        if len(row)-1<max(indexcol,datacol):
            return "В настройках задан неверный номер столбца"
        if row[indexcol]==indexcolvalue:
            return row[datacol]
    return "Не удалось найти значение в таблице SNMP"


# get wmi table from OpenHardwaqreMonitor like {Identifier:value,...}
# no error handling
def _wmiOhmTable():
    return
    # c=wmi.WMI(namespace="OpenHardwareMonitor")
    # wql="select * from Sensor"
    # q=c.query(wql)
    # return {item.Identifier:item.Value for item in q}


# identifier of openHardwareMonitor table value like "/intelcpu/0/temperature/0"
def taskOhmTableValue(identifier):
    if not hasattr(taskOhmTableValue,"tableDict"):
        try:
            taskOhmTableValue.tableDict=_wmiOhmTable()
        except Exception as e:
            return str(e)
    tableDict=taskOhmTableValue.tableDict

    return tableDict.get(identifier,"Не удалось найти значение в таблице Open Hardware Monitor")


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
        # task['value']="preved!!!"
        # task['unit']="кг/ам"

        if task['type'] == 'snmp':
            task['value']=taskSnmp(**task['config'])
        if task['type'] == 'snmpTableValue':
            task['value']=taskSnmpTableValue(**task['config'])
        if task['type'] == 'ohmTableValue':
            task['value']=taskOhmTableValue(**task['config'])
        
        #calc current timestamp after end of collecting results        
        nowDateTime=(datetime.utcnow()).strftime(timeStampFormat)
        task['timeStamp']=nowDateTime


if __name__ == '__main__':
    # print(taskOhmTableValue("/intelcpu/0/temperature/2"))
    # print(taskOhmTableValue("/intelcpu/0/temperature/0"))
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
