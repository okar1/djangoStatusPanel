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

import wmi

mqConf={
    "server":'127.0.0.1',
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
            # skip sending error tasks from server
            if task.get('error',False):
                continue
            timeStamp=(datetime.utcnow()).strftime(timeStampFormat)
            msgBody=task['config']
        else:
            msgBody=task.get('value',"")
            timeStamp=task['timeStamp']

        msgHeaders={'key':taskKey,'timestamp':timeStamp,'unit':task['unit']}

        if "error" in task.keys():
            msgHeaders['error']=task['error']

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

    mqMessages = []
    while True:
        # connect and check errors (amqp)
        getOk = None
        try:
            getOk, *mqMessage = channel.basic_get(receiveFromQueue, no_ack=True)
        except Exception as e:
            vErrors += [str(e)]
            return vErrors

        if getOk:
            mqMessage=[getOk]+mqMessage
            if mqMessage[1].content_type != 'application/json':
                vErrors += ["Неверный тип данных " + mqMessage[0].content_type]
                return vErrors
            mqMessages += [mqMessage]
        else:
            break
    # endwhile messages in rabbit queue            

    # now we have list of mqMessages
    for msg in mqMessages:

        try:
            headers = msg[1].headers
            taskKey=headers['key']
            taskTimeStamp=headers['timestamp']
            taskUnit=headers['unit']
        except Exception as e:
            errStr = "Ошибка обработки сообщения: неверный заголовок."
            if errStr not in vErrors:
                vErrors += [errStr]
            continue

        # parse message payload
        try:
            msgBody = json.loads((msg[2]).decode('utf-8'))
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
            if "error" in headers.keys():
                tasksToPoll[taskKey]['error']=headers['error']
        else:
            tasksToPoll[taskKey]={'module':'heartbeat','agentKey':msg[0].routing_key,
                                  'unit':taskUnit,'config':msgBody}
    # endfor messages in current request
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


def taskSnmpTableValue(include, oid=".0.0.0.0", host="127.0.0.1",port=161, readcommunity='public', indexcol=1, datacol=2):
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

    res={}
    assert type(include)==list

    for row in table.values()::
        if len(row)-1<max(indexcol,datacol):
            raise Exception("значение indexcol или datacol в настройках больше, чем количество столбцов в таблице snmp")
        
        key=row[indexcol]

        filterOk=True
        for kw in include:
            if itemKey.find(kw)==-1: ???????
                filterOk=False
                break
        if filterOk:
            res.update({itemKey:itemValue})



        if row[indexcol]==indexcolvalue:
            return row[datacol]
    return "Не удалось найти значение в таблице SNMP"





    if not tableDict:
        raise Exception("Не удалось загрузить данные Open Hardware Monitor. Проверьте работу ПО.")
    if len(res)==0:
        raise Exception("Не удалось найти значение в таблице Open Hardware Monitor")
    elif len(res)==1:
        return res[list(res.keys())[0]]
    else:
        return res



# get wmi table from OpenHardwaqreMonitor like {Identifier:value,...}
# no error handling
def _wmiOhmTable():
    c=wmi.WMI(namespace="OpenHardwareMonitor")
    wql="select * from Sensor"
    q=c.query(wql)
    return {item.Identifier+" "+item.Name:item.Value for item in q}


# identifier of openHardwareMonitor table value like "/intelcpu/0/temperature/0"
def taskOhwTableValue(include):
    res={}
    if not hasattr(taskOhwTableValue,"tableDict"):
        try:
            taskOhwTableValue.tableDict=_wmiOhmTable()
        except Exception as e:
            return str(e)
    tableDict=taskOhwTableValue.tableDict

    assert type(include)==list

    for itemKey,itemValue in tableDict.items():
        filterOk=True
        for kw in include:
            if itemKey.find(kw)==-1:
                filterOk=False
                break
        if filterOk:
            res.update({itemKey:itemValue})

    if not tableDict:
        raise Exception("Не удалось загрузить данные Open Hardware Monitor. Проверьте работу ПО.")
    if len(res)==0:
        raise Exception("Не удалось найти значение в таблице Open Hardware Monitor")
    elif len(res)==1:
        return res[list(res.keys())[0]]
    else:
        return res


# check that keys in config are corresponding filterset template
# removes unneed keys, raise exception if some key is absent
def filterConfigParameters(config,filterSet):
    assert type(config)==dict
    res={k:v for k,v in config.items() if k in filterSet}
    if len(res)!=len(filterSet):
        raise Exception("неправильная конфигурация элемента данных")
    return res


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

        print("got task",taskKey,task)

        if task.get('module',None)!='heartbeat':
            continue

        
        taskConfig=task.get('config',None)
        if taskConfig is None:
            continue

        item=taskConfig.get('item',None)
        if item is None:
            continue

        # task['value']={"key1":"value1","key2":"value2"}
        # task['unit']="кг/ам"

        try:
            if item=="ohwtable":
                filterSet={"include"}
                task['value']=taskOhwTableValue(**filterConfigParameters(taskConfig,filterSet))
            elif item=="snmptable":
                filterSet={"oid","host","port", "readcommunity", "indexcol", "datacol", "include"}
                task['value']=taskSnmpTableValue(**filterConfigParameters(taskConfig,filterSet))
                #task['value']=taskSnmp(**task['config'])
                #task['value']=taskSnmpTableValue(**task['config'])
            else:
                task['error']="Неизвестный тип элемента данных: "+item
        except Exception as e:
            task['error']=str(e)



        if ('value' not in task.keys()) and ('error' not in task.keys()):
            task['error']="Значение не вычислено"
        if 'value' in task.keys():
            print ("result:",task['value'])
        if 'error' in task.keys():
            print("error:",task['error'])


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
