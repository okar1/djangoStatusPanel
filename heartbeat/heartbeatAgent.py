# # -*- coding: utf-8 -*-
# import requests
import pika
import json
from datetime import datetime

# pyasn1 help http://www.red-bean.com/doc/python-pyasn1/pyasn1-tutorial.html#1.1.7
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
import re

# import binascii

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
    # req = requests.get('http://{0}:{1}/api/overview'.format(
        # mqConf["server"], mqConf["port"]), auth=(mqConf["user"], mqConf["pwd"]))

    # get total number of messages on rabbitMQ
    # msgTotal = req.json()['queue_totals']['messages']
    # totals and messages can absent

    # check if too many messages on rabbitMQ
    # if msgTotal > maxMsgTotal:
        # vErrors += ["Необработанных сообщений на RabbitMQ : " + str(msgTotal)]

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
        channel.exchange_declare(exchange=sendToExchange, exchange_type='topic', durable=True)
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
            msgBody={"config":task['config'],
                     "format":task.get('format',None)
                    }
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

    try:
        channel.exchange_declare(exchange=receiveFromQueue, exchange_type='topic', durable=True)
    except Exception as e:
        vErrors+= [str(e)]
        return vErrors

    try:
        channel.queue_bind(queue=receiveFromQueue, exchange=receiveFromQueue, routing_key="#")
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
                                  'unit':taskUnit,'config':msgBody['config'],
                                  'format':msgBody['format']}
    # endfor messages in current request
    return vErrors


def _snmpFormatValue(v):
    _type=type(v)
    if _type is NoSuchObject:
        res="Указан неверный OID"
    elif sum([issubclass(_type,baseType) for baseType in [TimeTicks,Integer,Integer32,Gauge32,Counter32,Counter64,Unsigned32]]):
        res=int(v)
    elif sum([issubclass(_type,baseType) for baseType in [ObjectIdentity,ObjectIdentifier,IpAddress]]):
        res=str(v.prettyPrint())
    elif sum([issubclass(_type,baseType) for baseType in [OctetString]]):
        pp=v.prettyPrint()
        s=str(v)
        # sometimes prettyPrint returns hex values instead of string because of trailing 00 byte
        # if we got not a string - try to remove trailing 00
        # if we got a string after it - use this value
        if pp!=s:
            if len(v)>0:
                if v[len(v)-1]==0:
                    v2=v[:len(v)-1]
                    pp2=v2.prettyPrint()
                    s2=str(v2)
                    if pp2==s2:
                        v=v2
        res=v.prettyPrint()
    else:
        res="Неизвестный тип данных "+str(_type)
    
    # if issubclass(_type,OctetString):
    #     print("v=",v)
    #     # print("res=",res)
    #     print("pp=",v.prettyPrint())
    #     print("bytes=",v.asOctets())
    #     print("bytes-decode=",v.asOctets().decode('iso-8859-1'))
    #     print("str=",str(v))
    #     print("hex=",binascii.hexlify(v.asOctets()).decode("utf8"))
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


# get full snmp table like dict {indexRow1:[col1,col2,col3],...}
# where indexRow is row's snmp oid end digit (differ for every row)
# no error handling
def _snmpGetTable(mainoid,host,port,readcommunity):
    # sample oid for table
    # oid=".1.3.6.1.2.1.4.20"
    if mainoid[0]=='.':
        mainoid=mainoid[1:]
    res={}

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
    # print(res)
    return res


# table like {key:value}
# includeList like ["regex pattern string"]
# result like {key:value} if key match at least 1 pattern
def filterTable(table,include):
    assert type(include)==list
    res={}
    for key,value in table.items():
        for pattern in include:
            if re.search(pattern,key) is not None:
                res.update({key:value})
                break
    return res


def taskSnmpTableValue(include, oid=".0.0.0.0", host="127.0.0.1",port=161, readcommunity='public', indexcol=1, datacol=2):
    if not hasattr(taskSnmpTableValue,"tableDict"):
        taskSnmpTableValue.tableDict={}
    tableDict=taskSnmpTableValue.tableDict

    tableId=oid+"&"+host+"&"+str(port)+"&"+readcommunity
    if tableId not in tableDict.keys():
        tableList=list(_snmpGetTable(oid,host,port,readcommunity).values())
        if tableList:
            if len(tableList[0])-1<max(indexcol,datacol):
                print(tableList)
                raise Exception("значение indexcol или datacol в настройках больше, чем количество столбцов в таблице snmp")
            table={row[indexcol]:row[datacol] for row in tableList}
        else:
            table={}
        tableDict[tableId]=table
    else:
        table=tableDict[tableId]

    if table:
        res=filterTable(table,include)
    else:
        raise Exception("Не удалось загрузить таблицу SNMP. Проверьте настройки ПО и доступность оборудования.")
    
    if len(res)==0:
        raise Exception("Не удалось найти значение в таблице SNMP")
    # elif len(res)==1:
    #     return res[list(res.keys())[0]]
    else:
        return res


# get wmi table from OpenHardwaqreMonitor like {Identifier:value,...}
# no error handling
def _wmiOhmTable():
    c=wmi.WMI(namespace="OpenHardwareMonitor")
    wql="select * from Sensor"
    q=c.query(wql)
    return {item.Identifier:item.Value for item in q} # +item.Name


# identifier of openHardwareMonitor table value like "/intelcpu/0/temperature/0"
def taskOhwTableValue(include):
    res={}
    if not hasattr(taskOhwTableValue,"tableDict"):
        taskOhwTableValue.table=_wmiOhmTable()
    table=taskOhwTableValue.table

    if table:
        res=filterTable(table,include)
    else:
        raise Exception("Не удалось загрузить данные Open Hardware Monitor. Проверьте работу ПО.")

    if len(res)==0:
        raise Exception("Не удалось найти значение в таблице Open Hardware Monitor")
    # elif len(res)==1:
    #     return res[list(res.keys())[0]]
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


# convert v to float. Raise exception if impossible.
# check +-inf and nan and raises exception
def toFloat(v):
    res=float(v)
    if res!=res:
        raise Exception("NaN values not supported")
    if res==float("inf") or res==float("-inf"):
        raise Exception("infinitive values not supported")
    return res


# format value or multivalue with formatter settings.
# returns formatted result
def formatValue(v,format):
    
    # check that format contains mandatory parameters and that parameter types are correct
    # vFormat like {"item": "formatName", "param1":param1value, "param2":"param2value"}
    # pattern like {"param1":{"type":dict,"mandatory":true}, "param2": ...}
    # if mandatory parameter absent - raise exception
    # if optional parameter absent - None value is added to vFormat
    def _checkFormatParameters(vFormat,pattern):
        for paramKey,paramOpts in pattern.items():
            isMandatory=paramOpts.get('mandatory',True)
            if isMandatory and (paramKey not in vFormat.keys()):
                raise Exception("не указан параметр "+paramKey+" Проверьте настройку обработки результата")
            else:
                # (mandatory and present) or (not mandatory)
                paramValue=vFormat.get(paramKey,None)
                if paramValue is not None:
                    paramType=paramOpts['type']
                    # paramtype is list like [int, str, float, ...]
                    # and type of value is absent in this list
                    # \ or
                    # paramtype is type (like float)
                    # and type of value not equal it
                    if (type(paramType)==list and (type(paramValue) not in paramType)) or \
                       (type(paramType)!=list and (type(paramValue)!=paramType)):
                        raise Exception("тип параметра "+paramKey+" задан неверно. Проверьте настройку обработки результата")                        
                else:
                    vFormat[paramKey]=None #paramValue


    # value is single and format is {format}
    def _do1value1format(v,format):
        
        if type(format) is not dict:
           raise Exception("проверьте настройку обработки результата")
        
        item=format.get("item",None)
        # convert to numeric value.
        # optional parameter "decimalplaces" is supported (default is 0)
        if item =="number":
            _checkFormatParameters(format,{
                "decimalplaces":{"type":int,
                                 "mandatory":False}}
            )
            v=toFloat(v)
            decPlaces=format['decimalplaces']
            if decPlaces is not None:
                v=round(v,decPlaces)
        # convert to boolean value.
        # optional lists [truevalues] and [falsevalues] are supported
        # default value is returned if not found in truevalues and falsevales
        elif item=="bool":
            _checkFormatParameters(format,{
                "truevalues":{"type":list,
                              "mandatory":False},
                "falsevalues":{"type":list,
                               "mandatory":False},              
                "default":{"type":bool,
                               "mandatory":False},              
                }
            )
            if format['default'] is None:
                default=False
            else:
                default=format['default']
            
            trueValues=format['truevalues']
            falseValues=format['falsevalues']

            # both truevalues and falsevalues are specified
            if (trueValues is None) and (falseValues is None):
                trueValues=[1,'1',True,"true","True",'t','T',"y","Y"]
                falseValues=[0,'0',False,'false','False','f','F','n',"N"]
                if v in trueValues:
                    v=True
                elif v in falseValues:
                    v=False
                else:
                    v=default
            # only truevalues are specified. Default value will be ignored
            elif trueValues is not None:
                if v in trueValues:
                    v=True
                else:
                    v=False
            # only falsevalues are specified. Default value will be ignored
            else:
                if v in falseValues:
                    v=False
                else:
                    v=True

        # find and replace text in string value (regEx supported)
        # mandatory strings "find" and "replace" must be specified
        elif item=="replace":
            _checkFormatParameters(format,{
                "find":{"type":str,
                        "mandatory":True},
                "replace":{"type":str,
                           "mandatory":True},
                "ignorecase":{"type":bool,
                           "mandatory":False},
                }
            )
            sFind=format['find']
            sReplace=format['replace']
            ignoreCase=format['ignorecase']
            if ignoreCase is None:
                ignoreCase=False
            
            if ignoreCase:
                pattern=re.compile(sFind,re.IGNORECASE)
            else:
                pattern=re.compile(sFind)
            if type(v)!=str:
                v=str(v)

            v=pattern.sub(sReplace,v)
        # add number to number value.
        # mandatory number "value" must be specified
        elif item=="add":
            _checkFormatParameters(format,{
                "value":{"type":[int,float],
                        "mandatory":True},
                }
            )
            v=toFloat(v)+format['value']
        # multiply number to number value.
        # mandatory number "value" must be specified
        elif item=="multiply":
            _checkFormatParameters(format,{
                "value":{"type":[int,float],
                        "mandatory":True},
                }
            )
            v=toFloat(v)*format['value']
        else:
            raise Exception("поле item не задано либо некорректно. Проверьте настройку обработки результата")
            
        return v

    # value is single
    def _do1value(v,format):
        
        if type(format)!=list:
            # format is {format}
            return _do1value1format(v,format)
        else:
            # format is [{format1},{format2},...]
            # let's apply formats turn by turn
            for f in format:
                v=_do1value1format(v,f)
            return v

    if type(v)==dict:
        # for multitask value is dict.
        # in this case let's format every value in dict
        return {key:_do1value(value,format) for key,value in v.items()}
    else:
        return _do1value(v,format)
    return v

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

        print("*********")
        print("got task:",taskKey,task)

        if task.get('module',None)!='heartbeat':
            task['error']="Указано неверное имя модуля"
        else:
            taskConfig=task.get('config',None)
            if taskConfig is None or type(taskConfig)!=dict:
                task['error']="Не заданы настройки элемента данных"
            else:
                item=taskConfig.get('item',None)
                if item is None:
                    task['error']="В настройках элемента данных не задано значение item"
                else:
                    # task['value']={"key1":"value1","key2":"value2"}
                    # task['unit']="кг/ам"

                    try:
                        if item=="ohwtable" or item=="snmptable":
                            if taskConfig.get("include",None) is None:
                                taskConfig['include']=[".*"]

                        if item=="ohwtable":
                            filterSet={"include"}
                            task['value']=taskOhwTableValue(**filterConfigParameters(taskConfig,filterSet))
                        elif item=="snmptable":
                            filterSet={"oid","host","port", "readcommunity", "indexcol", "datacol", "include"}
                            task['value']=taskSnmpTableValue(**filterConfigParameters(taskConfig,filterSet))
                        else:
                            task['error']="Неизвестный тип элемента данных: "+item
                    except Exception as e:
                        task['error']=str(e)

                    if ('error' not in task.keys()):
                        if ('value' not in task.keys()):
                            task['error']="Значение не вычислено"
                        else:
                            if task.get("format",None) is not None:
                                try:
                                    task['value']=formatValue(task['value'],task['format'])
                                except Exception as e:
                                    task['error']="обработка результата: "+str(e)                                

        task.pop('config',None)
        task.pop('format',None)
        print("")
        print ("result:",task)
        #calc current timestamp after end of collecting results        
        nowDateTime=(datetime.utcnow()).strftime(timeStampFormat)
        task['timeStamp']=nowDateTime

def agentStart():
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
    print("*********")
    print("agent end")


if __name__ == '__main__':
    agentStart()