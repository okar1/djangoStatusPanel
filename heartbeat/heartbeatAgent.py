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

presets={
    "smLsiRaid":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.100.1.11", "indexcol":7, "datacol":27},
    "smChasis":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1", "indexcol":2, "datacol":4, "include":["Chassis.*"]},
    "smPowerStatus":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1","indexcol":2, "datacol":4, "include":["PS.*Status"]},
    "smTemperature":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1", "indexcol":2, "datacol":4, "include":[".*Temp.*"]},
    "smFan":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1", "indexcol":2, "datacol":4, "include":["FAN.*",".*Fan.*"]},
    "ohwHddUsed":{"item":"ohwtable", "include":["/hdd.*/load/.*"]},
    "ohwRamUsed":{"item":"ohwtable", "include":["/ram/load"]},
    "ohwCpuTemperature":{"item":"ohwtable", "include":["cpu/.*/temperature/0"]},
    "ohwCpuLoad":{"item":"ohwtable", "include":["cpu/.*/load/0"]},
}

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
                vErrors += ['Ошибка обработки сообщения: неверный ключ '+taskKey]
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


def taskSnmp(oid, host ,port, readcommunity):
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
    req = snmp.getCmd(snmp.SnmpEngine(),
                snmp.CommunityData(readcommunity),
                snmp.UdpTransportTarget((host, port)),
                snmp.ContextData(),
                snmp.ObjectType(snmp.ObjectIdentity(oid)))
    reply=next(req)

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
        raise Exception(str(reply[0]))


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
            # 1 - column index (usually starts from 1, but can differ)
            # 2 - row index (usually starts from 1, but can differ)
            # 127.0.0.1 - index row value
            
            # load first entry from table oid
            entryIndex=1
            prefix=mainoid+'.'+str(entryIndex)+"."

            if not oid.startswith(prefix):
                break

            oid=oid[len(prefix):]
            # now oid like 2.127.0.0.1

            splitted=oid.split('.')

            # sometimes colindex can skip some cols: 1,2, 4,5,6
            colIndex=int(splitted[0])

            # index value = row index + index row value
            # sometimes inde row value is empty - so use only row value
            indexValue='.'.join(splitted[1:])
            value=_snmpFormatValue(varBind[1])

            if indexValue not in res.keys():
                res[indexValue]=[]
            rowValuesList=res[indexValue]

            oldLen=len(rowValuesList)
            if colIndex<oldLen:
                raise Exception("Unknown SNMP table error")

            if colIndex>oldLen:
                # extend array if colindex had skipped values
                rowValuesList+=[None for nothing in range(colIndex-oldLen)]

            rowValuesList+=[value]

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


def taskSnmpTableValue(include, oid, host, port, readcommunity, indexcol, datacol):
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
    # value is single and format is single
    def _do1value1format(v,format):
        
        if type(format) is not dict:
           raise Exception("проверьте настройку обработки результата")
        
        item=format.get("item",None)

        # convert to numeric value.
        # optional parameter "decimalplaces" is supported (default is 0)
        if item =="number":
            params=checkParameters(format,{
                "decimalplaces":{"type":int,
                                 "mandatory":False}}
            )
            v=toFloat(v)
            decPlaces=params['decimalplaces']
            if decPlaces is not None:
                v=round(v,decPlaces)
        # convert to boolean value.
        # optional lists [truevalues] and [falsevalues] are supported
        # default value is returned if not found in truevalues and falsevales
        elif item=="bool":
            params=checkParameters(format,{
                "truevalues":{"type":list,
                              "mandatory":False},
                "falsevalues":{"type":list,
                               "mandatory":False},              
                "default":{ "type":bool,
                            "mandatory":False,
                            "default":False},              
                }
            )

            default=params['default']
            trueValues=params['truevalues']
            falseValues=params['falsevalues']

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
            params=checkParameters(format,{
                "find":{"type":str,
                        "mandatory":True},
                "replace":{"type":str,
                           "mandatory":True},
                "ignorecase":{"type":bool,
                           "mandatory":False,
                           "default":False},
                }
            )
            sFind=params['find']
            sReplace=params['replace']
            ignoreCase=params['ignorecase']
            
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
            params=checkParameters(format,{
                "value":{"type":[int,float],
                        "mandatory":True},
                }
            )
            v=toFloat(v)+params['value']
        # multiply number to number value.
        # mandatory number "value" must be specified
        elif item=="multiply":
            params=checkParameters(format,{
                "value":{"type":[int,float],
                        "mandatory":True},
                }
            )
            v=toFloat(v)*params['value']
        # exclude tasks if got value from list specified.
        elif item=="exclude":
            params=checkParameters(format,{
                "values":{"type":list,
                        "mandatory":True},
                }
            )
            if v in params['values']:
                v=None
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

# check that format contains mandatory parameters and that parameter types are correct
# param like {"item": "formatName", "param1":param1value, "param2":"param2value"}
# pattern like {"param1":{"type":dict,"mandatory":true}, "param2": ...}
# if mandatory parameter absent - raise exception
# if optional parameter absent - add to param "default" value or None
# returns modified "param" value
def checkParameters(param,pattern):
    
    param=param.copy()

    #remove param items if absent in pattern
    par2remove=set({k:None for k in param.keys() if k not in pattern.keys()})
    for p2r in par2remove:
        param.pop(p2r)

    # check params
    for paramKey,paramOpts in pattern.items():
        isMandatory=paramOpts.get('mandatory',True)
        if isMandatory and (paramKey not in param.keys()):
            raise Exception("не указан параметр "+paramKey+" Проверьте настройку обработки результата")
        else:
            # (mandatory and present) or (not mandatory)
            paramValue=param.get(paramKey,None)
            if paramValue is not None:
                paramType=paramOpts['type']
                # paramtype is list of types like [int, str, float, ...]
                # and check OK if type of value is in this list
                # or
                # paramtype is type (like float)
                # and check OK if type of value is equal it
                if (type(paramType)==list and (type(paramValue) not in paramType)) or \
                   (type(paramType)!=list and (type(paramValue)!=paramType)):
                    raise Exception("тип параметра "+paramKey+" задан неверно. Проверьте настройку обработки результата")                        
            else:
                # use default value if "default" key specified in pattern
                defaultValue=paramOpts.get('default',None)
                param[paramKey]=defaultValue
    # param is modified in any case
    return param


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
    task2remove=set()

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

                    preset=presets.get(item,None)

                    # apply preset to taskConfig. Old taskConfig parameters has priority
                    # but only "item" parameter from preset has priority
                    if preset is not None:
                        preset=preset.copy()
                        item=preset['item']
                        preset.update(taskConfig)
                        preset['item']=item
                        taskConfig=preset
                        preset=None

                    try:
                        if item=="ohwtable":
                            task['value']=taskOhwTableValue(**checkParameters(taskConfig,{
                                "include":{ "type":list,
                                            "mandatory":True},
                                }))
                        elif item=="snmptable":
                            task['value']=taskSnmpTableValue(**checkParameters(taskConfig,{
                                "include":{ "type":list,
                                            "mandatory":False,
                                            "default":[".*"]},
                                "oid":{ "type":str,
                                        "mandatory":True},
                                "host":{    "type":str,
                                            "mandatory":False,
                                            "default":"127.0.0.1"},
                                "port":{    "type":int,
                                            "mandatory":False,
                                            "default":161},
                                "readcommunity":{   "type":str,
                                                    "mandatory":False,
                                                    "default":"public"},
                                "indexcol":{    "type":int,
                                                "mandatory":True},
                                "datacol":{     "type":int,
                                                "mandatory":True},
                                }))
                        else:
                            raise Exception( "Неизвестный тип элемента данных: "+item)
                    except Exception as e:
                        task['error']=str(e)

                    # got a value, not got a error
                    if ('error' not in task.keys()) and ('value' in task.keys()):

                        # formatting value
                        if task.get("format",None) is not None:
                            try:
                                task['value']=formatValue(task['value'],task['format'])
                            except Exception as e:
                                task['error']="обработка результата: "+str(e)    
                        value=task['value']
                            
                        # value is set, but equals none - remove such task
                        if value is None:
                            task2remove.add(taskKey)
                        elif type(value)==dict:
                            # and for composite tasks - remove None results too
                            task['value']={k:v for k,v in value.items() if v is not None}

        task.pop('config',None)
        task.pop('format',None)
        print("")
        print ("result:",task)
        #calc current timestamp after end of collecting results        
        nowDateTime=(datetime.utcnow()).strftime(timeStampFormat)
        task['timeStamp']=nowDateTime
    #end for

    # remove tasks that returned none
    for t2r in task2remove:
        tasksToPoll.pop(t2r)


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