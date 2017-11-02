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
import subprocess
import sys
import traceback
# import binascii

mqConf={
    "server":'127.0.0.1',
    "port":"15672",
    "user":"{{rabbitmq_user}}",
    "pwd":"{{rabbitmq_pass}}",
    "vhost":"/"
    }

logFile='heartbeatAgent.log'
receiveFromQueue='heartbeatAgentRequest'
sendToExchange='heartbeatAgentReply'
maxMsgTotal=50000
amqpPort = 5672
timeStampFormat="%Y%m%d%H%M%S"
agentProtocolVersion=2

presets={
    "smLsiRaid":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.100.1.11", "indexcol":7, "datacol":27},
    "smChasis":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1", "indexcol":2, "datacol":4, "include":["Chassis.*"]},
    "smPowerStatus":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1","indexcol":2, "datacol":4, "include":["PS.*Status"]},
    "smTemperature":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1", "indexcol":2, "datacol":4, "include":[".*Temp.*"]},
    "smFan":{"item":"snmptable","oid":"1.3.6.1.4.1.10876.2.1.1.1", "indexcol":2, "datacol":4, "include":["FAN.*",".*Fan.*"]},
    "ohwHddUsed":{"item":"ohwtable", "include":["hdd.*\.load\."]},
    "ohwRamUsed":{"item":"ohwtable", "include":["ram\.load"]},
    "ohwCpuTemperature":{"item":"ohwtable", "include":["cpu\..*\.temperature\.0"]},
    "ohwCpuLoad":{"item":"ohwtable", "include":["cpu\..*\.load\.0"]},
    "windowsFirewallStatus":{"item":"shell", "command":"netsh Advfirewall show allprofiles", "utf8":True, "timeout":3,
        "include":[r"""
            (?sx)
            .*?
             (\w*?)\ Profile\ Settings: # key: Domain Private or Public
            .*?
            State\s*(\w*?)\r\n # value: ON or OFF
        """]},
    "windowsNetworkType":{"item":"shell", "command":"powershell -c Get-NetConnectionProfile", "utf8":True, "timeout":6,
        "include":[r"""
            (?sx)
            InterfaceAlias\s*:\s*(\w*) # Network adapter name
            .*?
            NetworkCategory\s*:\s*(\w*) # Public Domain Private
        """]},
    
    # sample ntpq -p output:
    #      remote           refid      st t when poll reach   delay   offset  jitter
    # ==============================================================================
    #  *GPS_NMEA(6)     .GPS.            0 l    -   64    0    0.000    0.000   0.000
    #  51.140.127.197  128.138.141.172  2 u   23   64    1   75.455  117.537   0.000
    "ntpSyncAgoSec":{"item":"shell", "command":"'c:\\Program Files (x86)\\NTP\\bin\\ntpq.exe' -p", "utf8":True, "timeout":10,
        "include":[r"""
            (?x)
               ([\w]* [\(\.] [\S]*)  # remote field: to filter header it must contain dot or parentheses: key
               \s*\S*                # refid field
               \s*\S*                # st field
               \s*\S*                # t field
               \s*(\S*)              # when field: value
               \s*\S*                # poll field
               \s*\S*                # reach field
               \s*\S*                # delay field
               \s*\S*                # offset field
               \s*\S*                # jitter field
        """]},
    "ntpSyncOffset":{"item":"shell", "command":"'c:\\Program Files (x86)\\NTP\\bin\\ntpq.exe' -p", "utf8":True, "timeout":10,
        "include":[r"""
            (?x)
               ([\w]* [\(\.] [\S]*)  # remote field: to filter header it must contain dot or parentheses: key
               \s*\S*                # refid field
               \s*\S*                # st field
               \s*\S*                # t field
               \s*\S*                # when field
               \s*\S*                # poll field
               \s*\S*                # reach field
               \s*\S*                # delay field
               \s*(\S*)              # offset field: value
               \s*\S*                # jitter field
        """]},
    "intelRaid":{"item":"shell", "command":"c:\\monitoring\\rstcli64.exe -I -v", "utf8":True, "timeout":3,
        "include":[r"""
            (?sx)
            --VOLUME\ INFORMATION-- #this string first
            .*?
            Name:\s*(\w*) # Name: <some spaces> Volume1
            .*?
            State:\s*(\w*) # State: <some spaces> Normal
            .*?
            --DISKS\ IN\ VOLUME # include it in search to find next volume and skip disks
        """]},
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
    
    errors=set()

    if not mqAmqpConnection:
        errors.add('Соединение с RabbitMQ не установлено')
        return errors

    channel = mqAmqpConnection.channel()

    try:
        channel.exchange_declare(exchange=sendToExchange, exchange_type='topic', durable=True)
    except Exception as e:
        errors.add(str(e))
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
            
            # filter fields for sending server --> agent
            msgBody={k:v for k,v in task.items() if k in ['config',]}
        else:
            timeStamp=task['timeStamp']
            # filter fields for sending agent --> server
            msgBody={k:v for k,v in task.items() if k in ['value',]}
            
        msgHeaders={'key':taskKey,'timestamp':timeStamp,'unit':task['unit'],
                    'protocolversion':agentProtocolVersion}

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

    errors=set()

    if not mqAmqpConnection:
        errors.add('Соединение с RabbitMQ не установлено')
        return errors

    channel = mqAmqpConnection.channel()

    try:
        channel.queue_declare(queue=receiveFromQueue, durable=True,arguments={'x-message-ttl':1800000})
    except Exception as e:
        errors.add(str(e))
        return errors

    try:
        channel.exchange_declare(exchange=receiveFromQueue, exchange_type='topic', durable=True)
    except Exception as e:
        errors.add(str(e))
        return errors

    try:
        channel.queue_bind(queue=receiveFromQueue, exchange=receiveFromQueue, routing_key="#")
    except Exception as e:
        errors.add(str(e))
        return errors

    mqMessages = []
    while True:
        # connect and check errors (amqp)
        getOk = None
        try:
            getOk, *mqMessage = channel.basic_get(receiveFromQueue, no_ack=True)
        except Exception as e:
            errors.add(str(e))
            continue
        if getOk:
            mqMessage=[getOk]+mqMessage
            if mqMessage[1].content_type != 'application/json':
                errors.add("Неверный тип данных " + mqMessage[0].content_type)
                continue
            mqMessages += [mqMessage]
        else:
            break
    # endwhile messages in rabbit queue            

    # now we have list of mqMessages
    for msg in mqMessages:

        try:
            headers = msg[1].headers
            taskKey=headers['key']
            taskUnit=headers['unit']
        except Exception as e:
            errors.add("Ошибка обработки сообщения: неверный заголовок.")
            continue

        # parse message payload
        try:
            msgBody = json.loads((msg[2]).decode('utf-8'))
            assert type(msgBody)==dict
        except Exception as e:
            errors.add("Ошибка обработки сообщения: неверное содержимое.")
            continue
        
        if serverMode:            
            if headers.get('protocolversion',0) < agentProtocolVersion:
                errors.add('Версия heartbeatAgent устарела. Обновите heartbeatAgent на '+msg[0].routing_key)
                continue

            if taskKey not in tasksToPoll.keys():
                errors.add('Ошибка обработки сообщения: неверный ключ '+taskKey)
                continue

            try:
                stringTimeStamp=headers['timestamp']
                taskTimeStamp=datetime.strptime(stringTimeStamp, timeStampFormat)
                tasksToPoll[taskKey]['timeStamp']=taskTimeStamp
            except Exception as e:
                errors.add("Ошибка обработки сообщения: неверная метка времени.")
                continue

            error=headers.get('error',None)
            
            # filter fields when reseiving at server side
            tasksToPoll[taskKey].update({k:v for k,v in msgBody.items() if k in ['value',] })
            tasksToPoll[taskKey]['unit']=taskUnit
            if error is not None:
                tasksToPoll[taskKey]['error']=error
        else:
            # filter fields when reseiving at agent side
            tasksToPoll[taskKey]={'module':'heartbeat','agentKey':msg[0].routing_key,'unit':taskUnit}
            tasksToPoll[taskKey].update({k:v for k,v in msgBody.items() if k in ['config',]  })
    # endfor messages in current request
    return errors


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
            raise Exception("не указан параметр "+paramKey+" Проверьте настройки программы")
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
                    raise Exception("тип параметра "+paramKey+" задан неверно. Проверьте настройки программы")                        
            else:
                # use default value if "default" key specified in pattern
                defaultValue=paramOpts.get('default',None)
                param[paramKey]=defaultValue
    # param is modified in any case
    return param


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
    return {((item.Identifier).replace('/','.')[1:]):item.Value for item in q} # +item.Name


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


# parses string with every regex in include. Return key:value dict.
# See taskShellTableValue description for more details
# no error handling
def parseString(string,include):
    assert type(string)==str
    assert type(include)==list

    res={}
    counter=0

    for pattern in include:
        match=re.findall(pattern,string)
        if match:
            for item in match:
                if type(item)==str and item!='':
                    # got only value from regex. Auto generate the key
                    res[str(counter)]=item
                    counter+=1
                elif type(item)==tuple:
                    if len(item)==2:
                        # got key and value from regex
                        res[item[0]]=item[1]
                    else:
                        raise Exception("regex должен возвращать строго 1 или 2 группы. Проверьте настройки ПО.")
                else:
                    raise Exception("неизвестная ошибка regex")
    return res


# run shell command. Read stdout. Parse stdOut with every regex in include list
# regex must return 2 or 1 groups:
# 2 groups result is processed as taskkey: taskvalue
# 1 group result is processed as taskvalue, taskkey will be generated automatically
# If "utf8" flag is set - decode console stdout as cp65001 else cp866
#   (it is usefull with system commands on russian windows locale. All output messages became english)
# timeout is optional time limit for running shell command in seconds
def taskShellTableValue(command,include,utf8,timeout):
    assert type(command)==str
    assert type (include)==list
    assert type(utf8)==bool

    if not hasattr(taskShellTableValue,"commandDict"):
        taskShellTableValue.commandDict={}
    commandDict=taskShellTableValue.commandDict



    if command not in commandDict.keys():
        
        # when file name or path contain spaces - full path must be included in single or double quotes
        # like "c:\long path\some program.exe" -param1 -param2  
        quoteSymbol=False
        if command[0]=='"' or command[0]=="'":
            quoteSymbol=command[0]
            # check closing quote
            if command.find(quoteSymbol,1)==-1:
                raise Exception("Если путь к программе содержит пробелы - используйте кавычки")
            exeNamePath=command.split(quoteSymbol)[1]
            paramString=command[len(exeNamePath)+2:] # 2 symbols for open and close quotes
            commandList= [exeNamePath] + paramString.split(" ")
            commandList=[v for v in commandList if v!=''] #remove empty spaces in case of -param1<space1><space2>-param2
        else:
            commandList=command.split(" ")

        # for switching windows console stdout to unicode mode:
        # 1) use shell=True (this param is no need elsewhere)
        # 2) add key to windows registry:
        #       regtool add /user/Console
        #       regtool add /user/Console/%SystemRoot%_system32_cmd.exe
        #       regtool set /user/Console/%SystemRoot%_system32_cmd.exe/CodePage 0xfde9
        # 3) set var utf8=True for correct decoding
        if timeout is not None:
            runResult=subprocess.run(commandList,stderr=subprocess.PIPE,stdout=subprocess.PIPE,timeout=timeout,shell=True)
        else:
            runResult=subprocess.run(commandList,stderr=subprocess.PIPE,stdout=subprocess.PIPE,shell=True)

        if runResult.returncode!=0:
            err=runResult.stderr
            err=err.decode("cp65001") if utf8 else err.decode("cp866")
            raise Exception("Код "+str(runResult.returncode)+" "+err)

        commandResult=runResult.stdout
        # print(commandResult)
        commandResult=commandResult.decode("cp65001") if utf8 else commandResult.decode("cp866")

        # store command stdout for caching future requests
        commandDict[command]=commandResult
    else:
        commandResult=commandDict[command]
    #end if

    return parseString(commandResult,include)

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
            continue

        taskConfig=task.get('config',None)
        if taskConfig is None or type(taskConfig)!=dict:
            task['error']="Не заданы настройки элемента данных"
            continue

        item=taskConfig.get('item',None)

        if item is None:
            task['error']="В настройках элемента данных не задано значение item"
            continue
        
        if item=="shell":
            task['error']="Непосредственный запуск модуля shell запрещен в целях безопасности"
            continue

        # task['value']={"key1":"value1","key2":"value2"}
        # task['unit']="кг/ам"

        preset=presets.get(item,None)

        # apply preset to taskConfig. Old taskConfig parameters has priority
        # but some special reset parameters
        if preset is not None:
            preset=preset.copy()
            
            # remove items from taskConfig, that must be overriten by preset
            taskConfig.pop('item',None)
            if preset.get('item',None)=='shell':
                # for security reasons passing external shell commands is not allowed
                taskConfig.pop('command',None)
            
            preset.update(taskConfig)
            taskConfig=preset
            item=taskConfig['item']
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
            elif item=="shell":
                task['value']=taskShellTableValue(**checkParameters(taskConfig,{
                    "command":{ "type":str,
                                "mandatory":True},
                    "include":{ "type":list,
                                "mandatory":False,
                                "default":["(.*)"]},
                    "utf8":{ "type":bool,
                                "mandatory":False,
                                "default":False},
                    "timeout":{ "type":int,
                                "mandatory":False,
                                "default":None},

                    }))

            else:
                raise Exception( "Неизвестный тип элемента данных: "+item)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            task['error']=str(e)

        # got a value, not got a error
        if ('error' not in task.keys()) and ('value' in task.keys()):
            value=task['value']

            # value is set, but equals none - remove such task
            if value is None:
                task2remove.add(taskKey)
            else:
                if type(value)==dict:
                    # and for composite tasks - remove None results too
                    task['value']={k:v for k,v in value.items() if v is not None}
                    # not return empty composite tasks. Return  none instead
                    if not task['value']:
                        task['value']=None
                        task2remove.add(taskKey)
        # endif

        task.pop('config',None)
        print("")
        if taskKey not in task2remove:
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
    errors=set()
    tasksToPoll={}
    mqAmqpConnection=getMqConnection(mqConf,errors,maxMsgTotal)
    errors.update(receiveHeartBeatTasks(mqAmqpConnection,tasksToPoll,receiveFromQueue))
    processHeartBeatTasks(tasksToPoll)
    errors.update(sendHeartBeatTasks(mqAmqpConnection,tasksToPoll,sendToExchange))
    if errors:
        for e in errors:
            print(e)
    print("*********")
    print("agent end")


if __name__ == '__main__':
    agentStart()