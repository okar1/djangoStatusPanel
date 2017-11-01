# -*- coding: utf-8 -*-
import psycopg2
import re

# get period from mresultconfiguration + filter modules, period is int
'''
sql="""
select magent.entity_key as "agentkey",
    magenttask.entity_key as "taskkey",
     mresultconfiguration.samplingrate as "period"
from magenttask,mmediaagentmodule,magent,mresultconfiguration
where
    magenttask.deleted=false and
    magenttask.disabled=false and
    magenttask.parent_id=mmediaagentmodule.id and
    mmediaagentmodule.parent_id=magent.id and
    magenttask.resultconfiguration_id=mresultconfiguration.id and
    mmediaagentmodule.entity_key not like '%.MediaRecorder' and
    mmediaagentmodule.entity_key not like '%.MediaStreamer' and
    mmediaagentmodule.entity_key not like '%.RecordAndStream'
"""
'''


def getDbConnection(dbConf):
    dbHost = dbConf.get('server', '')
    dbPort = dbConf.get('port', 5432)
    dbName = "qos"
    dbUser = dbConf.get('user', '')
    dbPassword = dbConf.get('pwd', '')

    conString = "dbname='{1}' user='{2}' host='{0}' password='{3}' port='{4}'".format(
        dbHost, dbName, dbUser, dbPassword, dbPort)

    return psycopg2.connect(conString)


# return tuple with error strings and all active task like {taskKey:
# {"agentKey":"aaa", "period":10} }}
def getTasks(dbConnection):
    # get period mproperty (no filter need), period is string

    error = None
    result = {}

    sql = """SELECT magent.entity_key as "agentkey",
    magent.displayname as "agentname",
    magenttask.entity_key as "taskkey",
    magenttask.displayname as "displayname",
    mproperty.value as "period"
    from magenttask,mmediaagentmodule,magent,magenttask_mproperty,mproperty
    where
    magent.deleted=false and
    magenttask.deleted=false and
    magenttask.disabled=false and
    magenttask.parent_id=mmediaagentmodule.id and
    mmediaagentmodule.parent_id=magent.id and
    magenttask_mproperty.magenttask_id=magenttask.id and
    magenttask_mproperty.properties_id=mproperty.id and
    mproperty.name='period'
    """

    try:
        cur = dbConnection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

        result = {row[2]: {"agentKey": row[0], "agentName":row[1], "itemName": row[3],
            "period": int(row[4])} for row in rows}
        return (error, result)
    except Exception as e:
        error = str(e)
        return (error, result)


# return tuple with error strings and integer originatorID for specified
# alarm type string
def getOriginatorIdForAlertType(dbConnection, alertType):

    # re.match("^[_A-Za-zа-яА-Я0-9/./-]+$","dAs.AПривет-A_A")
    if not re.match("^[_A-Za-zа-яА-Я0-9/./-]+$", alertType):
        return ("Неверно задан тип аварийного сообщения, проверьте настройку qosAlertType", None)

    sql = """SELECT gp.id from mgrouppolicy as "gp",malerttype as "al" where
            gp.state!='DELETED' and
            gp.alerttype_id=al.id and
            al.name='{0}'
            LIMIT 1""".format(alertType)

    try:
        cur = dbConnection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

        if len(rows) == 1:
            return (None, rows[0][0])
        else:
            return ("Создайте хотя бы одно оповещение с типом " + alertType, None)

    except Exception as e:
        return(str(e), None)


# check wich channels are scheduled to run in specified time and which are scheduled to be paused
# returns dict like {agent:{1:True,3:True, 5:False}}
# where agent - agentkey
# number - integer channel id (tasks with schedule support has taskkey like someAgentKey.CaptionsAnalyzer.869)
# bool - is task scheduled to run in specified timestamp or scheduled to be paused
def getChannelScheduleStatus(dbConnection,timeStamp,zapas):
    
    # check that db schema supports task schedule feature 
    sql="""
        SELECT EXISTS (
           SELECT 1
           FROM information_schema.tables 
           WHERE table_name = 'task'
        );
    """
    cur = dbConnection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    
    # table "task" not exists in db, so schedule feature is not supported
    if rows[0][0]!= True :
        return {}

    sql="""
        SELECT 
        agent.entity_key as "agentkey",
        task.channel_id as "channelid", 
        bool_or({0}>sc.begin_time and {1}<sc.end_time and devices.type != 'MATCH') as "channelactive"
        FROM qos.task as "task", qos.task_schedule as "sc", qos.magent as "agent", qos.devices as "devices"
        WHERE task.probe_id=agent.id and sc.task_id=task.id and task.device_id=devices.id
        GROUP BY agent.entity_key, task.channel_id
    """.format(str(timeStamp-zapas),str(timeStamp+zapas))
    
    cur = dbConnection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    res={}
    for row in rows:
        agentDict=res.setdefault(row[0],{})
        agentDict.update({row[1]:row[2]})

    # print(res)
    return res
