# -*- coding: utf-8 -*-
import psycopg2
import re

# wich modules are allowed to run by specific qos device and qos module group
# like {device:{"modules":{modules}} or {device:{"module_groups":"{modules}}
qosScheduleDeviceToModuleMapping={
    "MATCH":{
        "modules":{"Match"}},
    "SELF":{
        "modules":{"SelfMonitor"}},
    "SNMP":{
        "modules":{"GenericMonitor"}},
    "_other_":{
        "module_groups":{
            "RF":{"RfMeasurement"},
            "IP":{"IPStatistics"},
            "IP_STATISTICS":{"IPStatistics"},
            "RAW_RECORDING":{"RawDataRecorder"},
            "EMERGENCY_RECORDING":{"MediaRecorder"},
            "VIDEO_HASH":{"ImageSearch","ClipSearch","VideoFingerprint",},
            "AUDIO_HASH":{"AudioFingerprint"},
            "RECORDING":{"MediaRecorder"},
            "STREAMING":{"MediaStreamer"},
            "TS":{"TR101290","AtscA78","TSStructure"},
            "SUBTITLES":{"CaptionsAnalyzer"},
            "AD":{"MpegTSSplicingControlModule"},
            "AUDIO":{"LoudnessR128","LoudnessA85"},
            "VIDEO":{"QoEVideo"},
            "BITRATE":{"MpegTSStatisticsIPTVControlModule"},
        }
    }
}

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


# return tuple with error strings and all active task like 
# (error,{taskKey: {"agentKey":"aaa", "period":10}} )
def getTasks(dbConnection, defaultPeriod):
    # get period mproperty (no filter need), period is string

    error = None
    result = {}

    # This behemoth is executed once per poll (usually 60 or 120 sec).
    # it costs nearly 900 msec or less for one of our production servers.
    # sql text is constant, no conditional formatting.

    # LEFT JOIN on module is used for "url" field
    # this field is none when module havent url property (all but MediaRecorder and MediaStreamer)
    # LEFT JOIN on magenttask is used for "period" field 
    # this field is none when module havent period property (i.e. MediaRecorder and MediaStreamer)
    sql = """
    SELECT 
        agent.entity_key as "agentkey",
        agent.displayname as "agentname",
        module.displayname as "modulename",
        task.entity_key as "taskkey",
        task.displayname as "displayname",
        prop.value as "period",
        urls.url
    FROM 
        magent as "agent", mmediaagentmodule as "module" LEFT JOIN (
                -- mmediaagentmodule_mstream is a link table from mmediaagentmodule to both mrecordedstream and mlivestream
                SELECT DISTINCT ON (id) 
                    link2u.mmediaagentmodule_id as "id", m2u.url
                FROM mmediaagentmodule_mstream as "link2u",
                        -- mrecordedstream and mlivestream can be united because their id's are not intersects
                        (
                        -- mrecordedstream contains urls for MediaRecorder
                        SELECT id, templatestreamurl as "url" FROM mrecordedstream
                            UNION ALL
                        -- mlivestream contains urls for MediaStreamer
                        SELECT id, templateurl as "url" FROM mlivestream
                        ) as "m2u"
                WHERE link2u.templatestreams_id=m2u.id
            ) as "urls" ON module.id=urls.id,
        magenttask as "task" LEFT JOIN (
                -- select all properties with name="period"
                SELECT t2p.magenttask_id, p.value
                FROM magenttask_mproperty as "t2p", mproperty as "p"
                WHERE p.name='period' and t2p.properties_id=p.id
            ) as "prop"
            ON task.id=prop.magenttask_id
    WHERE
        agent.id=module.parent_id and
        module.id=task.parent_id and
        
        agent.deleted=false and
        task.deleted=false and
        task.disabled=false
    """

    try:
        cur = dbConnection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    except Exception as e:
        error = str(e)
        return (error, result)

    # incoming url like "rtmpt://10.10.10.10:8077/qligentPlayer/stream_${properties.streamTaskId}"
    # result like 10.10.10.10:8077
    def _getAddress(url):
        if url is None:
            return None
        pattern=r"\d*://(.+):(\d+)/.*"
        r=re.search(pattern,url)
        if r is not None:
            return (r.group(1),r.group(2))
        else:
           return None

    res={}
    for row in rows:
        tmp=_getAddress(row[6])
        res.update({
            row[3]: {
                "agentKey": row[0],
                "agentName":row[1],
                "module": row[2],
                "itemName": row[4],
                "enabled":True, # disabled tasks are excluded by sql
                "period": defaultPeriod if row[5] is None else int(row[5]),
                "serviceIp": tmp[0] if tmp is not None else None,
                "servicePort": tmp[1] if tmp is not None else None,
                }            
            })
    return (error, res)



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


# check wich modules are scheduled to run on channels in specified time
# returns dict like {agent:{1:{GenericMonitor},3:{CaptionsAnalyzer,MediaRecorder}, 5:{}}}
# where agent - agentkey
# number - integer channel id (tasks with schedule support has taskkey like someAgentKey.CaptionsAnalyzer.869)
# set  - is modules that scheduled to run in specified timestamp
# if some channel id is absent - this channel is absent in schedule
# when schedule not supported by qos - returns none
def getChannelScheduledModules(dbConnection,timeStamp,zapas):
    
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
        return None

    sql="""
        SELECT 
        agent.entity_key as "agentkey",
        task.channel_id as "channelid",
        devices.type as "device",
        profile.configuration as "module_groups",
        bool_or({0}>sc.begin_time and {1}<least(sc.end_time,sc.until) and devices.type!='MATCH') as "channelactive"
        FROM qos.task as "task", qos.task_schedule as "sc", qos.magent as "agent", qos.devices as "devices", qos.monitoring_profile as "profile"
        WHERE task.probe_id=agent.id and task.id=sc.task_id and task.device_id=devices.id and task.profile_id=profile.id and devices.deleted=false
        GROUP BY agent.entity_key, task.channel_id, devices.type, profile.configuration
    """.format(str(timeStamp-zapas),str(timeStamp+zapas))
    
    cur = dbConnection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    if len(rows)==0:
        return None

    def _getModulesForDevice(channelIsActive,device,allowedGroups):
        # if channel is not scheduled now - cancel module search
        if not channelIsActive:
            return set()

        allMap=qosScheduleDeviceToModuleMapping
        if device in allMap.keys():
            devMap=allMap[device]
        else:
            devMap=allMap["_other_"]

        if "modules" in devMap.keys():
            res=devMap["modules"]
        else:
            groupMap=devMap["module_groups"]
            res=set()
            for groupName, groupIsAllowed in allowedGroups.items():
                if groupIsAllowed and (groupName in groupMap.keys()):
                    res.update(groupMap[groupName])
        return res 

    res={}
    for row in rows:
        agentDict=res.setdefault(row[0],{})
        modulesSet=agentDict.setdefault(row[1],set())
        modulesSet.update(_getModulesForDevice(row[4],row[2],row[3]['details']))
        # if row[1]==1534:
        #     print(_getModulesForDevice(row[4],row[2],row[3]['details']))
    # print(res)
    return res
