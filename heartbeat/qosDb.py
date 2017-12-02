# -*- coding: utf-8 -*-
import psycopg2
import re
import json

service_type=0
broadcasting_type=1
medium_type=2
delivery_method=3

codeToName={
    service_type:{
        0:"PROGRAM",
        1:"SNMP",
        2:"MEASUREMENT",
        3:"SELF",
        4:"SCAN"
    },
    broadcasting_type:{
        0:"TV",
        1:"RF"
    },
    medium_type:{
        0:"CABLE",
        1:"IP",
        2:"ETHER"
    },
    delivery_method:{
        0:"DIGITAL",
        1:"ANALOG"
    },
}

sampletype_logo=0
sampletype_vdRadio=1
sampletype_zipRadio=3
sampletype_vdTV=4
sampletype_zipTV=2

sampletypesForImageSearch={sampletype_logo,sampletype_zipTV}
sampletypesForClipSearch={sampletype_vdRadio,sampletype_zipRadio,sampletype_vdTV}

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


def getAllAgents():
    if not hasattr(getAllAgents,"__result__"):
        getAllAgents.__result__={}
    return getAllAgents.__result__


# return tuple with error strings and all active task like 
# (error,{taskKey: {"agentKey":"aaa", "period":10}} )
def getTasks(dbConnection, defaultPeriod):
    # get period mproperty (no filter need), period is string

    error = None
    result = {}
    allAgents=None

    sql="SELECT entity_key,displayname FROM magent WHERE not deleted"
    try:
        cur = dbConnection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    except Exception as e:
        error = str(e)
        return (error, result)
    getAllAgents.__result__={row[0]:row[1] for row in rows}

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
        urls.url,
        task.disabled        
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
        task.deleted=false
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
        agentKey=row[0]
        agentName=row[1]
        moduleName=row[2]
        taskkey=row[3]

        if taskkey.find(agentKey)!=0:
            if allAgents is None:
                allAgents=getAllAgents()
            taskArr=taskkey.split(".")
            if len(taskArr)<3:
                error="Неверный ключ задачи: "+taskkey
                return (error, {})
            agentKey=taskArr[0]
            agentName=allAgents.get(agentKey,None)
            if agentName is None:
                error="Неверный ключ БК: "+agentName
                return (error, {})

        res.update({
            row[3]: {
                "agentKey": agentKey,
                "agentName":agentName,
                "module": moduleName,
                "itemName": row[4],
                "enabled":not row[7],
                "qosEnabled":not row[7],                
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


# when schedule not supported by qos - returns none
# else returns set of task keys
def getScheduledTaskKeys(dbConnection,timeStamp,zapas):
    
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
        id,configuration
        FROM
        qos.monitoring_profile
    """
    cur = dbConnection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    profiles={row[0]:row[1]['details'] for row in rows}


    with open('probe-config-defaults-v2.json',encoding="UTF-8") as f:
        data = f.read()
    probeConfigFull=json.loads(data)
    probeConfigServices=probeConfigFull['services']
    probeConfigDevices=probeConfigFull['devices']
    probeConfigModules=probeConfigFull['modules']

    sql="""
        SELECT 
            agent.entity_key as "agentkey",
            task.channel_id,
            devices.type as "device",
            task.profile_id,
            services.service_type,
            services.broadcasting_type,
            services.medium_type,
            services.delivery_method,
            array_agg(samples.sampletype) as "sampletypes",
            CASE
              --load parameters for selfMonitor only
              WHEN service_type=3 then services.parameters
              ELSE null
            END as "parameters"
        FROM 
            qos.task_schedule as "sc", qos.magent as "agent", qos.devices as "devices", qos.mass_media_monitoring as "services",
            --join sample types specified for this task and aggregate them as list
            qos.task as "task" LEFT JOIN (SELECT task_id,type as "sampletype" from mass_media_sample_group ) as "samples" ON task.id=samples.task_id
        WHERE
            task.channel_id=services.id and task.probe_id=agent.id and task.id=sc.task_id and task.device_id=devices.id and devices.deleted=false
            and {0}>sc.begin_time and {1}<least(sc.end_time,sc.until)
        GROUP BY 
            agent.entity_key,
            task.channel_id,
            devices.type,
            task.profile_id,
            services.service_type,
            services.broadcasting_type,
            services.medium_type,
            services.delivery_method,
            services.parameters
    """.format(str(timeStamp-zapas),str(timeStamp+zapas))
    
    cur = dbConnection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()


    if len(rows)==0:
        return None

    res=set()
    for row in rows:
        agentKey, channelId, device, profileId, *configCodes, sampleTypes, taskParameters = row

        # "MATCH" device supports only "Match" module
        if device=="MATCH":
            moduleNames={"Match."}
        # find allowed modules for non-"MATCH" devices
        else:
            # confignames contains level keys in probeConfigServices structure like
            # ["PROGRAM","TV","CABLE","DIGITAL"]
            # or 
            # ["SCAN", None, None ,None]
            configNames=[]
            for i,code in enumerate(configCodes):
                configNames+=[codeToName.get(i,{}).get(code,None)]

            # scan structure from DB like 
            # {"AD": false, "RF": false, "TS": true, "AUDIO": false, "VIDEO": true, "BITRATE": false, ...}
            # and collect allowedProfiles like
            # {"TS","VIDEO"}

            # service_type is "PROGRAM"
            if configCodes[0]==0:
                allowedProfiles=set({pName: pAllowed for pName,pAllowed in profiles[profileId].items() if pAllowed})
            # service_type is "SELF"
            elif configCodes[0]==3:
                allowedProfiles=set({pName: pAllowed for pName,pAllowed in taskParameters['profiles'].items() if pAllowed})
            # allow all profiles for other service types
            else:   
                allowedProfiles=set()
            
            # scan probeConfigServices structure and get allowed module profile names wich are allowed to run now
            # moduleProfiles like {"TR101290","MpegTSStatisticsIPTVControlModule_STREAM"}

            moduleProfiles=set()
            levelData=probeConfigServices
            for nextLevelName in configNames:
                if 'signals' in levelData:
                    levelData=levelData['signals']
                if 'config' in levelData:
                    levelData=levelData['config']
                if nextLevelName is not None and nextLevelName in levelData:
                    levelData=levelData[nextLevelName]
                if 'profile' in levelData:
                    allModuleProfiles=levelData['profile']

                    if not allowedProfiles:
                        tmp=allModuleProfiles
                    else:
                        tmp={pName:pValue for pName,pValue in allModuleProfiles.items() if pName in allowedProfiles}

                    for profilesList in tmp.values():
                        moduleProfiles.update(set(profilesList))
                    break

            # TODO: scan probeConfigDevices structure and check that device supports moduleProfiles and signal types.
            # Exclude moduleProfiles that are not supported by device

            pass

            # scan probeConfigModules structure and convert moduleProfiles names to module name with dot and perfix
            # moduleNamePerfix like {"TR101290.","MpegTSStatisticsIPTVControlModule.","SelfMonitor.SMCPU_"}
            # (perfixes mainly used in SelfMonitor only)

            moduleNames=set()
            for profileName in moduleProfiles:
                p=probeConfigModules[profileName]
                if "prefix" in p:
                    prefix=p['prefix']+"_"
                else:
                    prefix=""
                moduleNames.add(p['module']+"."+prefix)
        # endif not "MATCH"

        # convert moduleNames to full-specified taskKeys like "MyAgent.SelfMonitor.SMCPU_2223"
        # add taskKeys to res
        sampleTypes=set(sampleTypes)
        for moduleNameAndPerfix in moduleNames:
            # add ImageSearch and ClipSearch tasks only if samples of corresponding type are specified
            if moduleNameAndPerfix=="ImageSearch.":
                if not (sampleTypes & sampletypesForImageSearch):
                    continue
            if moduleNameAndPerfix=="ClipSearch.":
                if not (sampleTypes & sampletypesForClipSearch):
                    continue
            res.add(agentKey+"."+moduleNameAndPerfix+str(channelId))

    # endfor row

    return res
