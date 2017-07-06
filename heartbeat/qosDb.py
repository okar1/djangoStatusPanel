# -*- coding: utf-8 -*-
import psycopg2

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
	magenttask.parent_id=mmediaagentmodule.id and mmediaagentmodule.parent_id=magent.id and
	magenttask.resultconfiguration_id=mresultconfiguration.id and 
	mmediaagentmodule.entity_key not like '%.MediaRecorder' and
	mmediaagentmodule.entity_key not like '%.MediaStreamer' and
	mmediaagentmodule.entity_key not like '%.RecordAndStream' 
"""
'''
#return tuple with error strings and all active task like {taskKey: {"agentKey":"aaa", "period":10} }} 
def getTasks(dbConf):
	# get period mproperty (no filter need), period is string
	
	dbHost=dbConf.get('server','')
	dbPort=dbConf('port',5432)
	dbName="qos"
	dbUser=dbConf.get('user','')
	dbPassword=dbConf.get('pwd','')

	error=None
	result=[]

	conString="dbname='{1}' user='{2}' host='{0}' password='{3}' port='{4}'".format(
		dbHost,dbName,dbUser,dbPassword,dbPort)

	sql="""select magent.entity_key as "agentkey", 
	magenttask.entity_key as "taskkey",
	magenttask.displayname as "displayname", 
	mproperty.value as "period"
	from magenttask,mmediaagentmodule,magent,magenttask_mproperty,mproperty
	where 
	magent.deleted=false and
	magenttask.deleted=false and
	magenttask.disabled=false and
	magenttask.parent_id=mmediaagentmodule.id and mmediaagentmodule.parent_id=magent.id and
	magenttask_mproperty.magenttask_id=magenttask.id and magenttask_mproperty.properties_id=mproperty.id and
	mproperty.name='period'
	"""

	try:
		conn = psycopg2.connect(conString)
		cur = conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()


		result={row[1]: {"agentKey":row[0], "displayname":row[2], "period":int(row[3]) } for row in rows}
		# result={}
		# for row in rows:
		# 	v=result.get(row[0],{})
		# 	v.update({row[1]: {"period":int(row[2])} })
		# 	result[row[0]]=v
		return (error,result)
	except Exception as e:
		error=str(e)
		return (error,result)

# return tuple with error strings and integer originatorID for specified alarm type string
def getOriginatorIdForAlertType(dbConf,alertType):
	pass