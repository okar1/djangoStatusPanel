# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras


def tupleToString(v):
	l=len(v)
	if l==0:
		return ""
	res=str(tuple(v))
	if res[l-2]==")":
		res=res[:(l-3)]+")"
	return res	



def query(agentList=(), channelList=(),moduleList=(), resultType=("agent","channel","module","parameterName","taskName","taskKey")):
	try:
	    conn = psycopg2.connect("dbname='qos' user='qos' host='localhost' password='Tecom1' port=5432")
	except psycopg2.Error as err:
	    raise Exception("Connection error: {}".format(err))

	#5 групп по 3 символа через точку. В каждой группе буквы,цифры,+ или _. Первая группа - начинается с буквы
	#allowedChannelNames=r"[a-zA-Z][a-zA-Z0-9_+]{2}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}"
	allowedChannelNames=r"[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3} "
	allowedChannelSQL="substring(mgrouppolicy.taskidentifier_taskname from '{0}')".format(allowedChannelNames)

	sectDistinct=[]
	sectSelect=[]
	sectGroup=[]
	sectOrder=[]
	if "agent" in resultType:
		sectDistinct.append('"agentKey"')
		sectSelect.append('mgrouppolicy_agent_list.agent_name as "agentKey"')
		sectGroup.append('"agentKey"')
		sectOrder.append('"agentKey" ASC')
	if "module" in resultType:
		sectDistinct.append('"moduleName"')
		sectSelect.append('mgrouppolicy.taskidentifier_modulename as "moduleName"')
		sectGroup.append('"moduleName"')
		sectOrder.append('"moduleName" ASC')
	if "taskKey" in resultType:
		sectDistinct.append('"taskKey"')
		sectSelect.append("""replace(magenttask.entity_key, mgrouppolicy_agent_list.agent_name || '.' || mgrouppolicy.taskidentifier_modulename || '.','') as "taskKey" """ )
		#sectSelect.append("""reverse(split_part(reverse(magenttask.entity_key),'.',1)) as "taskKey" """)
		sectGroup.append('"taskKey"')
		sectOrder.append('"taskKey" ASC')
	if "parameterName" in resultType:
		sectDistinct.append('"parameterName"')
		sectSelect.append('mgrouppolicy.parameteridentifier_name as "parameterName"')
		sectGroup.append('"parameterName"')
		sectOrder.append('"parameterName" ASC')
	if "channel" in resultType:
		sectDistinct.append('"chanelName"')
		sectSelect.append(allowedChannelSQL+' as "chanelName"')
		#sectSelect.append("""array_to_string(regexp_matches(mgrouppolicy.taskidentifier_taskname,'{0}'),'') as "chanelName" """.format(allowedChannelNames))
		#sectSelect.append("""split_part(mgrouppolicy.taskidentifier_taskname,'~~',1) as "chanelName" """)
		sectGroup.append('mgrouppolicy.taskidentifier_taskname')
		sectOrder.append('"chanelName" ASC')
	if "taskName" in resultType:
		sectDistinct.append('"taskName"')
		sectSelect.append('mgrouppolicy.taskidentifier_taskname as "taskName"')
		sectGroup.append('"taskName"')
		sectOrder.append('"taskName" ASC')

	sectDistinct=",".join(sectDistinct)
	sectSelect=",".join(sectSelect)
	sectGroup=",".join(sectGroup)
	sectOrder=",".join(sectOrder)

	sectWhere=""
	if len(agentList)>0:
		sectWhere+=" AND mgrouppolicy_agent_list.agent_name in {0}".format(tupleToString(agentList))
	if len(channelList)>0:
		#sectWhere+=""" AND split_part(mgrouppolicy.taskidentifier_taskname,'~~',1) in {0}""".format(tupleToString(channelList))		
		sectWhere+=""" AND {0} in {1}""".format(allowedChannelSQL,tupleToString(channelList))
	if len(moduleList)>0:
		sectWhere+=" AND mgrouppolicy.taskidentifier_modulename in {0}".format(tupleToString(moduleList))

	sqlString="""
	   	SELECT 
		DISTINCT ON({0})
		{1}
		FROM 
		  qos.mgrouppolicy, 
		  qos.mgrouppolicy_agent_list, 
		  qos.magenttask
		WHERE 
		  {5} IS NOT NULL AND  --только задачи, из которых получилось вытащить имя каналп regexом
		  magenttask.entity_key like mgrouppolicy_agent_list.agent_name || '%' AND
		  magenttask.entity_key like '%'  || mgrouppolicy.taskidentifier_modulename || '%' AND
		  mgrouppolicy_agent_list.id = mgrouppolicy.id AND
		  magenttask.displayname = mgrouppolicy.taskidentifier_taskname AND
		  mgrouppolicy.state = 'ACTIVE' AND 
		  magenttask.deleted != TRUE AND 
		  magenttask.disabled != TRUE
		  {2}
		GROUP BY
			{3}
		ORDER BY
			{4}
		--LIMIT 200  ;
		""".format(sectDistinct,sectSelect,sectWhere,sectGroup,sectOrder,allowedChannelSQL)

	data=[]
	try:
	    cur = conn.cursor()
	    cur.execute(sqlString)
	    data = cur.fetchall()
	except psycopg2.Error as err:
		raise Exception("Query error: {}".format(err)+sqlString)
	
	#преобразуем tuple в строку
	if len(resultType)==1:
		l=len(data)
		for i in range(l):
			data[i]=data[i][0]
			
	return data

#********************************************
#********************************************
#********************************************
if __name__ == "__main__":
	res=query()
	channelList=('VLG.L73.013.SD4.TA1','VLG.L73.014.SD4.TA1','VLG.L73.015.SD4.TA1')
	for i in res:
		print(i)
