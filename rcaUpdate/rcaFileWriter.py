# -*- coding: utf-8 -*-
import sys
sys.path.append('..')
from rcaDataSource import query
import json
from transliterate import translit
from CustomDict import CustomDict,CustomEncoder


#aggregation level indices (random numbers)
itmplParameter,itmplTask,itmplModule,itmplAgent,itmplLevel,itmplGroup=22,15,18,100,333,58
#setting indices (random numbers)
iNameLabel,iDataLabel,iNeedPrio=23,654,765

#settings for result file
s={
	#
	itmplParameter:{
		#name key in json
		iNameLabel:"name",
		#write priority flag to json
		iNeedPrio:True,
	},
	itmplTask:{
		iNameLabel:"name",
		iDataLabel:"parameters",
		iNeedPrio:False,
	},		
	itmplModule:{
		iNameLabel:"name",
		iDataLabel:"tasks",
		iNeedPrio:False,
	},		
	itmplAgent:{
		iNameLabel:"key",
		iDataLabel:"modules",
		iNeedPrio:False,
	},		
	itmplLevel:{
		iNameLabel:"name",
		iDataLabel:"agents",
		iNeedPrio:True,
	},		
	itmplGroup:{
		iNameLabel:"name",
		iDataLabel:"levels",
		iNeedPrio:False,
	},

}

#merge b into a
def mergeDict(a,b,path=None,dictLen=0):
	
	if path is None: path = []
	for key in b.keys():

		if key in a.keys():
			if ((isinstance(a[key], CustomDict) or (isinstance(a[key], dict))) and  \
				(isinstance(b[key], CustomDict) or isinstance(b[key], dict))):
				#resursive call
				mergeDict(a[key], b[key], path=path+[str(key)], dictLen=len(a))
			elif isinstance(a[key], list) and isinstance(b[key], list):
				a[key].append(b[key])
			elif a[key] == b[key]:
				pass # same leaf value
			elif key=='priority' or key=='taskKey':
				pass
			else:
				print(a)
				print(b)
				raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
		else:
			a[key] = b[key]
			if 'priority' in b[key].keys():
				b[key]['priority']=len(a)
	#endfor
#********************************************
#********************************************
#********************************************
#called for every db line. Collects vResult structure
def processDbRow(
	# list of aggregation level indices, from low (parameter) to high (group)
	pOrder,
	#list of data of current DB row for every pOrder item
	pNames
	):

	rowData=None
	for i in range(len(pOrder)):

		curName=pNames[i]
		curIndex=pOrder[i]
		curSettings=s[curIndex]

		# first item not contain data, only name
		if rowData is None:
			tmp={curSettings[iNameLabel] : curName}
		else:
			tmp={curSettings[iNameLabel] : curName, curSettings[iDataLabel] : rowData}

		if curSettings[iNeedPrio]:
			tmp.update({"priority":1})

		rowData=CustomDict()
		rowData[curName]=tmp

	return rowData

#********************************************
#********************************************
#********************************************    
def _do_channelKeyName(rusName):
	s=translit(rusName, 'ru',reversed=True)
	#это не пробел
	s=s.replace(" ","")
	#это пробел
	s=s.replace(" ","")
	s=s.replace("+","")
	s=s.replace("'","")
	s=s.replace("-","")
	s=s.replace(".","")
	s=s.replace("(","")
	s=s.replace(")","")
	s=s.replace("!","")
	s=s.replace(":","")
	s=s.replace(r"/","")
	s=s.lower()
	return(s)
#********************************************
#********************************************
#********************************************
#main sub. Read DB and call writer
def makeFile(pAlertTypeTemplate,pChannelNameRegex,pRules,pOriginatorId,pRcaSeverity,pOutputFile):

	#db query must be gr as: agent->module->task->parameter
	# expected indices of columns in db table
	iAgent=0
	iModule=1
	iTask=2
	iParameter=3
	iChannel=4

	db=query(channelNameRegex=pChannelNameRegex)
	res = CustomDict()

	for curLine in range(len(db)):
		cursor=db[curLine]
		
		alertTypeKey=pAlertTypeTemplate
		if alertTypeKey.find("{channel}")!=-1:
			alertTypeKey=alertTypeKey.replace("{channel}",_do_channelKeyName(cursor[iChannel]))
		if alertTypeKey.find("{agent}")!=-1:
			alertTypeKey=alertTypeKey.replace("{agent}",cursor[iAgent])
		if alertTypeKey.find("{module}")!=-1:
			alertTypeKey=alertTypeKey.replace("{module}",cursor[iModule])
		if alertTypeKey.find("{task}")!=-1:
			alertTypeKey=alertTypeKey.replace("{task}",cursor[iTask])
		if alertTypeKey.find("{parameter}")!=-1:
			alertTypeKey=alertTypeKey.replace("{parameter}",cursor[iParameter])
		
		rowData=processDbRow(
			pOrder=[itmplParameter,itmplTask,itmplModule,itmplAgent,itmplLevel,itmplGroup],
			pNames=[cursor[iParameter],cursor[iTask],cursor[iModule],cursor[iAgent],alertTypeKey,alertTypeKey],
		)

		taskKey=cursor[iAgent]+"."+cursor[iModule]+"."+cursor[iTask]
		for item in rowData.values():
			item["taskKey"]=taskKey
		#merge common result with data of current row
		mergeDict(res,rowData)
	#endfor db	

	print ("following alertTypes was created:")
	for item in res.values():
		item['rules']=pRules
		item['originatorId']=pOriginatorId
		item['alertTypeName']="qos.RCA."+item['name']
		print(item['alertTypeName'])
		item['rcaSeverity']=pRcaSeverity
	print (len(res)," alertTypes total")
	res={"groups":res}

	#save result to file
	with open(pOutputFile, 'w', encoding="utf8") as f:
	  json.dump(res, f, cls=CustomEncoder, ensure_ascii=False, indent=True)

#********************************************
#********************************************
#********************************************
if __name__ == "__main__":
	#channelNameRegex для Рт - 5 групп по 3 символа через точку. В каждой группе буквы,цифры,+ или _. Первая группа - начинается с буквы
	#channelNameRegex=r"[a-zA-Z][a-zA-Z0-9_+]{2}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}\.[a-zA-Z0-9_+]{3}"
	# для бел "что то - имя канала - что то"
	channelNameRegex=r"^.*?- *(.*?) *-"

	# key for grouping tasks to rca groups
	# available fields: {agent} {channel} {module} {task} {parameter}
	alertTypeTemplate="{channel}.{module}.{parameter}"

	makeFile(
		pAlertTypeTemplate=alertTypeTemplate,
		pChannelNameRegex=channelNameRegex,
		pRules=["L2"],
		pOriginatorId=29681,
		pRcaSeverity=4,
		pOutputFile=r"d:\files\rs",)

	q=query(channelNameRegex=channelNameRegex)
	print(len(q), "db records with channelName detected")

	q=query()
	print(len(q), "db records total")


