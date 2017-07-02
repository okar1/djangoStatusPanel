# -*- coding: utf-8 -*-
import sys
sys.path.append('..')
from qsettings.rcaDataSource import query
import json
from transliterate import translit
from CustomDict import CustomDict,CustomEncoder


#aggregation level indices (random numbers)
itmplParameter,itmplTask,itmplModule,itmplAgent,itmplChannel,itmplLevel=22,15,18,100,333,58
#setting indices (random numbers)
iNameLabel,iDataLabel,iNeedPrio,iVirtual,iTopLevelTemplate,iOutputFile=23,654,765,354,86,32

#settings for result file
s={
	#
	itmplParameter:{
		#name key in json
		iNameLabel:"name",
		#write priority flag to json
		iNeedPrio:True,
		#not affect to json data, only to grouping key
		iVirtual:False,
	},
	itmplTask:{
		iNameLabel:"name",
		iDataLabel:"parameters",
		iNeedPrio:False,
		iVirtual:False,
		#json jeader if iTmplTask selected for grouping
		iTopLevelTemplate:{"originatorid":"lalala","originatorid2":"lalala2"},
		#file name
		iOutputFile: "byTask",
	},		
	itmplModule:{
		iNameLabel:"name",
		iDataLabel:"tasks",
		iNeedPrio:False,
		iVirtual:False,
		iTopLevelTemplate:{"originatorid":"lalala","originatorid2":"lalala2"},
		iOutputFile: "byModule",
	},		
	itmplAgent:{
		iNameLabel:"key",
		iDataLabel:"modules",
		iNeedPrio:False,
		iVirtual:False,
		iTopLevelTemplate:{"originatorid":"age","originatorid2":"age"},
		iOutputFile: "byAgent",
	},		
	itmplChannel:{
		iVirtual:True,
		iTopLevelTemplate:{"originatorid":"ch","originatorid2":"ch"},
		iOutputFile: "byChannel",
	},
	itmplLevel:{
		iNameLabel:"name",
		iDataLabel:"agents",
		iNeedPrio:True,
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
			elif key=='priority':
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
#called for every db line. Writes result file
def makeFiles(
	#len pOrder==len(pData)+1. Last is level definition
	#order of parameters from low to high (const at every run)
	pOrder=[itmplParameter,itmplTask,itmplAgent,itmplChannel,itmplLevel],
	#current data from db
	pData=["par","task","age","chan"],
	#end collect data and write file
	isLastStep=False,
	#index of param for grouping data in result file
	groupIndices=[itmplAgent]
	):
	
	if not hasattr(makeFiles, "res"):
		makeFiles.res = [CustomDict() for i in range(len(groupIndices))]

	# got data to merge
	if True:
		curData=''
		for i in range(len(pOrder)-1):

			if not s[pOrder[i]][iVirtual]:
				if i==0:
					tmp={s[pOrder[i]][iNameLabel] : pData[i]}
				else:
					tmp={s[pOrder[i]][iNameLabel]: pData[i], s[pOrder[i]][iDataLabel]:curData}

				if s[pOrder[i]][iNeedPrio]:
					tmp.update({"priority":1})

				curData=CustomDict()
				curData[pData[i]]=tmp
			#endif	
	#endif

	#every gi produces 1 result file
	for gi in range(len(groupIndices)):

		#calc group key and keys of every level
		if True:
			keys=['' for i in range(len(pOrder)-1)]
			key=[]
			giPos=-1
			for i in range(len(pOrder)-2,-1,-1):
				curParam=pOrder[i]
				key.append(pData[i])
				keys[i]=".".join(key)
				if curParam==groupIndices[gi]:
					giPos=i
					break
			if giPos==-1:
				raise("groupIndex {0} not in order list".format(gi))
		#endif
		key=keys[giPos]

		#curdata & key --> leveldata 
		if True:
			last=len(pOrder)-1
			tmp={s[pOrder[last]][iNameLabel]: key, s[pOrder[last]][iDataLabel]:curData}		
			if s[pOrder[last]][iNeedPrio]:
				tmp.update({"priority":1})
			levelData=CustomDict()
			levelData[key]=tmp
		#endif

		#merge level data with result in prev iterations			
		res=makeFiles.res[gi]
		mergeDict(res,levelData)

		if isLastStep:
			#add top level template to result
			res={"levels":res}
			res.update(s[groupIndices[gi]][iTopLevelTemplate])

			#save result to file
			with open(s[groupIndices[gi]][iOutputFile], 'w', encoding="utf8") as f:
			  json.dump(res, f, cls=CustomEncoder, ensure_ascii=False, indent=True)

			#reset var
			makeFiles.res[gi]=CustomDict()

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
def do_makefiles(db,path,detailLevels=("agent","channel","module","task","all")):

	#db query must be gr as: agent->module->task->parameter
	# expected indices of columns in db table
	iAgent=0
	iModule=1
	iTask=2
	iParameter=3
	iChannel=4

	isLastStep=False
	for curLine in range(len(db)):
		if curLine==len(db)-1:
			isLastStep=True
		cursor=db[curLine]
		
		# channel=_do_channelKeyName(cursor[iChannel])
		makeFiles(
			pOrder=[itmplParameter,itmplTask,itmplModule,itmplAgent,itmplLevel],
			pData=[cursor[iParameter],cursor[iTask],cursor[iModule],cursor[iAgent],],
			isLastStep=isLastStep,
			groupIndices=[itmplAgent]
		)
	#endfor db	
#********************************************
#********************************************
#********************************************
if __name__ == "__main__":
	q=query()
	# for i in q:
	# 	print(i)
	path="d:\\files\\"
	do_makefiles(q,path)
	#makefiles()
