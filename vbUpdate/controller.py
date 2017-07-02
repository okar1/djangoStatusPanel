# -*- coding: utf-8 -*-
from pysnmp.hlapi import *
import re,os
from shared.ssh import RemoteHost

def askSingleBridge(currentTask,opt=[],pollResult={},channelCount={}):
	#print(currentTask)
	#    [{'name': 'VLG-JOLA-RF', 'controlblock_id': 2, 'ip': '10.66.65.1', 'id': 2, 'chcount': 0}]
	#print(opt)
	#   {'taskFileBackup': '/opt/qligent/vision/Conf/XMLCFG/config.backup.xml',
	#    'nameReplaceRules': {'+': '_'},
	#    'channelIpOID': '.1.3.6.1.4.1.24562.510.1.3',
	#    'channelNameOID': '.1.3.6.1.4.1.24562.510.1.2',
	#    'taskFile': '/opt/qligent/vision/Conf/XMLCFG/config.xml',
	#    'channelPortOID': '.1.3.6.1.4.1.24562.510.1.4',
	#    'blockedChannelNames': ['TST', '\\.SW\\.', 'TEST', '\\(', '\\)', '.{30,}'],
	#    'startReplaceMarker': '<!-- Начало автозамены задач VB -->\n',
	#    'snmpReadCommunity': 'teoco',
	#    'endReplaceMarker': '\n<!-- Конец автозамены задач VB -->',
	#    'channelTableOID': '.1.3.6.1.4.1.24562.510',
	#    'channelIndexOID': '.1.3.6.1.4.1.24562.510.1.1'}

	
	#snmp get table from device
	channelIndexOID=opt["channelIndexOID"]
	channelNameOID=opt["channelNameOID"]
	channelIpOID=opt["channelIpOID"]
	channelPortOID=opt["channelPortOID"]
	snmpReadCommunity=opt["snmpReadCommunity"]
	blockedChannelNames=opt["blockedChannelNames"]
	nameReplaceRules=opt["nameReplaceRules"]

	# vbName=currentTask["name"]
	vbIP=currentTask["ip"]
	oldChannelCount=currentTask['chcount']

	# channels count that was requested early. Used for progressbar calculation
	if oldChannelCount==0:
		oldChannelCount=600

	oldChannelCount*=1.1

	if "error" in currentTask.keys():
		del currentTask["error"]

	# print("Опрос {0} ({1}):".format(vbName,vbIP))

	currentTask['progress']=10
	vbChannels=[]
	i=0
	for (errorIndication,
		  errorStatus,
		  errorIndex,
		  varBinds) in nextCmd(SnmpEngine(),
							  CommunityData(snmpReadCommunity, mpModel=0),
							  UdpTransportTarget((vbIP, 161)),
							  ContextData(),
							  ObjectType(ObjectIdentity(channelIndexOID)),
							  ObjectType(ObjectIdentity(channelNameOID)),
							  ObjectType(ObjectIdentity(channelIpOID)),
							  ObjectType(ObjectIdentity(channelPortOID)),
							  lexicographicMode=False):
		if errorIndication:
			currentTask['error']="Ошибка SNMP: "+str(errorIndication)
			# print(currentTask['error'])
			break
		elif errorStatus:
			currentTask['error']=('%s at %s' % (errorStatus.prettyPrint(),
						  errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
			# print(currentTask['error'])
			break
		else:
			index=varBinds[0][1].prettyPrint()
			name=varBinds[1][1].prettyPrint()
			ip=varBinds[2][1].prettyPrint()
			port=varBinds[3][1].prettyPrint()

			#replacing symbols in name
			for rule in nameReplaceRules:
				name=name.replace(rule,nameReplaceRules[rule])

			#check channel name is blocked
			nameIsBlocked=False
			for pattern in blockedChannelNames:
				if name=="" or re.search(pattern,name)!=None:
					nameIsBlocked=True
					break

			if not nameIsBlocked:
				vbChannels.append({'index':index,'name':name,'ip':ip,'port':port})

		#progressbar
		i+=1
		if i%10==0:
			currentTask['progress']=10+int((i*90/oldChannelCount)%90) 
	else:
		#if no break
		currentTask['progress']=100
	#endfor (snmp)

	if "error" not in currentTask.keys():
		vbId=currentTask["id"]
		pollResult[vbId]=vbChannels
		channelCount[vbId]=i

	return currentTask

#compare oldlist and newlist, (for GUI usability only) return compare results:
#1) removed channels first, add "style":"rem" tag
#2) added channels then, add "style":"add" tag
#3) same channels last, no "style" tag
def compareChanelLists(oldChList,newChList):
	# print(oldChList)
	# print(newChList)

	chFullName=lambda ch: "{0} {1}:{2} (index {3})".format(ch["name"],ch["ip"],ch["port"],ch["index"])

	def update2(d,v):
		# v=v.copy()
		d.update(v)
		d["name"]=chFullName(d)
		return d

	oldChDict={chFullName(item):item.copy() for item in oldChList}
	newChDict={chFullName(item):item.copy() for item in newChList}

	removedChannels={k:update2(v,{"style":"rem"}) for k,v in oldChDict.items() if k not in newChDict.keys()}
	addedChannels={k:update2(v,{"style":"add"}) for k,v in newChDict.items() if k not in oldChDict.keys()}
	noChangeChannels={k:update2(v,{"style":""}) for k,v in oldChDict.items() if k in newChDict.keys()}

	return sorted(removedChannels.values(),key=chFullName)+\
			sorted(addedChannels.values(),key=chFullName)+\
			sorted(noChangeChannels.values(),key=chFullName)


#task = {"cbName":,"cbIp":,"data":,"cb":,"fake":}
#task["data"] = [{"chName": ,"chIp":, "chPort": , "chIndex": ,"vbName" , "vbIp": }]
def saveCBFile(task,opt=[]):
	
	if task["fake"]==True:
		return task

	data=task["data"]
	task['progress'].set(10)

	#generate task text
	fullTaskText=""
	for ch in data:
	  fullTaskText+="""<Task id="{vbName} {chName}" name="{vbName} {chName} ({chIp}:{chPort})"><include_preset name="VBPreset"/><index>{chIndex}</index><device>/snmp/{vbIp}</device></Task>\n""".format(
	    chName=ch["chName"],
	    chIp=ch["chIp"],
	    chPort=ch["chPort"],
	    chIndex=ch["chIndex"],
	    vbName=ch["vbName"],
	    vbIp=ch["vbIp"])
	  
	cbIp=task["cbIp"]
	startReplaceMarker=opt["startReplaceMarker"]
	endReplaceMarker=opt["endReplaceMarker"]
	taskFile=opt["taskFile"]
	taskFileBackup=opt["taskFileBackup"]
	sshUser=opt["sshUser"]

	host=RemoteHost(sshUser+"@"+cbIp)	

	task['progress'].set(20)
	e = host.allowAccess("qligent","qligent",os.path.dirname(taskFile))
	if e is not None:
		task['error'].set('1 '+e)
		return task

	task['progress'].set(30)
	e = host.allowAccess("qligent","qligent",taskFile)
	if e is not None:
		task['error'].set('2 '+e)
		return task

	task['progress'].set(40)
	host.allowAccess("qligent","qligent",taskFileBackup)

	#load taskfiletext via ssh
	task['progress'].set(50)
	taskFileText, e = host.readFile(taskFile)
	if e is not None:
		task['error'].set('3 '+e)
		return task

	#find markers in taskfiletext
	startMarkerPos=taskFileText.find(startReplaceMarker)+len(startReplaceMarker)
	endMarkerPos=taskFileText.find(endReplaceMarker)
	if startMarkerPos==-1 or endMarkerPos==-1 or endMarkerPos<(startMarkerPos-1):
		e="Не указаны границы блока задач VB в файле задания "+taskFile+" "
		if startMarkerPos==-1:
			e+=startReplaceMarker
		if endMarkerPos==-1:
			e+=endReplaceMarker
		task['error'].set('4 '+e)
		return task

	#replace text inside markers with fulltasktext
	taskFileText=taskFileText[:startMarkerPos]+fullTaskText+taskFileText[endMarkerPos:]

	#backup taskfile
	task['progress'].set(60)
	e=host.moveFile(taskFile,taskFileBackup)
	if e is not None:
		task['error'].set('5 '+e)
		return task

	#write new task text
	task['progress'].set(70)
	e=host.writeFile(taskFile,taskFileText)
	if e is not None:
		task['error'].set('6 '+e)
		return task

	#set default permissions to file
	task['progress'].set(80)
	e = host.allowAccess("qligent","qligent",os.path.dirname(taskFile))
	if e is not None:
		task['error'].set('7 '+e)
		return task

	#restart vision service
	task['progress'].set(90)
	e=host.restartDaemon("vision")
	if e is not None:
		task['error'].set('8 '+e)
		return task

	task['progress'].set(100)
	return task