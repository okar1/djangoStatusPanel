#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Для работы скрипта необходим Python3 с модулем pysnmp

# настройки, общие для всех БК

channelTableOID=".1.3.6.1.4.1.24562.510"
channelIndexOID=channelTableOID+".1.1"
channelNameOID=channelTableOID+".1.2"
channelIpOID=channelTableOID+".1.3"
channelPortOID=channelTableOID+".1.4"
snmpReadCommunity="teoco"
taskFile="""/opt/qligent/vision/Conf/XMLCFG/config.xml"""
taskFileBackup="""/opt/qligent/vision/Conf/XMLCFG/config.backup.xml"""
blockedChannelNames=["TST","\.SW\.","TEST","\(","\)",".{30,}"]
nameReplaceRules={'+':'_'}
startReplaceMarker=r"""<!-- Начало автозамены задач VB -->\n"""
endReplaceMarker=r"""\n<!-- Конец автозамены задач VB -->"""

# **************************************************************************
from pysnmp.hlapi import *
import sys,os,re,json,os.path

#проверяем права доступа к файлам
if not os.access(os.path.dirname(taskFile), os.W_OK):
	print("Нет доступа для записи к каталогу конфигурации: ",os.path.dirname(taskFile))
	sys.exit()

if not os.path.isfile(taskFile):
  print("Файл задания не найден: "+taskFile)
  sys.exit()
else:
	if not os.access(taskFile, os.W_OK):
		print("Нет доступа для записи к файлу конфигурации: ",taskfile)
		sys.exit()

if os.path.isfile(taskFileBackup):
	if not os.access(taskFileBackup, os.W_OK):
		print("Нет доступа для записи к резервному файлу конфигурации: ",taskFileBackup)
		sys.exit()

#загружаем настройки
optFile=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
try:
  with open(optFile, 'r', encoding="utf8") as intfile:
     opt=json.load(intfile)
except FileNotFoundError as e:
   print ("Файл настроек не найден "+str(e))
   sys.exit()  
else:
  pass
finally:
  pass

if True:
  #опрос устройств по SNMP
  newVBlist=[] 
  for vbName in opt["videoBridges"].keys():
    vbIP=opt["videoBridges"][vbName]["ip"]
    print("Опрос {0} ({1}):".format(vbName,vbIP))
    vbChannels=[]
    i=0
    progressString=""
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
            print("Ошибка SNMP: "+str(errorIndication))
            sys.exit()
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                                errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
            sys.exit()
        else:
            vbChannels.append({'index':varBinds[0][1].prettyPrint(),
                                'name':varBinds[1][1].prettyPrint(),
                                'ip':varBinds[2][1].prettyPrint(),
                                'port':varBinds[3][1].prettyPrint(),
              })
        #прогрессбар
        i+=1
        if (i%10==0):
          if len(progressString)>60:
            progressString=""
            print()
          progressString+="*"
          print(progressString,end='\r')    
    #endfor (snmp)
    newVBlist.append({"name":vbName,"ip":vbIP,"channels":vbChannels})
    print()
  #endfor (vb)  
else:
  #debug mode - берем результат из файла без проведения опроса
  newVBFile=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'debug.json')
  try:
    with open(newVBFile, 'r', encoding="utf8") as intfile:
       newVBlist=json.load(intfile)
  except FileNotFoundError as e:
     print ("Файл настроек не найден "+str(e))
     sys.exit()  
  else:
    pass
  finally:
    pass        

#анализируем полученный список каналов, сравниваем его с прошлым сканом
oldVBDict=opt["videoBridges"]
newTaskList=[]
fullTaskText=""
for vb in newVBlist:
  vbName=vb["name"]
  print ("VideoBridge: "+vbName)
  
  if (vbName in oldVBDict):
    oldVB=oldVBDict[vbName]
  else:
    oldVB={"channels":{}}  

  newNames=[]
  removedNames=[]
  filteredNames=[]
  changedNames=[]
  dublicateNames=[]

  if "channels" not in oldVB:
    oldVB["channels"]={}

  oldChDict=oldVB["channels"]
  newChDict={}
  for ch in vb["channels"]:
    name=ch["name"].strip()
    ip=ch["ip"].strip()
    port=ch["port"].strip()
    index=ch["index"].strip()

    for rule in nameReplaceRules:
    	name=name.replace(rule,nameReplaceRules[rule])

    nameIsBlocked=False
    for pattern in blockedChannelNames:
    	if re.search(pattern,name)!=None:
    		nameIsBlocked=True
    		break

    #имя канала не подходит (пустое)
    if (name=="" or ip=="0.0.0.0"):
      pass
    #имя канала не подходит по regex
    elif nameIsBlocked:
      filteredNames.append(name)  
    #имя канала не подходит (повторяется)
    elif name in newChDict:
      dublicateNames.append(name)  
    else:
      #добавляем канал в новый ФЗ
      newChDict[name]={"ip":ip,"port":port,"index":index}

      #только для статистики: уточняем, канал новый/нет, поменялся/нет
      if name not in oldChDict:
        newNames.append(name)
      else:
        oldCh=oldChDict[name]
        if (ip!=oldCh["ip"] or port!=oldCh["port"] or index!=oldCh["index"]):
          changedNames.append(name)
        else:
          #канал не изменился
          pass
  #endfor (ch)
  #тут у нас есть newChDict каналов для нового ФЗ   

  #только для статистики: проверяем какие каналы былы удалены
  for name in oldChDict.keys():
    if ((name!="") and (name not in newChDict)):
      removedNames.append(name)

  #выводим статистику
  print("******Статистика по "+vbName)
  
  if (len(removedNames)>0):
    print ("***Удаленные каналы: {0} шт".format(len(removedNames)))
    print("\n".join(sorted(removedNames)))
  
  if (len(dublicateNames)>0):
    print ("***Каналы с повторяющимися именами: {0} шт".format(len(dublicateNames)))
    print("\n".join(sorted(dublicateNames)))
  
  if (len(filteredNames)>0):
    print ("***Каналы с неподходящим именем: {0} шт".format(len(filteredNames)))
    print("\n".join(sorted(filteredNames)))
  
  print ("***Новые каналы: {0} шт".format(len(newNames)))
  if (len(newNames)>0):
    print("\n".join(sorted(newNames)))
  
  print ("***Каналы, изменившиеся с момента прошлой проверки: {0} шт".format(len(changedNames)))
  if (len(changedNames)>0):
    print("\n".join(sorted(changedNames)))

  print("***Задач на {1} для записи в файл задания: {0} шт ".format(len(newChDict),vbName))
  #print("\n".join(sorted(newChDict)))  


  #сохраняем новый список каналов в настройках
  opt["videoBridges"][vbName]["channels"]=newChDict

  #генерим текст тасок для ФЗ
  taskText={}
  for ch in newChDict.keys():
    index=newChDict[ch]["index"]
    curTaskText="""<Task id="{vbName} {chName}" name="{vbName} {chName} ({chIp}:{chPort})"><include_preset name="VBPreset"/><index>{chIndex}</index><device>/snmp/{vbIp}</device></Task>\n""".format(
      chName=ch,
      chIp=newChDict[ch]["ip"],
      chPort=newChDict[ch]["port"],
      chIndex=index,
      vbName=vb["name"],
      vbIp=vb["ip"])
    taskText[int(index)]=curTaskText

  #сортируем текст для ФЗ
  for key in sorted(taskText.keys()):
    fullTaskText+=taskText[key]
#endfor (vb)

input("Нажмите Enter для подтверждения или CTRL+C для отмены\n") 

#читаем ФЗ
if not os.path.isfile(taskFile):
  print("Файл задания не найден: "+taskFile)
  sys.exit()
else:
  try:
    with open(taskFile, "r", encoding="utf8") as f:
      taskFileText=f.read()
  except e:
    print("Ошибка при чтении файла задания: "+taskFile)
    sys.exit()

#ищем маркеры в файле
startMarkerPos=taskFileText.find(startReplaceMarker)+len(startReplaceMarker)
endMarkerPos=taskFileText.find(endReplaceMarker)
if startMarkerPos==-1 or endMarkerPos==-1 or endMarkerPos<(startMarkerPos-1):
  print("Не указаны границы блока задач VB в файле задания "+taskFile)
  if startMarkerPos==-1:
    print(startReplaceMarker)
  if endMarkerPos==-1:
    print(endReplaceMarker)
  sys.exit()  

#заменяем задачи в ФЗ
taskFileText=taskFileText[:startMarkerPos]+fullTaskText+taskFileText[endMarkerPos:]

#бэкап старого ФЗ
if os.path.isfile(taskFileBackup):
  try:
    os.remove(taskFileBackup)
  except e:
    print("Ошибка при удалении резервной копии файла: "+str(e))
    sys.exit()

try:
  os.rename(taskFile,taskFileBackup)
except e:
  print("Ошибка при создании резервной копии файла: "+str(e))
  sys.exit()

#сохраняем ФЗ
try:
    with open(taskFile, "w", encoding="utf8") as f:
        f.write(taskFileText)
except e:
    print("Ошибка при сохранении файла задания: "+str(e))
    sys.exit()
	
print ("Новый файл задания записан: "+taskFile)

#сохраняем список каналов для сравнения при следующем сканировании
try:
  with open(optFile, 'w', encoding="utf8") as f:
    json.dump(opt, f, ensure_ascii=False, indent=True)
except e:
   print ("Ошибка сохранения настроек "+str(e))
   sys.exit()  

#перезапуск vision
print("Перезапускаю службу БК:")
print(os.system('sudo bash -c "systemctl restart vision"'))