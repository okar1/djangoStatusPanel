
#threadPoll.pollResult examples:

# no value. Error.
# [{
# 	'error': 'demo: Хост1: Обнаруженоошибок: 1из1',
# 	'pollServer': 'demo',
# 	'progress': 100,
# 	'data': [{
# 		'unit': '%',
# 		'error': 'задачанеприсылаетданные',
# 		'style': 'rem',
# 		'name': 'host1.1-1.someKey1(ЗагрузкаЦП): ошибка: задачанеприсылаетданные',
# 		'agentKey': 'host1',
# 		'enabled': True,
# 		'id': 'demo.host1.1-1.someKey1',
# 		'itemName': 'ЗагрузкаЦП'
# 	}],
# 	'id': 'demo.Хост1',
# 	'name': 'Хост1'
# }]


# no value. No error
# [{
# 	'id': 'demo.Хост1',
# 	'pollServer': 'demo',
# 	'data': [{
# 		'unit': '%',
# 		'style': 'ign',
# 		'name': 'host1.1-1.someKey1(ЗагрузкаЦП): задачанеприсылаетданные',
# 		'agentKey': 'host1',
# 		'enabled': True,
# 		'id': 'demo.host1.1-1.someKey1',
# 		'itemName': 'ЗагрузкаЦП'
# 	}],
# 	'name': 'Хост1'
# }]


# no error. value.
# [{
# 	'pollServer': 'demo',
# 	'name': 'Хост1',
# 	'data': [{
# 		'unit': '%',
# 		'value': 24.976688385009766,
# 		'name': 'host1.1-1.someKey1./intelcpu/0/load/0(ЗагрузкаЦП): 27секназадполученозначение24.976688385009766%',
# 		'timeStamp': '20171009065334',
# 		'agentKey': 'host1',
# 		'enabled': True,
# 		'itemName': 'ЗагрузкаЦП',
# 		'id': 'demo.host1.1-1.someKey1./intelcpu/0/load/0'
# 	}],
# 	'id': 'demo.Хост1'
# }]


# indlux developers: We recommend writing points in batches of 5,000 to 10,000 points. Smaller batches, and more HTTP requests, will result in sub-optimal performance.
# influx timestamp like 1465839830100400200

# influxDB line format escaping rules:
#   measure name - escape commas ","" and spaces " "
#   tag keys, tag values, and field keys - escape commas ",", spaces " ", equal "="
#   string field values - double quotes "
#   string "time" cannot be a field key or tag key

import requests
import time
from datetime import datetime

timeStampFormat="%Y%m%d%H%M%S"

# post pollResult to influxDB
# timeDbConfig like [{'httpUrl': 'http://localhost:1086/write?db=rfc'}]
def commitPollResult(timeDbConfig,pollResult)::
    
    urlList=[item.get('httpUrl',None) for item in timeDbConfig if type(item)==dict]
    urlList=[u for u in urlList if u is not None and u!='']

    if not urlList:
        return

    pointsPerRequest=2000
    
    curPoints=0
    body=""

    for box in pollResult:
        boxName=box['name']
        serverName=box['pollServer']
        errorPercent=box.get('progress',None)
        tasks=box['data']

        for task in tasks:
            timeStamp=task.get('timeStamp',None)
            if timeStamp is not None:
                timeStamp = datetime.strptime(timeStamp, timeStampFormat)
                #timestamp() returns 10-digit float. InxluxDB requires 19-digit integer
                timeStamp=int(timeStamp.timestamp()*1000000000)
id
enabled
itemName
agentKey
value
unit
error

    server=pollResult[]
    dataList=pollResult['data']
    for data in dataList

req="""
demo.host1.1-1.someKey1.intelcpu.0.load.0,server=demo,boxname=Хост\ 1,itemname=Загрузка\ ЦП,unit=% value=1e-16
demo.host1.1-1.someKey1.intelcpu.0.load.0,server=demo,boxname=Хост\ 1,itemname=Загрузка\ ЦП,unit=% error="ошибка 11"
demo.host2.1-1.someKey1.intelcpu.0.load.0,server=demo,boxname=Хост\ 2,itemname=Загрузка\ ЦП,unit=% value=21
demo.host2.1-1.someKey1.intelcpu.0.load.0,server=demo,boxname=Хост\ 2,itemname=Загрузка\ ЦП,unit=% error="ошибка 21"
demo.host1.1-1.someKey1.ram.0.load.0,server=demo,boxname=Хост\ 1,itemname=Загрузка\ RAM,unit=% value=31
demo.host1.1-1.someKey1.ram.0.load.0,server=demo,boxname=Хост\ 1,itemname=Загрузка\ RAM,unit=%,error=ошибка\ 31 isErrorNum=1
"""





url="http://localhost:8086/write?db=rfc"

while True:
	r=requests.post(url,req.encode("UTF-8"))
	print(r.status_code)
	print(r.text)
	# break
	time.sleep(5)