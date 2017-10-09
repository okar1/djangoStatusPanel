
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

import requests
import time

req="""
demo.host1.1-1.someKey1.intelcpu.0.load.0,server=demo,boxname=Хост\ 1,itemname=Загрузка\ ЦП,unit=% value=11
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