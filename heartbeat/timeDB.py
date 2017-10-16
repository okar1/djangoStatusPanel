
#threadPoll.pollResult examples:

# no value. Error.
# [{
#   'error': 'demo: Хост1: Обнаруженоошибок: 1из1',
#   'pollServer': 'demo',
#   'progress': 100,
#   'data': [{
#       'unit': '%',
#       'error': 'задачанеприсылаетданные',
#       'style': 'rem',
#       'agentKey': 'host1',
#       'enabled': True,
#       'id': 'demo.host1.1-1.someKey1',
#       'itemName': 'ЗагрузкаЦП'
#   }],
#   'id': 'demo.Хост1',
#   'name': 'Хост1'
# }]


# no value. No error
# [{
#   'id': 'demo.Хост1',
#   'pollServer': 'demo',
#   'data': [{
#       'unit': '%',
#       'style': 'ign',
#       'agentKey': 'host1',
#       'enabled': True,
#       'id': 'demo.host1.1-1.someKey1',
#       'itemName': 'ЗагрузкаЦП'
#   }],
#   'name': 'Хост1'
# }]


# no error. value.
# [{
#   'pollServer': 'demo',
#   'name': 'Хост1',
#   'data': [{
#       'unit': '%',
#       'value': 24.976688385009766,
#       'timeStamp': '20171009065334',
#       'agentKey': 'host1',
#       'enabled': True,
#       'itemName': 'ЗагрузкаЦП',
#       'id': 'demo.host1.1-1.someKey1./intelcpu/0/load/0'
#   }],
#   'id': 'demo.Хост1'
# }]

# indlux developers: We recommend writing points in batches of 5,000 to 10,000 points. Smaller batches, and more HTTP requests, will result in sub-optimal performance.
# influx timestamp like 1465839830100400200

# influxDB line format escaping rules:
#   measure name - escape commas "," and spaces " "
#   tag keys, tag values, and field keys - escape commas ",", spaces " ", equal "="
#   string field values - double quotes "
#   string "time" cannot be a field key or tag key

import requests
import calendar

pointsPerRequest=2000

# post pollResult to influxDB
# timeDbConfig like [{'httpUrl': 'http://localhost:1086/write?db=rfc'}]
def commitPollResult(timeDbConfig,pollResult,errors):
    # print("**** ", pollResult)
    urlList=[item.get('httpUrl',None) for item in timeDbConfig if type(item)==dict]
    urlList=[u for u in urlList if u is not None and u!='']
    reqData=[]
    if not urlList:
        return    

    # insert \ before sumbol in symbolToScreen list
    def doScreening(data,symbolsToScreen):
        t=type(data)
        if t==str:
            for s in symbolsToScreen:
                data=data.replace(s, '\\' + s)
        elif t==int or t==float:
            pass
        elif t==bool or t==type(None):
            data=None
        else:
            raise Exception("тип данных "+str(t)+" не поддерживается")
        return data

    # collect some amount of lines and sends them in single request.
    # Requests are sending to every url in urllist
    def sendMeasurement(mId,tags,values,timestamp,flushData=False):

        # print("-----",mId,tags,values,timestamp,flushData)
        nonlocal reqData

        if not flushData:
            mId=doScreening(mId,[","," "])
            tags={k:doScreening(v,[","," ","="]) for k,v in tags.items()}
            # filter for None and empty strings
            tags={k:v for k,v in tags.items() if v is not None and v!=''}

            values={k:doScreening(v,['"']) for k,v in values.items()}
            # filter for None and empty strings
            values={k:v for k,v in values.items() if v is not None and v!=''}

            # if all values are empty - not send anything
            if (mId is not None) and values:
                #curline is like demo.host1.1-1.someKey1.intelcpu.0.load.0,server=demo,boxname=Хост\ 1,itemname=Загрузка\ ЦП,unit=% value=1e-16
                curLine= \
                    mId + \
                    ''.join(["," + k + "=" + v for k,v in tags.items()]) + \
                    ' ' + \
                    ','.join([k + "=" + 
                        ('"' if type(v)==str else '') + 
                        str(v) + 
                        ('"' if type(v)==str else '') 
                        for k,v in values.items()]) + \
                    ((" "+str(timestamp)) if timestamp is not None else '')
                # print(timestamp)
                # print(curLine)
                reqData+=[curLine]
        #end if

        if (len(reqData)>=pointsPerRequest) or flushData:
            reqBody=('\n'.join(reqData)).encode("UTF-8")
            reqData=[]
            for url in urlList:
                try:
                    r=requests.post(url,reqBody)
                except Exception as e:
                    errors.add(str(e))
                else:
                    if r.status_code!=204:
                        errors.add(str(r.status_code)+" "+r.text )
        #end if 
    # end function

    for host in pollResult:
        
        hostId="host:"+host['id']
        hostName = host['name']
        serverName=host['pollServer']
        hostTags={
            'name':hostName,
            'server':serverName,
            }
        hostValues={
            'errorpercent' : host.get('progress',None)
        }
        sendMeasurement(hostId,hostTags,hostValues,None)

        items=host['data']
        for item in items:
            if item.get('enabled',True):
                itemId="item:"+item['id']
                itemTags={
                    'server':serverName,
                    'host':hostName,
                    'hostkey':item.get('agentKey',None),
                    'name':item.get('itemName',None),
                    'unit':item.get('unit',None)
                }

                error=item.get('error',None)

                itemTimeStamp=item.get('timeStamp',None)
                if itemTimeStamp is not None:
                    # convert from UTC datetime to UTC timestamp
                    itemTimeStamp=calendar.timegm(itemTimeStamp.timetuple())
                    #timestamp() returns 10-digit float. InxluxDB requires 19-digit integer
                    itemTimeStamp=int(itemTimeStamp*1000000000)
                    # itemTimeStamp=int(itemTimeStamp.timestamp()*1000000000)

                if  (error is not None) or (itemTimeStamp is None):
                    # in any strange situation - not publish values to DB.
                    # publish errors only
                    value=None
                else:
                    value=item.get('value',None)

                # allowed value types to write in db
                if type(value) not in [int,float,bool]:
                    value=None

                itemValues={'value':value,'error':error}
                sendMeasurement(itemId,itemTags,itemValues,itemTimeStamp)
        #endfor task
    #endfor host
    sendMeasurement(None,None,None,None,flushData=True)

    # errors.add("Преведтдт")
