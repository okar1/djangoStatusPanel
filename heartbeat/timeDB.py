# threadPoll.pollResult examples:

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

# indlux developers:
#   We recommend writing points in batches of 5,000 to 10,000 points.
# Smaller batches, and more HTTP requests, result in sub-optimal performance.
# influx timestamp like 1465839830100400200

# influxDB line format escaping rules:
#   measure name - escape commas "," and spaces " "
#   tag keys, tag values, and field keys:
#       escape commas ",", spaces " ", equal "="
#   string field values - double quotes "
#   string "time" cannot be a field key or tag key

import requests
import calendar

pointsPerRequest = 2000

# post pollResult to influxDB
# timeDbConfig like [{'httpUrl': 'http://localhost:1086/write?db=rfc'}]


def commitPollResult(timeDbConfig, pollResult, errors):
    # print("**** ", pollResult)
    urlList = [item.get('httpUrl', None)
               for item in timeDbConfig if type(item) == dict]
    urlList = [u for u in urlList if u is not None and u != '']
    reqData = []
    if not urlList:
        return

    # insert \ before sumbol in symbolToScreen list
    def doScreening(data, symbolsToScreen):
        t = type(data)
        if t == str:
            for s in symbolsToScreen:
                data = data.replace(s, '\\' + s)
        elif t == int or t == float:
            pass
        elif t == bool:
            data = None
        elif data is None:
            pass
        else:
            raise Exception("тип данных "+str(t) +
                            " не поддерживается")
        return data

    # collect some amount of lines and sends them in single request.
    # Requests are sending to every url in urllist
    def sendMeasurement(mId, tags, values, timestamp, flushData=False):

        # print("-----",mId,tags,values,timestamp,flushData)
        nonlocal reqData

        if not flushData:
            mId = doScreening(mId, [",", " "])

            values = {k: doScreening(v, ['"']) for k, v in values.items()}
            # filter for None and empty strings
            values = {k: v for k, v in values.items(
            ) if v is not None and v != ''}

            tags = {k: doScreening(v, [",", " ", "="])
                    for k, v in tags.items()}
            # filter for None and empty strings
            tags = {k: v for k, v in tags.items() if v is not None and v != ''}

            # add special boolean tags: haserror and hasvalue
            tags.update({
                'haserror': str(int("error" in values.keys())),
                'hasvalue': str(int("value" in values.keys())),
            })

            # if all values are empty - not send anything
            if (mId is not None) and values:
                # curline is like
                # demo.host1.1-1.someKey1.intelcpu.0.load.0,server=demo,boxname=Хост\
                # 1,itemname=Загрузка\ ЦП,unit=% value=1e-16
                curLine = \
                    mId + \
                    ''.join(["," + k + "=" + v for k, v in tags.items()]) + \
                    ' ' + \
                    ','.join([k + "=" +
                              ('"' if type(v) == str else '') +
                              str(v) +
                              ('"' if type(v) == str else '')
                              for k, v in values.items()]) + \
                    ((" "+str(timestamp)) if timestamp is not None else '')
                # print(timestamp)
                # print(curLine)
                reqData += [curLine]
        # end if

        if (len(reqData) >= pointsPerRequest) or flushData:
            reqBody = ('\n'.join(reqData)).encode("UTF-8")
            reqData = []
            for url in urlList:
                try:
                    r = requests.post(url, reqBody)
                except Exception as e:
                    errors.add(str(e))
                else:
                    if r.status_code != 204:
                        errors.add(str(r.status_code)+" "+r.text)
        # end if
    # end function

    for host in pollResult:

        hostId = "host:"+host['id']
        hostName = host['name']
        serverName = host['pollServer']
        # errors count at host
        errorCount = sum([record.get("style", None) ==
                          "rem" for record in host['data']])
        hostTags = {
            'name': hostName,
            'server': serverName,
        }
        hostValues = {
            'errorcount': errorCount
        }

        # send error information only if errors presents
        if errorCount > 0:
            sendMeasurement(hostId, hostTags, hostValues, None)

        items = host['data']
        for item in items:
            if item.get('enabled', True):

                # split id parts. item['id'] like:
                #    server.host.unuque_key.param_name.suffix
                # ex. localhost.localhost.2-6.hddUsed.hdd.1.load.0
                idParts = item['id'].split('.')

                if len(idParts) < 3:
                    errors.add("wrong item id:"+item['id'])
                    print("wrong item id:", item['id'])
                    continue

                # measure id is a parameter name with marker
                # ex. "item:hddUsed"
                # measure id hould not contain hostname or another additionals
                # because group operation (like sum) are allowed only
                # inside single measurement
                # ex. sum of unprocessed MQ messages for all CBK
                # see influxDB docs for details
                measureId = "item:" +\
                    (idParts[2] if len(idParts) == 3 else idParts[3])
                # measureId="item:"+item['id']

                # measure suffix for combo tasks from itemID
                # ex. localhost.localhost.2-6.hddUsed.hdd.1.load.0 -->
                # hdd.1.load.0
                msuffix = '.'.join(idParts[4:])

                itemTags = {
                    'server': serverName,
                    'host': hostName,
                    'hostkey': item.get('agentKey', None),
                    'name': item.get('itemName', None),
                    'msuffix': msuffix
                    # 'unit':item.get('unit',None)
                }

                error = item.get('error', None)

                itemTimeStamp = item.get('timeStamp', None)
                if itemTimeStamp is not None:
                    # convert from UTC datetime to UTC timestamp
                    itemTimeStamp = calendar.timegm(itemTimeStamp.timetuple())
                    # timestamp() returns 10-digit float. InxluxDB requires
                    # 19-digit integer
                    itemTimeStamp = int(itemTimeStamp*1000000000)
                    # itemTimeStamp=int(itemTimeStamp.timestamp()*1000000000)

                if (error is not None) or (itemTimeStamp is None):
                    # in any strange situation - not publish values to DB.
                    # publish errors only
                    value = None
                else:
                    value = item.get('value', None)

                # allowed value types to write in db
                if type(value) not in [int, float, bool]:
                    value = None

                # convert bool to int, because grafana not support displaying
                # bools
                if type(value) == bool:
                    value = int(value)

                itemValues = {'value': value, 'error': error}
                sendMeasurement(measureId, itemTags, itemValues, itemTimeStamp)
        # endfor task
    # endfor host
    sendMeasurement(None, None, None, None, flushData=True)

    # errors.add("Преведтдт")
