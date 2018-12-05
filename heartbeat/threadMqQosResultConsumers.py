import zlib
from datetime import timedelta
from datetime import datetime
from .threadMqConsumers import MqConsumers
import re
import time

timeStampFormat="%Y%m%d%H%M%S"
timeStampFormatZ="%Y-%m-%dT%H:%M:%S.%fZ"

taskKeyPattern=re.compile(r'"taskKey"\s*:\s*"(.+?)"')

# old time format: 20181205103803.002
# or 20181205103803
# or 20181205103803MSK
resultDateTimePattern=re.compile(r'"(resultDateTime|timestamp)"\s*:\s*"(\d+)(([A-Z]|[a-z]|\.)*.*?)"')

# new time format: rfc
resultDateTimePatternZ=re.compile(r'"(resultDateTime|timestamp)"\s*:\s*"((\d|T|-|:|\.)+?Z)"')

probeAvailablePattern=re.compile(r'"probe_availability"\s*:\s*1')
lettersOnlyPattern=re.compile(r'([A-Z]|[a-z])+')

# delta between local time and UTC
localTZdelta=timedelta(seconds=time.timezone)

class MqQosResultConsumers(MqConsumers):


    # overriding parent method
    def onMessageGenericHandler(self, mMetaData, mProperties, mData):
        unknownMessageKey="__unknownMessageKey__"

        # print("mq gen 2 handler")
        try:
            mHeaders=mProperties.headers
            if mProperties.content_type != 'application/json':
                self.messages[unknownMessageKey] = {"error": "Неверный тип данных в сообщении RabbitMQ" + mProperties}
                return

            #unpack data if it is compresed
            if hasattr(mProperties,"content_encoding"):
                if mProperties.content_encoding=='gzip':
                    mData=zlib.decompress(mData,16+zlib.MAX_WBITS)
        except Exception as e:
            # print(mData)
            self.messages[unknownMessageKey] = {"error": str(e)}
            return

        msgType = ""
        try:
            msgType = mHeaders['__TypeId__']
        except Exception as e:
            self.messages[unknownMessageKey] = {"error": "Ошибка обработки сообщения: нет информации о типе."}
            return

        # parse message payload
        try:
            mData = mData.decode('utf-8')
            searchRes=taskKeyPattern.search(mData)
            if searchRes is not None:
                taskKey=searchRes.group(1)
            else:
                self.messages[unknownMessageKey] = {"error": "Ошибка обработки сообщения: нет информации о taskKey."}
                return

            if mData.find('.SelfMonitor.availability') != -1:
                # for .SelfMonitor.availability task ignore results
                # if probe is unavailable
                # i.e. "probe_availability":0 means no data for this task
                if probeAvailablePattern.search(mData) is None:
                    return


            # parse message DateTime
            searchTime=resultDateTimePatternZ.search(mData)
            if searchTime is not None:
                # new time format like RFC
                dt=datetime.strptime(searchTime.group(2), timeStampFormatZ)
            else:
                # old time format
                searchTime=resultDateTimePattern.search(mData)
                if searchTime is not None:
                    dt=datetime.strptime(searchTime.group(2), timeStampFormat)

                    # some modules (like SelfMonitor.availability) return timezone information
                    # in non-standart format like "resultDateTime":"20180515111400MSK"
                    dtSuffix=searchTime.group(3)
                    if dtSuffix!='' and lettersOnlyPattern.search(dtSuffix) is not None  : # i.e. == 'MSK'
                        # asserting that if timezone information exitst - it is server local timezone
                        # TODO: 
                        # * check zone identifier if server timezone is non-MSK.
                        # * add if-else for such non-standard timezones if these are not local server timezones
                        
                        # if timezone info exists - converting from server timezone to UTC
                        dt+=localTZdelta
                else:
                    self.messages[taskKey] = {"error": "Нет информации о времени: " + mData}
                    return

                    
            if msgType in ['com.tecomgroup.qos.communication.message.ResultMessage',
                           'com.tecomgroup.qos.communication.probe2server.ResultMessage',
                           'com.tecomgroup.qos.communication.message.MatchResultMessage',
                           'com.tecomgroup.qos.communication.message.TSStructureResultMessage']:

                    self.messages[taskKey] = {'timeStamp': dt}
            elif msgType ==  'com.tecomgroup.qos.communication.message.TaskStatus':
                pass
            else:
                self.messages[taskKey] = {"error": "Неизвестный тип сообщения: " + msgType}
                return

        except Exception as e:
            key=unknownMessageKey if "taskKey" not in locals() else taskKey
            self.messages[key] = {"error": str(e)}
            return 
