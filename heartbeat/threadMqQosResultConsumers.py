import zlib
from datetime import timedelta
from datetime import datetime
from .threadMqConsumers import MqConsumers
import re
import time

timeStampFormat="%Y%m%d%H%M%S"
matchTimeStampFormat="%Y-%m-%dT%H:%M:%S.%fZ"

# resultDateTimePattern=re.compile(r'"resultDateTime"\s*:\s*"(.+?)"')
resultDateTimePattern=re.compile(r'"resultDateTime"\s*:\s*"(\d+?)(\D*?)"')
probeAvailablePattern=re.compile(r'"probe_availability"\s*:\s*1')

timestampPattern=re.compile(r'"timestamp"\s*:\s*"(.+?)"')
taskKeyPattern=re.compile(r'"taskKey"\s*:\s*"(.+?)"')

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
            # mData = json.loads(mData.decode('utf-8'))
            # taskKey = mData['taskKey']

            # taskKey=mHeaders['taskKey']
            searchRes=taskKeyPattern.search(mData)
            if searchRes is not None:
                taskKey=searchRes.group(1)
            else:
                self.messages[unknownMessageKey] = {"error": "Ошибка обработки сообщения: нет информации о taskKey."}
                return

            # if taskKey=="IRKUTSK-P-1.ClipSearch.2583":
            #     print(mData)

            if msgType == 'com.tecomgroup.qos.communication.message.ResultMessage':
                searchRes=resultDateTimePattern.search(mData)
                if searchRes is not None:
                    
                    if mData.find('.SelfMonitor.availability') != -1:
                        # for .SelfMonitor.availability task ignore results
                        # if probe is unavailable
                        # i.e. "probe_availability":0 means no data for this task
                        if probeAvailablePattern.search(mData) is None:
                            return

                    timeString=searchRes.group(1)
                    dt=datetime.strptime(timeString, timeStampFormat)
                    
                    # some modules (like SelfMonitor.availability) return timezone information
                    # in non-standart format like "resultDateTime":"20180515111400MSK"

                    if searchRes.group(2)!='': # i.e. == 'MSK'
                        # asserting that if timezone information exitst - it is server local timezone
                        # TODO: 
                        # * check zone identifier if server timezone is non-MSK.
                        # * add if-else for such non-standard timezones if these are not local server timezones
                        
                        # if timezone info exists - converting from server timezone to UTC
                        dt+=localTZdelta

                    self.messages[taskKey] = {'timeStamp': dt}

                # taskResults = mData['results']
                # for tr in taskResults:
                #     # if result has any parameters - store timeStamp in results
                #     if len(tr['parameters'].keys()) > 0:
                #         self.messages[taskKey] = \
                #             {'timeStamp': datetime.strptime(tr['resultDateTime'], timeStampFormat)}

            elif msgType ==  'com.tecomgroup.qos.communication.message.MatchResultMessage':
                searchRes=resultDateTimePattern.search(mData)
                if searchRes is not None:
                    self.messages[taskKey] = \
                        {'timeStamp': datetime.strptime(searchRes.group(1), matchTimeStampFormat)}

                # print("****************")
                # print(mData)
                # self.messages[taskKey] = \
                #     {'timeStamp': datetime.utcnow()}

                # taskResults = mData['results']
                # for tr in taskResults:
                #     if len(tr['parameters'].keys()) > 0:
                #         self.messages[taskKey] = \
                #             {'timeStamp':datetime.utcnow()}

            elif msgType ==  'com.tecomgroup.qos.communication.message.TaskStatus':
                pass
            elif msgType == 'com.tecomgroup.qos.communication.message.TSStructureResultMessage':
                searchRes=timestampPattern.search(mData)
                if searchRes is not None:
                    self.messages[taskKey] = \
                        {'timeStamp': datetime.strptime(searchRes.group(1), timeStampFormat)}

                # if len(mData['TSStructure']) > 0:
                #     self.messages[taskKey] = \
                #         {'timeStamp': datetime.strptime(mData['timestamp'], timeStampFormat) }
            else:
                self.messages[taskKey] = {"error": "Неизвестный тип сообщения: " + msgType}
                return

        except Exception as e:
            key=unknownMessageKey if "taskKey" not in locals() else taskKey
            self.messages[key] = {"error": str(e)}
            return 
