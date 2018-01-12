import zlib
from datetime import datetime
from .threadMqConsumers import MqConsumers
import re

timeStampFormat = "%Y%m%d%H%M%S"
matchTimeStampFormat = "%Y-%m-%dT%H:%M:%S.%fZ"

resultDateTimePattern = re.compile(r'"resultDateTime"\s*:\s*"(.+?)"')
timestampPattern = re.compile(r'"timestamp"\s*:\s*"(.+?)"')
taskKeyPattern = re.compile(r'"taskKey"\s*:\s*"(.+?)"')


class MqQosResultConsumers(MqConsumers):

    # overriding parent method
    def onMessageGenericHandler(self, mMetaData, mProperties, mData):
        unknownMessageKey = "__unknownMessageKey__"

        # print("mq gen 2 handler")
        try:
            mHeaders = mProperties.headers
            if mProperties.content_type != 'application/json':
                self.messages[unknownMessageKey] = {
                    "error":
                    "Неверный тип данных в сообщении RabbitMQ" + mProperties
                }
                return

            # unpack data if it is compresed
            if hasattr(mProperties, "content_encoding"):
                if mProperties.content_encoding == 'gzip':
                    mData = zlib.decompress(mData, 16 + zlib.MAX_WBITS)
        except Exception as e:
            # print(mData)
            self.messages[unknownMessageKey] = {"error": str(e)}
            return

        msgType = ""
        try:
            msgType = mHeaders['__TypeId__']
        except Exception as e:
            self.messages[unknownMessageKey] = {
                "error": "Ошибка обработки сообщения: нет информации о типе."}
            return

        # parse message payload
        try:
            mData = mData.decode('utf-8')
            # mData = json.loads(mData.decode('utf-8'))
            # taskKey = mData['taskKey']

            # taskKey=mHeaders['taskKey']
            searchRes = taskKeyPattern.search(mData)
            if searchRes is not None:
                taskKey = searchRes.group(1)
            else:
                self.messages[unknownMessageKey] = {
                    "error":
                    "Ошибка обработки сообщения: нет информации о taskKey."
                }
                return

            # if taskKey=="IRKUTSK-P-1.ClipSearch.2583":
            #     print(mData)

            if msgType == 'com.tecomgroup.qos.communication.message.' + \
                    'ResultMessage':
                searchRes = resultDateTimePattern.search(mData)
                if searchRes is not None:
                    self.messages[taskKey] = \
                        {'timeStamp': datetime.strptime(
                            searchRes.group(1), timeStampFormat)}
            elif msgType == 'com.tecomgroup.qos.communication.message.' + \
                    'MatchResultMessage':
                searchRes = resultDateTimePattern.search(mData)
                if searchRes is not None:
                    self.messages[taskKey] = \
                        {'timeStamp': datetime.strptime(
                            searchRes.group(1), matchTimeStampFormat)}

                # print("****************")
                # print(mData)
                # self.messages[taskKey] = \
                #     {'timeStamp': datetime.utcnow()}

                # taskResults = mData['results']
                # for tr in taskResults:
                #     if len(tr['parameters'].keys()) > 0:
                #         self.messages[taskKey] = \
                #             {'timeStamp':datetime.utcnow()}

            elif msgType == 'com.tecomgroup.qos.communication.message.' + \
                    'TaskStatus':
                pass
            elif msgType == 'com.tecomgroup.qos.communication.message.' + \
                    'TSStructureResultMessage':
                searchRes = timestampPattern.search(mData)
                if searchRes is not None:
                    self.messages[taskKey] = \
                        {'timeStamp': datetime.strptime(
                            searchRes.group(1), timeStampFormat)}

                # if len(mData['TSStructure']) > 0:
                #     self.messages[taskKey] = \
                #         {'timeStamp': datetime.strptime(mData['timestamp'],
                #         timeStampFormat) }
            else:
                self.messages[taskKey] = {
                    "error": "Неизвестный тип сообщения: " + msgType}
                return

        except Exception as e:
            key = unknownMessageKey if "taskKey" not in locals() else taskKey
            self.messages[key] = {"error": str(e)}
            return
