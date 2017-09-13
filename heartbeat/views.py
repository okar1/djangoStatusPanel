# -*- coding: utf-8 -*-
from shared.views import BoxForm, BoxFormView
from .threadPoll import threadPoll
from .models import Servers, ServerGroups, Options
import time


class MainViewForm(BoxForm):
    pass
    headString = "heartbeat"
    # annotationString="Выберите пользователя из списка,
    # затем щелкните по виджету для отображения списка задач"


# group format [{"id":1,"name":groupName1},{"id":2,"name":groupName2}]
# boxesForGroup format [{"id":boxId,"name":boxName,"error":someErrorText}]
# (if "error" is not present - box is green else red with message)
# recordsForBox format [{"id":recId,"name":recName,"style":"add/rem"}]
# style is optional. Use default style if key is not present
class MainView(BoxFormView):
    form_class = MainViewForm
    buttons = []

    def getGroups(self):
        return list(ServerGroups.objects.all().order_by('name').values('id', 'name'))

    # return base timestamp for timer value calculation
    def getProgresUpdatedAgoSec(self):
        return int(time.time()) - threadPoll.pollTimeStamp

    # return time for timer become red
    def getTimerDangerousTimeSec(self):
        return 3 * Options.getOptionsObject()['pollingPeriodSec']

    # 1) if there are >1 servers in this group - then add servername to all box captions
    # 2) if there is 1 server in this group - don't add servername to caption
    # 3) if caption length too long - then truncate it
    def getBoxesForGroup(self, groupID):
        def makeBoxCaption(key,value,serverName,addServerName):
            if key=="name":
                if addServerName and serverName!=value:
                    # prepend server name to box caption
                    # also look threadpollsubs.py/pollResultSort for sorting rule
                    res=serverName+" "+value
                else:
                    res=value
                # truncate box caption>25 symbols
                if len(res)>25:
                    res=res[:25]+"..."
                return res
            else:
                return value

        def filterBoxFields(d,addServerName=False):
            return {
                    k : makeBoxCaption(k,v,d['pollServer'],addServerName)
                    for k, v in d.items()
                    if k in ['id', 'name', 'error', 'progress']}

        if groupID is None:
            serverCount=Servers.objects.count()
            res = [
                    filterBoxFields(v,serverCount!=1)
                    for v in threadPoll.pollResult]
        else:
            serverList = list(
                            Servers.objects.filter(servergroups__id=int(groupID)).
                            order_by('name').values_list('name', flat=True)
                            )
            serverCount = len(serverList)            
            res = [
                    filterBoxFields(v,serverCount!=1)
                    for v in threadPoll.pollResult
                    if v['pollServer'] in serverList]
        self.progressArray = res
        return res

    def getRecordsForBox(self, boxID):
        # print(boxID)
        for item in threadPoll.pollResult:
            if item['id'] == boxID:
                return item['data']
        return []
