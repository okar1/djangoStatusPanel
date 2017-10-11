# -*- coding: utf-8 -*-
from shared.views import BoxForm, BoxFormView
from .threadPoll import threadPoll
from .models import Servers, ServerGroups, Options
import time
from datetime import datetime


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
    #static
    tasks={}
    timeStamp=0

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
        if self.__class__.timeStamp < threadPoll.pollTimeStamp or not self.__class__.tasks:
            # print ('update tasks view data')
            self.__class__.timeStamp = threadPoll.pollTimeStamp
            self.updateTasks()
        # print(boxID)
        return self.__class__.tasks.get(boxID,[])

    # tasks like {boxid:{[task data]}}
    def updateTasks(self):
        def secondsCountToHumanString(sec):
            weekSec=604800
            daySec=86400
            hourSec=3600
            minSec=60
            if sec//weekSec>0:
                cnt=sec//weekSec
                if cnt%10==1:
                    unit="неделю"
                elif cnt%10<=4:
                    unit="недели"
                else:
                    unit="недель"

            elif sec//daySec>0:
                cnt=sec//daySec
                if cnt%10==1:
                    unit = "день"
                elif cnt%10<=4:
                    unit = "дня"
                else:
                    unit = "дней"

            elif sec//hourSec>0:
                cnt=sec//hourSec
                unit="ч"

            elif sec//minSec>0:
                cnt=sec//minSec
                unit="мин"

            else:
                cnt=sec
                unit="сек"
            return str(cnt) + " "+unit
        # end sub

        # converts value before displaying it into view
        def formatValue(v):

            # display string values in quotes
            if type(v)==str:
                return "\""+v+"\""

            # display floating numbers without decimal place as integer (ex "23" instead "23.0")
            if type(v)==float and v==int(v):
                v=int(v)

            # predefined strings for boolean values
            if type(v)==bool:
                if v:
                    v="ДА"
                else:
                    v="НЕТ"
            return str(v) 
        #end sub
        
        # rule for sorting tasks in view
        def sortTasks(t):
            taskStyle = t.get('style', None)
            if taskStyle == 'rem':
                res = '0'
            elif taskStyle == "ign":
                res = '1'
            else:
                res = '2'
            return res+t['name']

        def getNameForTask(task):
            # print(task)
            taskKey=task['id']

            if task.get('servertask',False):
                name=taskKey+" : "
            else:
                name= "{0} ({1}) : ".format(taskKey, task['itemName'])

            if not task.get('enabled',True):
                name += "Задача отключена"
            else:
                if task.get('timeStamp', None) is None:
                    # when error - this tex already present in error message
                    # so no need to dublicate it
                    if task.get('style',None)!='rem':
                        name += "задача не присылает данные"
                else:
                    idleTime = datetime.utcnow() - task['timeStamp']
                    idleTime = idleTime.days * 86400 + idleTime.seconds
                    name+=(" "+secondsCountToHumanString(idleTime)+" назад")

                if (task.get('value',None) is not None) and (task.get('error',None) is None):
                    # not display a value in case of error
                    name += (" получено значение " + formatValue(task['value']))

                    unit=task.get('unit','')
                    if unit !='':
                        name += (" "+unit)
                # end if

                error=task.get('error',None)
                if (error is not None):
                    name += (" ошибка : " + error)
            return name
        #end sub

        tasks={}
        for box in threadPoll.pollResult:
            boxTasks=[]
            for task in box['data']:
                viewTask=task.copy()
                viewTask['name']=getNameForTask(task)
                viewTask.pop('timeStamp',None)
                boxTasks+=[viewTask]
            boxTasks.sort(key=sortTasks)
            tasks[box['id']]=boxTasks
        self.__class__.tasks=tasks
    #end sub
