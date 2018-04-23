# -*- coding: utf-8 -*-
from .heartbeatBaseView import HeartbeatBaseView, HeartbeatBaseViewForm
from .threadPoll import threadPoll
from .models import Servers, ServerGroups, Options
from datetime import datetime

# generates heartheat tasks order and description for every box selected by user
class HeartbeatViewForm(HeartbeatBaseViewForm):
    pass
    headString = "heartbeat"
    # annotationString="Выберите пользователя из списка,
    # затем щелкните по виджету для отображения списка задач"


# group format [{"id":1,"name":groupName1},{"id":2,"name":groupName2}]
# boxesForGroup format [{"id":boxId,"name":boxName,"error":someErrorText}]
# (if "error" is not present - box is green else red with message)
# recordsForBox format [{"id":recId,"name":recName,"style":"add/rem"}]
# style is optional. Use default style if key is not present
class HeartbeatView(HeartbeatBaseView):
    form_class = HeartbeatViewForm
    buttons = []
    # static
    tasks = {}
    timeStamp = 0

    @staticmethod
    def getNameForTask(task):

        def secondsCountToHumanString(sec):
            weekSec = 604800
            daySec = 86400
            hourSec = 3600
            minSec = 60
            if sec // weekSec > 0:
                cnt = sec // weekSec
                if cnt % 10 == 1:
                    unit = "неделю"
                elif cnt % 10 <= 4:
                    unit = "недели"
                else:
                    unit = "недель"

            elif sec // daySec > 0:
                cnt = sec // daySec
                if cnt % 10 == 1:
                    unit = "день"
                elif cnt % 10 <= 4:
                    unit = "дня"
                else:
                    unit = "дней"

            elif sec // hourSec > 0:
                cnt = sec // hourSec
                unit = "ч"

            elif sec // minSec > 0:
                cnt = sec // minSec
                unit = "мин"

            else:
                cnt = sec
                unit = "сек"
            return str(cnt) + " " + unit
        # end sub


        # converts value before displaying it into view
        def formatValue(v):

            # display string values in quotes
            if type(v) == str:
                return "\"" + v + "\""

            # display floating numbers without decimal place as integer (ex
            # "23" instead "23.0")
            if type(v) == float and v == int(v):
                v = int(v)

            # predefined strings for boolean values
            if type(v) == bool:
                if v:
                    v = "ДА"
                else:
                    v = "НЕТ"
            return str(v)
        # end sub

        
        taskKey = task['id']

        if task.get('servertask', False):
            name = taskKey + " : "
        else:
            name = "{0} ({1}".format(taskKey, task['itemName'])

            alarms = task.get('alarms', {})
            # if 'pattern' not in keys - apply to all (like pattern .*)
            alarmsCount = sum([True
                               for alarmData in alarms.values()
                               if ('pattern' not in alarmData.keys()) or
                               alarmData['pattern'].search(taskKey)
                               ])
            if alarmsCount > 0:
                name += " +" + str(alarmsCount) + " alarm"

            name += ")"

        if (not task['enabled']) and ('error' not in task):
            name += " : Задача не контролируется "

        if 'timeStamp' in task.keys() and task.get('style', None) != 'ign':
            idleTime = datetime.utcnow() - task['timeStamp']
            idleTime = idleTime.days * 86400 + idleTime.seconds
            name += " : {0} назад".format(
                        secondsCountToHumanString(idleTime))

        if 'value' in task.keys():  # and (task.get('error',None) is None):
            # not display a value in case of error
            name += (" получено значение " + formatValue(task['value']))

            unit = task.get('unit', '')
            if unit != '':
                name += (" " + unit)
        # end if

        error = task.get('error', None)
        if (error is not None):
            name += (" : Ошибка: " + error)

        return name
    # end sub


    def getPollResult(self):
        return threadPoll.pollResult


