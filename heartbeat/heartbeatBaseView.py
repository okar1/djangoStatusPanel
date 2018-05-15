# -*- coding: utf-8 -*-
from shared.views import BoxForm, BoxFormView
from .threadPoll import threadPoll
from .models import Servers, ServerGroups, Options
import time


class HeartbeatBaseViewForm(BoxForm):
    pass
    headString = "redefine me"

# *** Abstract class ***
# generates view with heartbeat boxes, serverGroup selector and autoUpdate timer
# not generates tasks for every selected box.
# updateTasks(self) must be redefined in child class

class HeartbeatBaseView(BoxFormView):
    form_class = HeartbeatBaseViewForm
    buttons = []
    # static
    tasks = {}
    timeStamp = 0
    pollResult=None

    # returns serverGroup list to gui
    def getGroups(self):
        if self.request.user.is_staff or self.request.user.is_superuser:
            res = list(ServerGroups.objects.all().order_by(
                'name').values('id', 'name'))
        else:
            # get only servergroups, wich "usergroups" field contains one or
            # more usergroups of this user
            res = list(ServerGroups.objects.
                       filter(usergroups__in=self.request.user.groups.all()).
                       order_by('name').
                       values('id', 'name'))

        # if we got only one serverGroup - hide serverGroup selector in gui.
        if len(res) == 1:
            res = []
        return res

    # return base timestamp for timer value calculation
    def getProgresUpdatedAgoSec(self):
        return int(time.time()) - threadPoll.pollTimeStamp

    # return time for timer become red
    def getTimerDangerousTimeSec(self):
        return 3 * Options.getOptionsObject()['pollingPeriodSec']

    # return boxes list of requested servergroup to gui
    # 1) if there are >1 servers in this group - then add servername
    #    to all box captions
    # 2) if there is 1 server in this group - don't add servername to caption
    # 3) if caption length too long - then truncate it
    def getBoxesForGroup(self, serverGroupID):

        def makeBoxCaption(key, value, serverName, addServerName):
            if key == "name":
                if addServerName and serverName != value:
                    # prepend server name to box caption
                    # also look threadpollsubs.py/pollResultSort for sorting
                    # rule
                    res = serverName + " " + value
                else:
                    res = value
                # truncate box caption>25 symbols
                if len(res) > 25:
                    res = res[:25] + "..."
                return res
            else:
                return value

        def filterBoxFields(d, addServerName=False):
            return {
                k: makeBoxCaption(k, v, d['pollServer'], addServerName)
                for k, v in d.items()
                if k in ['id', 'name', 'error', 'progress', 'enabled']}

        if serverGroupID is None:
            # requested all available serverGroups
            serverGroups = ServerGroups.objects.all()
        else:
            # requested specified servergroup
            serverGroups = ServerGroups.objects.filter(id=serverGroupID)

        # for non-privilleged users filter only servergroups, wich user has
        # permission
        if not (self.request.user.is_staff or self.request.user.is_superuser):
            serverGroups = serverGroups.filter(
                usergroups__in=self.request.user.groups.all())

        # for privileged users display all servers if no one serverGroup exists
        if (self.request.user.is_staff or self.request.user.is_superuser) and \
                serverGroupID is None:
            serverq = Servers.objects.all()
        else:
            # in all other cases filter names of servers wich are members of
            # calculated servergroups
            serverq = Servers.objects.filter(servergroups__id__in=serverGroups)

        serverList = list(
            serverq.
            order_by('name').values_list('name', flat=True)
        )

        serverCount = len(serverList)
        self._updatePollAndTasks()

        res = [
            filterBoxFields(v, serverCount != 1)
            for v in self.__class__.pollResult
            if v['pollServer'] in serverList]

        self.progressArray = res
        return res


    def _updatePollAndTasks(self):
        if self.__class__.pollResult is None or \
                self.__class__.timeStamp < threadPoll.pollTimeStamp or \
                not self.__class__.tasks:
            # print ('update tasks view data')
            self.__class__.timeStamp = threadPoll.pollTimeStamp
            self.__class__.pollResult=self.getPollResult()
            self.updateTasks()


    def getRecordsForBox(self, boxID):
        # WARNING
        # for speed reasons here we not provide a security check
        # that requested boxID is part of server that part of serverGroup that
        #    current user has permission
        # so if any registered user knows exactly boxid like "xdemo3.Probe HLS
        # 3" - he can request records for this box
        self._updatePollAndTasks()
        # print(boxID)
        return self.__class__.tasks.get(boxID, [])


    # rule for sorting tasks in view
    @staticmethod
    def sortTasks(t):
        taskStyle = t.get('style', None)
        if taskStyle == 'rem':
            res = '0'
        elif taskStyle == "ign":
            res = '2'
        else:
            res = '1'
        return res + t['name']

    def getPollResult(self):
        raise Exception("pollResult generator must be redefined in child classes")

    @staticmethod
    def getNameForTask(task):
        # can be redefined in child to generate custom tasks name
        return task['itemName']

    # tasks like {boxid:{[task data]}}
    def updateTasks(self):
        tasks = {}
        for box in self.__class__.pollResult:
            boxTasks = []
            for task in box['data']:
                viewTask = task.copy()
                # alarms.pattern and timestamp are used here
                viewTask['name'] = self.getNameForTask(task)
                # now remove them because of non-need and non-serializable
                viewTask.pop('timeStamp', None)
                viewTask.pop('alarms', None)
                boxTasks += [viewTask]
            boxTasks.sort(key=self.sortTasks)
            tasks[box['id']] = boxTasks
        self.__class__.tasks = tasks
    # end sub
