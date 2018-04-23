# -*- coding: utf-8 -*-
from .heartbeatBaseView import HeartbeatBaseView, HeartbeatBaseViewForm
from . import threadPollSubs as subs
from . import qosDb
from .models import Servers,Hosts

# generic content view with info about group policy check status
# IMPORTANT. This generic content is generated only when browser tab is open!
# servergroups and boxes are the same as heartbeat
# tasks - contains info about group policies check



class PolicyCheckViewForm(HeartbeatBaseViewForm):
    pass
    headString = "Проверка оповещений"


class PolicyCheckView(HeartbeatBaseView):
    form_class = PolicyCheckViewForm
    buttons = []
    # static
    tasks = {}
    timeStamp = 0

    def getPollResult(self):
        # generating pollResult with custom content for boxes: group policy checks results
        # this sub is called once in poll period. Results, returned bu it will be cached
        # print('pollResult update!')
        
        delayedServerErrors = {}
        pollResult = []
        hostAliases = Hosts.getAllAliases()

        for server in Servers.objects.all():

            serverDbConfig = server.getConfigObject()['db']
            serverErrors = {}
            
            tasksToPoll=None

            serverDb, tasksToPoll = subs.pollDb(
                serverDbConfig,
                server.name,
                serverErrors,
                None,
                qosDb.getPolicyStatus
            )

            if serverDb:
                if tasksToPoll:
                    # if some tasksToPoll name found in alias list - rename
                    # such task to hostname, that owns alias
                    # in this case box of such taskToPoll will be merged into
                    # host's box
                    subs.applyHostAliases(hostAliases, server, tasksToPoll)


                # tasksToPoll,serverErrors -> serverPollResult
                # grouping tasks to boxes by agentKey, also create +1 box for
                # server errors
                serverPollResult = subs.makePollResult(
                    tasksToPoll,
                    server.name,
                    serverErrors,
                    delayedServerErrors.setdefault(server.name, set())
                )

                # calc error percent for boxes
                # percent is calculated for tasks with "style":"rem" to all tasks
                subs.pollResultCalcErrorPercent(serverPollResult)

                # add this server poll result to global poll result
                pollResult += serverPollResult

                serverDb.close()
        # end for server

        # sort boxex, make boxes with errors first. Not affects on tasks inside
        # boxes
        subs.pollResultSort(pollResult)

        return pollResult

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['pageColor'] = "4c4c4c"
        return context