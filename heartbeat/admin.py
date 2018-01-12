# -*- coding: utf-8 -*-
from .models import Servers, Options, ServerGroups, Hosts, Items, TaskSets, \
    Triggers, ResultFormatters
from .threadPollSubs import sendRegisterMessage

from django.contrib import admin
from django import forms
from django.forms.models import ModelMultipleChoiceField

import json

# TODO now host passwords are transfered to admin form unencrypted


@admin.register(Servers)
class ServersAdmin(admin.ModelAdmin):
    # form=ServerComponentsForm

    def get_fields(self, request, obj=None):
        res = super().get_fields(request, obj)
        fieldTemplate = Servers.getFieldTemplate()

        if obj is not None:
            # {'mq':[['server',port,'user','password'],
            #       ['server',port,'user','password']],
            # 'db':[...],'fe':[...],'be':[...]}
            config = json.loads(obj.config)
        else:
            config = {}

        # create django form fields with template and config data from DB
        fields = ['name', 'qosguialarm']
        for nodeName in fieldTemplate.keys():  # ['mq','db','fe','be']:
            # [['server',port,user,password],[['server',port,user,password]]]
            nodeValues = config.get(nodeName, [])

            nvLen = len(nodeValues)
            for nodeIndex in range(nvLen + 1):

                if nodeIndex < nvLen:
                    # ['server',port,user,password]
                    nodeValue = nodeValues[nodeIndex]
                else:
                    nodeValue = []

                curFields = []
                # nodefields constructor
                for fieldIndex, fieldKey in \
                        enumerate(fieldTemplate[nodeName].keys()):
                    # create field object with template data and initial values
                    fieldValue = (fieldTemplate[nodeName][
                                  fieldKey].fieldOptions).copy()

                    # first nodes has initial values from template, next are ''
                    if nodeIndex > 0:
                        fieldValue['initial'] = ''

                    # server 1,2,3 in field label
                    if fieldIndex == 0 and nodeIndex > 0:
                        fieldValue['label'] += (' ' + str(nodeIndex + 1))

                    # change initial value to actual value if nodeValue present
                    if fieldIndex < len(nodeValue):
                        fieldValue['initial'] = nodeValue[fieldIndex]

                    fieldClass = fieldTemplate[nodeName][fieldKey].fieldClass

                    # create field object with template data and initial values
                    fieldObject = fieldClass(**fieldValue)

                    fullFieldKey = nodeName + '.' + \
                        str(nodeIndex) + '.' + fieldKey  # mq.1.server
                    curFields += [fullFieldKey]

                    # django's fields definition dict, like {'keymquser' :
                    # CharField...)}
                    self.form.declared_fields[fullFieldKey] = fieldObject
                # endfor
                fields.append(curFields)
            # endfor node values
        # endfor nodes

        # django's fields order list, like [ ('keymquser', 'keymqpassword'),
        # ('keydbUser', 'keyDbPassword') ]
        self.fields = fields

        return res

    def save_model(self, request, model, form, change):
        # key like "db.0.pwd"
        def getCfgValue(config, key):
            key = key.split('.')
            res = ''
            if len(key) != 3:
                return res
            k1, k2, k3 = key
            if (k1 in config) and \
                    k2.isdigit() and int(k2) < len(config[k1]):
                res = config[k1][int(k2)].get(k3, '')
            return res

        # {'be.0.server': 'localhost',
        # 'mq.0.server': 'localhost',
        # 'mq.0.user': 'guest',
        # 'db.0.server': 'localhost',
        # 'fe.0.server': 'localhost',
        # 'db.0.pwd': '',
        # 'mq.0.port': 15672,
        # 'mq.0.pwd': 'guest',
        # 'db.0.user': 'qos',
        # 'db.0.port': 5432}
        data = form.cleaned_data
        template = Servers.getFieldTemplate()
        oldData = model.getConfigObject(decryptPwd=False)
        res = {}
        for nodeName in template.keys():
            i = 0
            nodeData = []
            while True:
                nodeItemData = []

                for key in template[nodeName].keys():
                    fullFieldKey = nodeName + '.' + \
                        str(i) + '.' + key  # mq.1.server

                    if fullFieldKey in data.keys():
                        itemValue = data[fullFieldKey]
                        if key == 'pwd' and itemValue != '':
                            oldEncryptedPwd = getCfgValue(
                                oldData, fullFieldKey)
                            if itemValue == oldEncryptedPwd:
                                pass  # field value not changed
                            else:
                                itemValue = Servers.encryptPassword(itemValue)
                        nodeItemData += [itemValue]
                    else:
                        break
                # end for
                else:
                    # for unbroken
                    if any(item not in [None, ''] for item in nodeItemData):
                        nodeData += [nodeItemData]

                    i += 1
                    continue

                break  # while
            # end while
            res[nodeName] = nodeData
        # end for

        model.config = json.dumps(res)
        super().save_model(request, model, form, change)


@admin.register(ServerGroups)
class ServerGroupsAdmin(admin.ModelAdmin):
    list_display = ['name', ]
    fields = ['name', 'servers', 'usergroups']


@admin.register(Options)
class OptionsAdmin(admin.ModelAdmin):
    # form=RulesForm
    # list_display = ['rule', 'comment']
    fields = ['name', 'value']


def hostRegister(modeladmin, request, qset):
    # queryset.update(status='p')
    hostRegister.short_description = "Зарегистрировать"

    # non-repeating list of servers of selected hosts
    serversSelected = {obj.server for obj in qset}

    try:
        for serv in serversSelected:
            # hosts for current server
            hosts = qset.filter(server__exact=serv)

            if len(hosts) == Hosts.objects.filter(server__exact=serv).count():
                # all hosts of current server was selected.
                # send "register all" command
                keyList = ["agent"]
            else:
                # some hosts of current server was selected
                # send register command for every host
                keyList = ["agent-" + host.key for host in hosts]

            sendRegisterMessage(serv, keyList)
            print("sent registration messages ", serv.name, keyList)
    except Exception as e:
        print("sent registration messages error: ", str(e))


# custom form for hosts and items
# adds ability to select linked taskSets
class TaskSetSelectorForm(forms.ModelForm):
    taskSets = ModelMultipleChoiceField(
        label="Связанные задачи",
        required=False,
        queryset=TaskSets.objects.all()
    )
    # Overriding __init__ here allows us to provide initial
    # data for 'toppings' field

    def __init__(self, *args, **kwargs):
        # Only in case we build the form from an instance
        # (otherwise, 'toppings' list should be empty)
        if kwargs.get('instance'):
            # We get the 'initial' keyword argument or initialize it
            # as a dict if it didn't exist.
            initial = kwargs.setdefault('initial', {})
            # The widget for a ModelMultipleChoiceField expects
            # a list of primary key for the selected data.
            initial['taskSets'] = [t.pk for t in kwargs[
                'instance'].tasksets_set.all()]
        forms.ModelForm.__init__(self, *args, **kwargs)

    # Overriding save allows us to process the value of 'toppings' field
    def save(self, commit=True):
        # Get the unsave Pizza instance
        instance = forms.ModelForm.save(self, False)

        # Prepare a 'save_m2m' method for the form,
        old_save_m2m = self.save_m2m

        def save_m2m():
            old_save_m2m()
            # This is where we actually link the pizza with toppings
            instance.tasksets_set.clear()
            for item in self.cleaned_data['taskSets']:
                instance.tasksets_set.add(item)
        self.save_m2m = save_m2m

        # Do we need to save all changes now?
        if commit:
            instance.save()
            self.save_m2m()
        return instance


# custom form for hosts
# adds ability to select linked taskSets
class HostsForm(TaskSetSelectorForm):

    class Meta:
        model = Hosts
        fields = ('name', 'key', 'server', 'enabled', 'config',
                  'aliases', 'alarms', 'comment', 'taskSets',)


# custom form for items
# adds ability to select linked taskSets
class ItemsForm(TaskSetSelectorForm):

    class Meta:
        model = Items
        fields = ('name', 'key', 'enabled', 'unit', 'config',
                  'resultformatter', 'alarms', 'comment', 'taskSets',)


@admin.register(Hosts)
class HostsAdmin(admin.ModelAdmin):
    form = HostsForm
    list_filter = ('server__name',)
    list_display = ['name', 'key', 'server', 'enabled', 'id', 'comment']
    # fields = ['name', 'value']
    actions = [hostRegister]


@admin.register(Items)
class ItemsAdmin(admin.ModelAdmin):
    form = ItemsForm
    list_display = ['name', 'key', 'enabled', 'unit', 'id', 'comment']
    # fields = ['name', 'value']


@admin.register(TaskSets)
class TaskSetsAdmin(admin.ModelAdmin):
    # form=RulesForm
    list_display = ['name', 'enabled', 'comment']
    # fields = ['name', 'value']


@admin.register(Triggers)
class TriggersAdmin(admin.ModelAdmin):
    # form=RulesForm
    list_display = ['name', 'config']
    # fields = ['name', 'value']


@admin.register(ResultFormatters)
class ResultFormattersAdmin(admin.ModelAdmin):
    pass
    # form=RulesForm
    # list_display = ['rule', 'comment']
    # fields = ['name', 'value']

# admin.site.register(ServerComponents, ServerComponentsAdmin)
