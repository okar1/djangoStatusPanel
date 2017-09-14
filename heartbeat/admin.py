# -*- coding: utf-8 -*-
from django.contrib import admin
from .models import Servers, Options, ServerGroups, Hosts, Items,TaskSets, Triggers, ResultFormatters
import json

# TODO now host passwords are transfered to admin form unencrypted


@admin.register(Servers)
class ServersAdmin(admin.ModelAdmin):
    # form=ServerComponentsForm

    def get_fields(self, request, obj=None):
        res = super().get_fields(request, obj)
        fieldTemplate = Servers.getFieldTemplate()

        if obj is not None:
            #{'mq':[['server',port,'user','password'],['server',port,'user','password']],'db':[...],'fe':[...],'be':[...]}
            config = json.loads(obj.config)
        else:
            config = {}

        # create django form fields with template and config data from DB
        fields = ['name', 'qosguialarm']
        for nodeName in fieldTemplate.keys():  # ['mq','db','fe','be']:
            # [['server',port,user,password],[['server',port,user,password]]]
            nodeValues = config.get(nodeName, [])

            l = len(nodeValues)
            for nodeIndex in range(l + 1):

                if nodeIndex < l:
                    # ['server',port,user,password]
                    nodeValue = nodeValues[nodeIndex]
                else:
                    nodeValue = []

                curFields = []
                # nodefields constructor
                for fieldIndex, fieldKey in enumerate(fieldTemplate[nodeName].keys()):
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

        #{'be.0.server': 'localhost', 'mq.0.server': 'localhost', 'mq.0.user': 'guest', 'db.0.server': 'localhost', 'fe.0.server': 'localhost', 'db.0.pwd': '', 'mq.0.port': 15672, 'mq.0.pwd': 'guest', 'db.0.user': 'qos', 'db.0.port': 5432}
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
    fields = ['name', 'servers']


@admin.register(Options)
class OptionsAdmin(admin.ModelAdmin):
    # form=RulesForm
    # list_display = ['rule', 'comment']
    fields = ['name', 'value']


@admin.register(Hosts)
class HostsAdmin(admin.ModelAdmin):
    # form=RulesForm
    list_display = ['name', 'key', 'enabled', 'id', 'comment']
    # fields = ['name', 'value']


@admin.register(Items)
class ItemsAdmin(admin.ModelAdmin):
    # form=RulesForm
    list_display = ['name', 'key', 'enabled', 'unit', 'id', 'comment']
    # fields = ['name', 'value']


@admin.register(TaskSets)
class TaskSetsAdmin(admin.ModelAdmin):
    # form=RulesForm
    list_display = ['name', 'server', 'enabled', 'comment']
    # fields = ['name', 'value']

@admin.register(Triggers)
class TriggersAdmin(admin.ModelAdmin):
    pass
    # form=RulesForm
    # list_display = ['rule', 'comment']
    # fields = ['name', 'value']

@admin.register(ResultFormatters)
class ResultFormattersAdmin(admin.ModelAdmin):
    pass
    # form=RulesForm
    # list_display = ['rule', 'comment']
    # fields = ['name', 'value']

# admin.site.register(ServerComponents, ServerComponentsAdmin)    