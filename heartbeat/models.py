# -*- coding: utf-8 -*-
import json
import random
from os import urandom
from base64 import b64encode, b64decode
from Crypto.Cipher import ARC4

from django import forms
from django.db import models
from shared.dotmap import DotMap


class Servers(models.Model):

    @staticmethod
    def encryptPassword(plaintext):
        try:
            letters = 'qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮйцукенгшщзхъфывапролджэячсмитьбю'
            SALT_SIZE = 8
            salt = urandom(SALT_SIZE)
            randomText = ''.join([random.choice(letters) for i in range(8)])
            plaintext = "%3d" % len(plaintext) + plaintext + randomText
            plaintext = plaintext.encode('utf-8')
            arc4 = ARC4.new(salt)
            crypted = arc4.encrypt(plaintext)
            res = salt + crypted
            # print('salt=',salt)
            # print('cr=',crypted)
            return b64encode(res).decode('ascii')
        except:
            return ""

    @staticmethod
    def getFieldTemplate():
        # fields template definition
        # field values are stored in "config" field
        fieldTemplate = DotMap()
        with fieldTemplate.mq as mq:
            mq.server.fieldClass = forms.CharField
            mq.server.fieldOptions = dict(
                label="RabbitMQ хост", initial="localhost", help_text="например mq.node.qos", required=False)

            mq.httpUrl.fieldClass = forms.CharField
            mq.httpUrl.fieldOptions = dict(
                label="HTTP url", initial="http://localhost:15672", help_text="например<br/>http://localhost:15672", required=False)

            mq.amqpUrl.fieldClass = forms.CharField
            mq.amqpUrl.fieldOptions = dict(
                label="AMQP url", initial="amqp://guest:guest@localhost:5672/%2f", help_text="например<br/>amqp://guest:guest@localhost:5672/%2f", required=False)

            mq.heartbeatQueue.fieldClass = forms.CharField
            mq.heartbeatQueue.fieldOptions = dict(
                label="Очередь heartbeat", initial="heartbeat", help_text="Название очереди для<br/>сбора сообщений Q,<br/>по умолчанию heartbeat", required=False)

            mq.heartbeatAgentRequest.fieldClass = forms.CharField
            mq.heartbeatAgentRequest.fieldOptions = dict(
                label="Exchange для hb agent", initial="heartbeatAgentRequest", help_text="Название exchange для<br/>отправки сообщений агентам", required=False)

            mq.heartbeatAgentReply.fieldClass = forms.CharField
            mq.heartbeatAgentReply.fieldOptions = dict(
                label="Очередь для hb agent", initial="heartbeatAgentReply", help_text="Название очереди для<br/>приема результатов от агентов", required=False)

        with fieldTemplate.db as db:
            db.server.fieldClass = forms.CharField
            db.server.fieldOptions = dict(
                label="БД хост", initial="localhost", help_text="например db.node.qos", required=False)

            db.port.fieldClass = forms.IntegerField
            db.port.fieldOptions = dict(
                label="Порт", initial=5432, help_text="обычно 5432", required=False)

            db.user.fieldClass = forms.CharField
            db.user.fieldOptions = dict(
                label="Пользователь", initial="qos", help_text="обычно qos", required=False)

            db.pwd.fieldClass = forms.CharField
            db.pwd.fieldOptions = dict(label="Пароль", initial="", help_text="Пароль БД",
                                       required=False, widget=forms.PasswordInput(render_value=True))

        with fieldTemplate.fe as fe:
            fe.server.fieldClass = forms.CharField
            fe.server.fieldOptions = dict(
                label="Frontend хост", initial="localhost", help_text="например fe.node.qos", required=False)

        with fieldTemplate.be as be:
            be.server.fieldClass = forms.CharField
            be.server.fieldOptions = dict(
                label="Backend хост", initial="localhost", help_text="например be.node.qos", required=False)

        return fieldTemplate

    name = models.CharField(
        "Имя сервера", unique=True, max_length=255, blank=False, null=False, editable=True)
    config = models.TextField(null=False, default="")
    qosguialarm = models.BooleanField(
        "Оповещать о событиях heartbeat в основном интерфейсе сервера", blank=False, null=False, editable=True, default=False)

    def getConfigObject(self, decryptPwd=True):

        def decryptPassword(ciphertext):
            try:
                SALT_SIZE = 8
                ciphertext = b64decode(ciphertext.encode('ascii'))
                salt = ciphertext[:SALT_SIZE]
                ciphertext = ciphertext[SALT_SIZE:]
                # print('salt=',salt)
                # print('cr=',ciphertext)
                arc4 = ARC4.new(salt)
                plaintext = arc4.decrypt(ciphertext)
                textLen = int(plaintext[:3])
                # print(textLen)
                plaintext = plaintext[3:].decode('utf-8')
                return plaintext[:textLen]
            except:
                return ""

        def decryptPwdField(fieldKey, fieldValue):
            if decryptPwd and fieldKey == 'pwd' and fieldValue != '':
                return decryptPassword(fieldValue)
            else:
                return fieldValue

        template = self.getFieldTemplate()
        try:
            config = json.loads(self.config)
        except:
            return {nodeName: [{}] for nodeName in template.keys()}

        if type(config) != dict:
            return {nodeName: [{}] for nodeName in template.keys()}

        # {'fe': [{'server':'localhost'}]}
        return {nodeName: [{nodeKey: decryptPwdField(nodeKey, nodeValue[i])
                            for i, nodeKey in enumerate(template[nodeName].keys()) if i < len(nodeValue)}
                           for nodeValue in config[nodeName] if nodeName in config.keys() and type(nodeValue) == list]
                for nodeName in template.keys()}

    class Meta:
        verbose_name = 'сервер'
        verbose_name_plural = "qos -> серверы"

    def __str__(self):
        return self.name

# visible servergroup to show set of servers in gui
class ServerGroups(models.Model):
    name = models.CharField("Имя группы", max_length=255,
                            blank=False, null=False, editable=True)
    servers = models.ManyToManyField(
        Servers, blank=False, verbose_name="Серверы", editable=True)

    class Meta:
        verbose_name = 'группа серверов'
        verbose_name_plural = "qos -> группы серверов"

    def __str__(self):
        return self.name

# heartbeat agentkey list. 
# If equals some of qos agentkeys - than heartbeat records and qos records are united in single box on gui
# if heartbeat agentkey not equals none of qos agentkey - than new box for heartbeat agent will be created
class Hosts(models.Model):
    name = models.CharField("Имя узла", max_length=255, blank=False, null=False, editable=True, default="")
    key = models.CharField("Ключ узла (routing key)", max_length=255, blank=False, null=False, editable=True, default="")
    server = models.ForeignKey(Servers,verbose_name="Сервер", blank=False, null=False, editable=True)
    enabled = models.BooleanField("Включен", blank=False, null=False, editable=True, default=True )
    config = models.TextField(null=False, blank=True, default="")
    comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True, default="")
    # hostgroup = models.ForeignKey(Servers,verbose_name="сервер", editable=True)
    class Meta:
        verbose_name = 'узел сети'
        verbose_name_plural = 'hb -> узлы сети (hosts)'

    def __str__(self):
        return ("(Отключен) " if not self.enabled else "") + " (" +  self.server.name + ") " + self.name

# post processing expressions for result
class ResultFormatters(models.Model):
    name = models.CharField("Имя обработчика", max_length=255, blank=False, null=False, editable=True, default="")
    config = models.TextField(blank=False, null=False, default="")
    
    class Meta:
        verbose_name = 'обработкчик результата'
        verbose_name_plural = 'hb -> обработка результата'

    def __str__(self):
        return self.name

# settings of heartbeat data collectors 
class Items(models.Model):
    name = models.CharField("Имя параметра", max_length=255, blank=False, null=False, editable=True, default="")
    key = models.CharField("Ключ параметра (parameter key)", max_length=255, blank=False, null=False, editable=True, default="")
    enabled = models.BooleanField("Включен", blank=False, null=False, editable=True, default=True)
    unit = models.CharField("Единица измерения", max_length=255, blank=True, null=False, editable=True, default="")
    config = models.TextField(blank=False, null=False, default="")
    resultformatter = models.ForeignKey(ResultFormatters,verbose_name="Обработчик результата", blank=True, null=True)
    comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True, default="")
    
    class Meta:
        verbose_name = 'элемент данных'
        verbose_name_plural = 'hb -> элементы данных (items)'

    def __str__(self):
        return ("(Отключена) " if not self.enabled else "")+self.name + ", "+ self.key+ \
            (" ("+self.comment+")" if self.comment!="" else "")

# alarm treshholds for items. Each item can contain 0 or more triggers
class Triggers(models.Model):
    name = models.CharField("Имя триггера", max_length=255, blank=False, null=False, editable=True, default="")
    config = models.TextField(blank=False, null=False, default="")
    durability=models.PositiveIntegerField("Длительность (сек)",  blank=False, null=False, editable=True, default=0)
    severity = models.CharField("Важность", max_length=255, blank=False, null=False, editable=True, default="high",
        choices=( 
            ("high","Высокая"),
            ("low","Низкая"),
        ))
    class Meta:
        verbose_name = 'триггер'
        verbose_name_plural = 'hb -> триггеры'
    
    def __str__(self):
        return self.config+" ("+self.name+")"


# hosts, items --> server mappings
# each server can contain 0 or more mappings
class TaskSets(models.Model):
    #related_name="membership_invites", on_delete=models.CASCADE
    name = models.CharField("Имя задачи", max_length=255, blank=False, null=False, editable=True)
    hosts = models.ManyToManyField(Hosts, verbose_name="Узлы сети", blank=True, editable=True) 
    items = models.ManyToManyField(Items, verbose_name="Элементы данных", blank=True, editable=True )
    triggers = models.ManyToManyField(Triggers, verbose_name="Триггеры", blank=True, editable=True )
    enabled = models.BooleanField("Включена", blank=False, null=False, editable=True, default=True)
    comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True)
    
    # if item is disabled - all its tasks willl be disabled
    # if agent is disabled - all its tasks is disabled
    #   if taskset is disabled - all itas tasks are disabled,
    #       but disabled tasks in taskset can be overlapped by other tasksets.
    #       in this case such tasks will be enabled
    def getHeartbeatTasks(server,pollingPeriodSec):
        sql="""
        select  
            Distinct ON (hosts.id,items.id,enabled)
            items.id || '-' || hosts.id as "id",
            items.name as "itemname",
            items.key as "itemkey",
            items.unit,
            items.config as "itemconfig",
            hosts.name as "hostname",
            hosts.key as "hostkey",
            hosts.config as "hostconfig",
            format.config as "format",
            bool_or(ts.enabled and hosts.enabled and items.enabled) as "enabled"
        from
            heartbeat_tasksets as "ts",
            heartbeat_hosts as "hosts",
            heartbeat_items as "items"
                left outer join heartbeat_resultformatters as "format" on (items.resultformatter_id=format.id),
            heartbeat_tasksets_hosts as "ts_hosts",
            heartbeat_tasksets_items as "ts_items"
        where 
            hosts.server_id={serverid} and 
            ts_hosts.tasksets_id=ts.id and
            ts_hosts.hosts_id=hosts.id and
            ts_items.tasksets_id=ts.id and
            ts_items.items_id=items.id
        group by items.id,hosts.id,format
        """.format(serverid=str(int(server.id)))
        
        tasks=TaskSets.objects.raw(sql)
        
        # combines itemConfig and HostConfig and converts it to JSON
        def getTaskConfig(sItemConfig,sHostConfig,itemKey):
            # string sItemConfig like {"item":"ohwtable","include":["cpu","load"]}
            # string sHostConfig (optional) like {itemkey:{paramToOverride1: valueToOverride1, paramToOverride2: ...}}
            
            if sItemConfig=="":
                taskConfig={}
            else:                
                try:
                    taskConfig=json.loads(sItemConfig.replace('\\','\\\\'))
                except:
                    raise Exception("неправильная конфигурация элемента данных")
                
            # if hostConfig has any parameters - add it to taskConfig
            if sHostConfig!="":
                try:
                    hostConfig=json.loads(sHostConfig.replace('\\','\\\\'))
                except Exception as e:
                    raise Exception("неправильная конфигурация узла сети "+str(e))

                if itemKey in hostConfig.keys():
                    # host parameters has priority and override same-named item patameters 
                    taskConfig.update(hostConfig[itemKey])
            return taskConfig

        def getTaskFormat(formatStr):
            if formatStr is None:
                return formatStr
            else:
                try:
                    return json.loads(formatStr.replace('\\','\\\\'))
                except Exception as e:
                    raise Exception("проверьте настройку обработки результата "+str(e))

        res={}
        for task in tasks:
            # taskKey=task.hostkey + '.heartbeat.' + task.itemkey
            taskKey="heartbeat." + str(task.id) + "." + task.itemkey
            taskValue={
                    "agentKey":task.hostkey,
                    "agentName":task.hostname,
                    "module":"heartbeat",
                    "displayname":task.itemname,
                    "unit":task.unit,
                    "period":pollingPeriodSec,
                    "enabled":task.enabled
            }
            try:
                taskConfig=getTaskConfig(task.itemconfig,task.hostconfig,task.itemkey)
            except Exception as e:
                taskConfig={}
                taskValue['error']=str(e)
            taskValue["config"]=taskConfig

            try:
                taskFormat=getTaskFormat(task.format)
            except Exception as e:
                taskFormat=None
                taskValue['error']=str(e)
            taskValue["format"]=taskFormat

            res[taskKey]=taskValue
        
        # print(res)
        return res

    class Meta:
        verbose_name = 'задача сбора данных'
        verbose_name_plural = 'hb -> задачи сбора данных'
    def __str__(self):
        return self.name


class Options(models.Model):
    name = models.CharField(
        "Имя", primary_key=True, max_length=255, blank=False, null=False, editable=True)
    value = models.TextField("Значение", max_length=255,
                             blank=False, null=False, editable=True)

    @property
    def valueObject(self):
        if self.name in ['maxMsgTotal', 'pollingPeriodSec']:
            return int(self.value)
        else:
            return self.value

    @staticmethod
    def getOptionsObject():
        return {opt.name: opt.valueObject for opt in Options.objects.all()}

    def valuen(self):
        return self.value.replace("\n", "\\n")
    valuen.short_description = 'Значение'

    class Meta:
        verbose_name = 'настройка'
        verbose_name_plural = "настройки"

    def __str__(self):
        return self.name + "=" + self.valuen()
