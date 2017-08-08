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
        "Имя", unique=True, max_length=255, blank=False, null=False, editable=True)
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
        verbose_name_plural = "qos - серверы"

    def __str__(self):
        return self.name

# visible servergroup to show set of servers in gui
class ServerGroups(models.Model):
    name = models.CharField("Имя", max_length=255,
                            blank=False, null=False, editable=True)
    servers = models.ManyToManyField(
        Servers, blank=True, verbose_name="Серверы", editable=True)

    class Meta:
        verbose_name = 'группа серверов'
        verbose_name_plural = "qos - группы серверов"

    def __str__(self):
        return self.name

# heartbeat agentkey list. 
# If equals some of qos agentkeys - than heartbeat records and qos records are united in single box on gui
# if heartbeat agentkey not equals none of qos agentkey - than new box for heartbeat agent will be created
class Hosts(models.Model):
    agentkey = models.CharField("Ключ БК", max_length=255, blank=False, null=False, editable=True)
    enabled = models.BooleanField("Включен", blank=False, null=False, editable=True, default=True)
    comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True)
    # hostgroup = models.ForeignKey(Servers,verbose_name="сервер", editable=True)
    class Meta:
        verbose_name = 'блок контроля'
        verbose_name_plural = 'heartbeat - блоки контроля (hosts)'

    def __str__(self):
        return self.agentkey


# settings of heartbeat data collectors 
class Items(models.Model):
    name = models.CharField("Имя", max_length=255, blank=False, null=False, editable=True)
    enabled = models.BooleanField("Включен", blank=False, null=False, editable=True, default=True)
    type = models.CharField("Тип опроса", max_length=255, blank=False, null=False, editable=True)
    unit = models.CharField("Единица измерения", max_length=255, blank=False, null=False, editable=True)
    config = models.TextField(null=False, default="")
    comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True)
    
    class Meta:
        verbose_name = 'сборщик данных'
        verbose_name_plural = 'heartbeat - сборщики данных (items)'

    def __str__(self):
        return self.name

# alarm treshholds for items. Each item can contain 0 or more triggers
class Triggers(models.Model):
    item = models.ForeignKey(Items,verbose_name="Сборщик данных", blank=False, null=False, editable=True)
    # = >= <= != < >
    operator = models.CharField("Тип порога", max_length=2, blank=False, null=False, editable=True)
    value = models.CharField("Значение порога", max_length=255, blank=False, null=False, editable=True)
    severity = models.CharField("Важность", max_length=20, blank=False, null=False, editable=True)

    class Meta:
        verbose_name = 'порог оповещения'
        verbose_name_plural = 'heartbeat - пороги оповещения (triggers)'

# hosts, items --> server mappings
# each server can contain 0 or more mappings
class TaskSets(models.Model):
    #related_name="membership_invites", on_delete=models.CASCADE
    name = models.CharField("Имя", max_length=255, blank=False, null=False, editable=True)
    server = models.ForeignKey(Servers,verbose_name="Сервер", blank=False, null=False, editable=True)
    hosts = models.ManyToManyField(Hosts, verbose_name="Блоки контроля", blank=True, editable=True) 
    items = models.ManyToManyField(Items, verbose_name="Сборщики данных", blank=True, editable=True )
    enabled = models.BooleanField("Включена", blank=False, null=False, editable=True, default=True)
    comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True)
    
    # if item is disabled - all its tasks willl be disabled
    # if agent is disabled - all its tasks is disabled
    #   if taskset is disabled - all itas tasks are disabled,
    #       but disabled tasks in taskset can be overlapped by other tasksets.
    #       in this case such tasks will be enabled
    def getHeartbeatTasks(server,pollingPeriodSec):
        sql="""select  
            Distinct ON (hosts.id,items.id,enabled)
            items.id || '-' || hosts.id as "id",
            items.id as "itemid",
            items.name,
            items.type,
            items.unit,
            items.config,
            hosts.agentkey,
            bool_or(ts.enabled and hosts.enabled and items.enabled) as "enabled"
            from
            heartbeat_tasksets as "ts",
            heartbeat_hosts as "hosts",
            heartbeat_items as "items",
            heartbeat_tasksets_hosts as "ts_hosts",
            heartbeat_tasksets_items as "ts_items"
            where 
            ts.server_id={serverid} and 
            ts_hosts.tasksets_id=ts.id and
            ts_hosts.hosts_id=hosts.id and
            ts_items.tasksets_id=ts.id and
            ts_items.items_id=items.id
            group by items.id,hosts.id""" \
            .format(serverid=str(int(server.id)))
        
        tasks=TaskSets.objects.raw(sql)
        # taskKey=agentkey+'.Heartbeat'+'.'+task.itemid
        return {
            task.agentkey + '.Heartbeat.' + str(task.itemid): {
                "agentKey":task.agentkey,
                # for heartbeat tasks agentKey and AgentName are the same
                "agentName":task.agentkey,
                "module":"heartbeat",
                "displayname":task.name,
                "type":task.type,
                "unit":task.unit,
                "config":json.loads(task.config),
                "period":pollingPeriodSec,
                "enabled":task.enabled}
                for task in tasks
        }




    class Meta:
        verbose_name = 'задача сбора данных'
        verbose_name_plural = 'heartbeat - задачи сбора данных'
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
