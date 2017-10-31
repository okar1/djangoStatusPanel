# -*- coding: utf-8 -*-
import json
import random
import traceback

import sys
from os import urandom
from base64 import b64encode, b64decode
from Crypto.Cipher import ARC4

from django import forms
from django.db import models
from django.contrib.auth.models import Group
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

        with fieldTemplate.timeDB as timeDB:
            timeDB.httpUrl.fieldClass = forms.CharField
            timeDB.httpUrl.fieldOptions = dict(
                label="Url для сохранения результатов", initial="", help_text="например http://localhost:8086/write?db=имяБД", required=False)

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
                           for nodeValue in config.get(nodeName,[]) if type(nodeValue) == list]
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
    usergroups = models.ManyToManyField(Group, blank=True, verbose_name="Доступна для группы пользователей", editable=True,
        help_text='Данная группа серверов будет видна только указанным группам пользователей.</br>'+ \
        'Пользователям с правами "Администратор" и "Персонал" всегда будут видны все группы серверов.</br>')

    class Meta:
        verbose_name = 'группа серверов'
        verbose_name_plural = "qos -> группы серверов"

    def __str__(self):
        return self.name

# heartbeat agentkey list. 
# If equals some of qos agentkeys - than heartbeat records and qos records are united in single box on gui
# if heartbeat agentkey not equals none of qos agentkey - than new box for heartbeat agent will be created
class Hosts(models.Model):
    name = models.CharField("Имя узла", max_length=255, blank=False, null=False, editable=True, default="",
        help_text="Название плитки heartbeat, понятное пользователю.</br>"+
                "Допускается создавать узлы с одиннаковыми названиями,</br>"+
                "при этом результаты по таким узлам будут выведены в общую плитку")
    key = models.CharField("Ключ узла (routing key)", max_length=255, blank=False, null=False, editable=True, default="",
        help_text="Ключ маршрутизации RabbitMQ, который будет назначен всем сообщениям </br>"+
                "на этом узле перед публикацией в RabbitMQ. Ключ маршрутизации может влиять на какую конкретно</br>"+
                "машину с heartbeat agent будет отправлено задание от элемента данных.</br>"+
                "Допускается создать несколько узлов с одинаковым ключом</br>"+
                'Например, когда часть результатов от одного из heartbeat agent нужно вынести в отдельную плитку')
    server = models.ForeignKey(Servers,verbose_name="Сервер", blank=False, null=False, editable=True,
        help_text="Сервер, к которому будет относиться плитка")
    enabled = models.BooleanField("Включен", blank=False, null=False, editable=True, default=True)
    config = models.TextField(null=False, blank=True, default="",
        help_text="Настройки узла в формате JSON. Дополняют настройки элементов данных, работающих на этом узле</br>"+
                    r'Например: {"item":"fanSpeed","resultcount":5} добавит опцию resultсоunt к элементу данных fanSpeed на этом узле')
    aliases = models.TextField("Алиасы", max_length=255, blank=True, null=False, editable=True, default="",
        help_text="Список алиасов (синонимов) для этой плитки heartbeat. Каждый алиас пишется в новой строке</br>"+
                'При совпадении имени БК с одним из алиасов, его задачи этого БК будут объединены с этой плиткой')
    alarms = models.TextField("Оповестить если", blank=True, null=False, default="",
        help_text="Если одно из условий выполняется заданное время, пользователь получит оповещение.</br>"+
                    "Время может задаваться после условия. Если время не указано, оповещение срабатывает сразу</br>"+
                    "Время задается как количество периодов опроса (1p, 2p итд) по умолчанию один период равен 60 сек</br>"+
                    "Если настройки узла заданы, они заменяют одноименные настройки элемента данных на этом узле</br>"+
                    "Примеры (ключ параметра задается в настройках элемента данных):</br>"+
                    "cpuLoad >=20</br>"+
                    "temperature>10 1p</br>"+
                    "lan=2 3p</br>"+
                    "someParam</br>"+
                    "!someParam2 2p")
    comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True, default="")
    # hostgroup = models.ForeignKey(Servers,verbose_name="сервер", editable=True)
    class Meta:
        verbose_name = 'узел сети'
        verbose_name_plural = 'hb -> узлы сети (hosts)'

    def __str__(self):
        return ("(Отключен) " if not self.enabled else "") + " (" +  self.server.name + ") " + self.name

    # returns hosts aliases like {server:{host:{aliases set},host2:{aliases set}},server2:...}
    def getAllAliases():
        res={}
        hosts=Hosts.objects.all()
        for host in hosts:
            server=host.server.name
            hostName=host.name
            
            singleServerAliases=res.setdefault(server,{})
            aliasesStr=host.aliases.replace('\r','')
            
            oldValue = singleServerAliases.get(hostName,set())
            newValue = set(filter(lambda x: x != '' , aliasesStr.split('\n')))
            # set union
            singleServerAliases[hostName]= oldValue | newValue

        return res


    # returns hosts enabled status like  {server:{"host with some name":True}}
    def getEnabled():
        res={}
        hosts=Hosts.objects.all()
        for host in hosts:
            server=host.server.name
            hostName=host.name
            hostEnabled=host.enabled

            singleServerEnabled=res.setdefault(server,{})

            oldValue = singleServerEnabled.get(hostName,True)
            newValue = hostEnabled

            # all hosts with such name must be enabled for it return enabled
            singleServerEnabled[hostName]= oldValue and newValue

        return res


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
    name = models.CharField("Имя параметра", max_length=255, blank=False, null=False, editable=True, default="",
        help_text="Название параметра (метрики), понятное пользователю")
    key = models.CharField("Ключ параметра (parameter key)", max_length=255, blank=False, null=False, editable=True, default="",
        help_text="Код параметра. Используется при проверке условий.</br>"+
                "Допускается создать несколько элементов данных с одинаковым ключом</br>"+
                'Например, если параметр cpuLoad собирается разными способами на разных хостах')
    enabled = models.BooleanField("Включен", blank=False, null=False, editable=True, default=True)
    unit = models.CharField("Единица измерения", max_length=255, blank=True, null=False, editable=True, default="")
    config = models.TextField(blank=False, null=False, default="",
        help_text="Настройки элемента данных в формате JSON. Определяют тип элемента данных и способ его работы</br>"+
                 r'Например: {"item":"windowsFirewallStatus" }')
    alarms = models.TextField("Оповестить если", blank=True, null=False, default="",
        help_text="Если одно из условий выполняется заданное время, пользователь получит оповещение.</br>"+
                    "Время может задаваться после условия. Если время не указано, оповещение срабатывает сразу</br>"+
                    "Время задается как количество периодов опроса (1p, 2p итд) по умолчанию один период равен 60 сек</br>"+
                    "Если настройки узла заданы, они заменяют одноименные настройки элемента данных на этом узле</br>"+
                    "Примеры (ключ параметра задается в настройках элемента данных):</br>"+
                    "cpuLoad >=20</br>"+
                    "temperature>10 1p</br>"+
                    "lan=2 3p</br>"+
                    "someParam</br>"+
                    "!someParam2 2p")
    resultformatter = models.ForeignKey(ResultFormatters,verbose_name="Обработчик результата", blank=True, null=True,
        help_text="Дополнительные действия после получения значения от источника данных. Умножение, сложение, форматирование строки итд")
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
    name = models.CharField("Имя задачи", max_length=255, blank=False, null=False, editable=True,
        help_text="Задачи сбора данных создают связи между хостами и элементами данных.</br>"+
                "Имя задачи указывается для удобства настройки, оно не видно пользователю.</br>"+
                "Допускается создание повторяющихся связей в разных задачах. Такие повторы будут автоматически устраняться.")
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
            hosts.id || '-' || items.id as "id",
            items.name as "itemname",
            items.key as "itemkey",
            items.unit,
            items.config as "itemconfig",
            items.alarms as "itemalarms",
            hosts.name as "hostname",
            hosts.key as "hostkey",
            hosts.config as "hostconfig",
            hosts.alarms as "hostalarms",
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
                    assert type(taskConfig)==dict
                except:
                    raise Exception("неправильная конфигурация элемента данных")
                
            # if hostConfig has any parameters - add it to taskConfig
            if sHostConfig!="":
                try:
                    hostConfig=json.loads(sHostConfig.replace('\\','\\\\'))
                    assert type(hostConfig)==dict
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
                    res=json.loads(formatStr.replace('\\','\\\\'))
                    assert type(res)==dict or type(res)==list
                    return res
                except Exception as e:
                    raise Exception("проверьте настройку обработки результата "+str(e))
                    traceback.print_exc(file=sys.stdout)


        # todo parse alarms correctly. Process hostAlarms
        def getTaskAlarms(sItemAlarms,sHostAlarms):
            if sItemAlarms!='':
                res=json.loads(sItemAlarms.replace('\\','\\\\'))
            else:
                res={}
            # print("------",res)
            # res={
            # "public > 15":{"pattern":r"\.Public", "item":"isfalse", "duration":0},
            # "private > 20":{"pattern":r"\.Private", "item":"isfalse", "duration":0}
            # }
            return res


        res={}
        for task in tasks:
            # taskKey=task.hostkey + '.heartbeat.' + task.itemkey
            # taskKey="heartbeat." + str(task.id) + "." + task.itemkey
            taskKey=task.hostkey + "." + str(task.id) + "." + task.itemkey
            taskValue={
                    "agentKey":task.hostkey,
                    "itemKey":task.itemkey,
                    "agentName":task.hostname,
                    "module":"heartbeat",
                    "itemName":task.itemname,
                    "unit":task.unit,
                    "period":pollingPeriodSec,
                    "enabled":task.enabled
            }
            try:
                taskConfig=getTaskConfig(task.itemconfig,task.hostconfig,task.itemkey)
            except Exception as e:
                taskConfig={}
                taskValue['error']=str(e)
                traceback.print_exc(file=sys.stdout)

            taskValue["config"]=taskConfig

            try:
                taskAlarms=getTaskAlarms(task.itemalarms,task.hostalarms)
                if taskAlarms:
                    taskConfig["alarms"]=taskAlarms
            except Exception as e:
                taskAlarms={}
                taskValue['error']="Настройка оповещений "+str(e)
                traceback.print_exc(file=sys.stdout)
            
            try:
                taskFormat=getTaskFormat(task.format)
                if taskFormat:
                    taskConfig["format"]=taskFormat
            except Exception as e:
                taskFormat=None
                taskValue['error']="Настройка обработки результата "+str(e)
                traceback.print_exc(file=sys.stdout)
            

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