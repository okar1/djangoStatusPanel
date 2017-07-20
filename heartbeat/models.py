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

            mq.port.fieldClass = forms.IntegerField
            mq.port.fieldOptions = dict(
                label="HTTP порт", initial=15672, help_text="обычно 15672", required=False)

            mq.user.fieldClass = forms.CharField
            mq.user.fieldOptions = dict(
                label="Пользователь", initial="guest", help_text="по умолчанию guest", required=False)

            mq.pwd.fieldClass = forms.CharField
            mq.pwd.fieldOptions = dict(label="Пароль", initial="guest", help_text="по умолчанию guest",
                                       required=False, widget=forms.PasswordInput(render_value=True))

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
        verbose_name_plural = "серверы"

    def __str__(self):
        return self.name


class ServerGroups(models.Model):
    name = models.CharField("Имя", max_length=255,
                            blank=False, null=False, editable=True)
    servers = models.ManyToManyField(
        Servers, blank=True, verbose_name="Серверы", editable=True)

    class Meta:
        verbose_name = 'группа серверов'
        verbose_name_plural = "группы серверов"

    def __str__(self)::
        return self.name


class Hosts(models.Model):
    agentkey = models.CharField("Ключ БК", max_length=255, blank=False, null=False, editable=True)
    comment = models.CharField("Комментарий", max_length=255, blank=False, null=False, editable=True)
    # hostgroup = models.ForeignKey(Servers,verbose_name="сервер", editable=True)
    class Meta:
        verbose_name = 'блок контроля'
        verbose_name_plural = 'блоки контроля (hosts)'

    def __str__(self):
        return self.name


class PollItems(models.Model):
    name = models.CharField("Имя", max_length=255, blank=False, null=False, editable=True)
    type_ = models.CharField("Тип опроса", max_length=255, blank=False, null=False, editable=True)
    unit = models.CharField("Единица измерения", max_length=255, blank=False, null=False, editable=True)
    config = models.TextField(null=False, default="")

    class Meta:
        verbose_name = 'сборщик данных'
        verbose_name_plural = 'сборщики данных (items)'

    def __str__(self):
        return self.name

class Triggers(models.Model):
    pollitem = models.ForeignKey(Servers,verbose_name="сервер", editable=True)
    # = >= <= != < >
    type_ = models.CharField("Тип порога", max_length=2, blank=False, null=False, editable=True)
    value = models.CharField("Значение порога", max_length=255, blank=False, null=False, editable=True)

    class Meta:
        verbose_name = 'порог оповещения'
        verbose_name_plural = 'пороги оповещения (triggers)'

    def __str__(self):
        return self.name


# Templates, templateGroups <-- --> agents, agentGroups mapping
class Templates(models.Model):
    name = models.CharField("Имя", max_length=255, blank=False, null=False, editable=True)
    server = models.ForeignKey(Servers,verbose_name="сервер", editable=True)
    hosts = models.ManyToManyField(AgentGroups, blank=True) #related_name="membership_invites",
    pollitems = models.ManyToManyField(PollTemplates, blank=True ) #on_delete=models.CASCADE

    class Meta:
        verbose_name = 'шаблон опроса'
        verbose_name_plural = 'шаблоны опроса (templates)'

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
