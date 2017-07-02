# -*- coding: utf-8 -*-

from django.db import models

# Create your models here.
class ControlBlocks(models.Model):
	name = models.CharField("Название", max_length=255, blank=False, null=False, editable=True)
	ip = models.GenericIPAddressField("Ip", protocol='IPv4', blank=False, null=False, editable=True)

	class Meta:	
		verbose_name='блок контроля'
		verbose_name_plural="блоки контроля"

	def __str__(self):
			return self.name

class Bridges(models.Model):
	name = models.CharField("Название", max_length=255, blank=False, null=False, editable=True)
	ip = models.GenericIPAddressField("Ip", protocol='IPv4', blank=False, null=False, editable=True)
	controlblock = models.ForeignKey(ControlBlocks,verbose_name="блок контроля", editable=True)
	# full channels count on VB, including skipped ones. Used for progress meter calculation
	chcount = models.IntegerField ("Количество каналов",  default=0, editable=True)

	class Meta:	
		verbose_name='видеобридж'
		verbose_name_plural="видеобриджи"

	def __str__(self):
			return self.name

class Channels(models.Model):
	name = models.CharField("Название", max_length=255, blank=False, null=False, editable=True)
	ip = models.GenericIPAddressField("Ip", protocol='IPv4', blank=False, null=False, editable=True)
	port = models.IntegerField ("Порт",  blank=False, null=False, editable=True)
	index = models.IntegerField("Индекс",  blank=False, null=False, editable=True)
	bridge=models.ForeignKey(Bridges,verbose_name="Видеобридж", editable=True)

	class Meta:	
		verbose_name='канал'
		verbose_name_plural="каналы"

	def __str__(self):
			return self.name

class Options(models.Model):
	name = models.CharField("Имя", primary_key=True, max_length=255, blank=False, null=False, editable=True)
	value = models.TextField("Значение", max_length=255, blank=False, null=False, editable=True)

	def valuen(self):
		return self.value.replace("\n","\\n")
	valuen.short_description = 'Значение'

	class Meta:	
		verbose_name='настройка'
		verbose_name_plural="настройки"

	def __str__(self):
			return self.name+"="+self.valuen()