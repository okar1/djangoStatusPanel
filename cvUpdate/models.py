# -*- coding: utf-8 -*-
from django.db import models


class Users(models.Model):
	login = models.CharField("Пользователь", max_length=255, blank=False, null=False, editable=False)	

	def __str__(self):
		return self.login

	def save(self, *args, **kwargs):
		return

	def delete(self, *args, **kwargs):
		return

	class Meta:
		managed=False
		db_table = 'muser'

# from qsettings.models import Boxes

class Rules(models.Model):
	rule = models.CharField("Правило", max_length=255, blank=False, null=False, editable=True)	
	comment = models.CharField("Комментарий", max_length=255, blank=True, null=False, editable=True)	


	class Meta:	
		verbose_name='правило для виджета'
		verbose_name_plural="правила для виджетов"

	def __str__(self):
		if self.comment=='':
			return self.rule
		else:
			return self.rule+' ('+self.comment+')'

class Tasks(models.Model):
	name = models.CharField("Название задачи", max_length=255, blank=False, null=False, editable=False)

	def __str__(self):
		return self.name

	def save(self, *args, **kwargs):
		return

	def delete(self, *args, **kwargs):
		return

	class Meta:
		managed=False
		# db_table = 'tasksn'
		default_permissions=()
		verbose_name='задача'
		verbose_name_plural="задачи"
		# ordering=['name']

class Boxes(models.Model):
	name = models.CharField("Название виджета", max_length=255, blank=False, null=False, editable=False)
	
	tasks = models.ManyToManyField(Tasks,blank=True,verbose_name="Задачи", editable=True)#, through='boxes_tasks')
	rules = models.ManyToManyField(Rules,blank=True,verbose_name="Правила")#, through='boxes_tasks')
	user = models.ForeignKey(Users,verbose_name="Пользователь", db_column="userid", editable=False)#, through='boxes_tasks')


	def save(self, *args, **kwargs):
		return

	def delete(self, *args, **kwargs):
		return

	def __str__(self):
		# return self.user.login+'.'+self.name
		return self.name

	class Meta:
		managed=False
		#db_table = 'msetconfiguration'
		default_permissions=()
		verbose_name='виджет'
		verbose_name_plural="виджеты"
		ordering=['user','name',]

# class boxes_tasks(models.Model):
# 	# поле в бд называется box_id
# 	boxes = models.ForeignKey(Boxes, on_delete=models.PROTECT)
# 	# поле в бд называется task_id
# 	tasks = models.ForeignKey(Tasks, on_delete=models.PROTECT)


# 	class Meta:
# 		managed=False
# 		unique_together =('boxes','tasks')


