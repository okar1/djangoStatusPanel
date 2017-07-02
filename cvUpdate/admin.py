# -*- coding: utf-8 -*-
from django.contrib import admin
from .models import Boxes,Rules
from django import forms
# from django.contrib.contenttypes.admin import GenericTabularInline
from django.forms.models import ModelMultipleChoiceField,ChoiceField


# admin.py
    

@admin.register(Boxes)
class BoxesAdmin(admin.ModelAdmin):
	def has_add_permission(self, request, obj=None):
		return False
	def has_delete_permission(self, request, obj=None):
		return False
	list_filter = ('user',)
	list_display = ['name', 'user']
	readonly_fields=('name','user')
	fields=['name','user','rules']
	exclude=('tasks',)
	# debug
	# fields=['name','user','rules','tasks']
	

# @admin.register(BoxesOld)
# class BoxesOldAdmin(admin.ModelAdmin):
# 	def has_add_permission(self, request, obj=None):
# 		return True
# 	def has_delete_permission(self, request, obj=None):
# 		return True
# 	list_display = ['name']



# @admin.register(TasksOld)
# class TasksOldAdmin(admin.ModelAdmin):
# 	pass
# 	# inlines = (BoxesTasksInline,)


class RulesForm(forms.ModelForm):
	boxes = ModelMultipleChoiceField(label="Связанные виджеты",required=False,queryset=Boxes.objects.all())
	tips=ChoiceField(label="Подсказки", required=False,choices=[
		('','---Выберите подсказку---'),
		('+.*НапишиТекст.*','Включить задачи с текстом'),
		('+^((?!НапишиТекст).)*$','Включить задачи без текста'),
		('-.*НапишиТекст.*','Исключить задачи с текстом'),
		('-^((?!НапишиТекст).)*$','Исключить задачи без текста'),
		('+.*{boxname}.*','Имя виджета в имени задачи'),
		('+.*{boxname,1,5}.*','Первые 5 букв виджета в имени задачи'),
		])
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
			initial['boxes'] = [t.pk for t in kwargs['instance'].boxes_set.all()]

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
			instance.boxes_set.clear()
			for item in self.cleaned_data['boxes']:
				instance.boxes_set.add(item)
		self.save_m2m = save_m2m

		# Do we need to save all changes now?
		if commit:
			instance.save()
			self.save_m2m()

		return instance

	class Meta:
		model = Rules
		fields=('rule','tips','comment','boxes')
		# fields = ['name', 'manager']
		# widgets = {
		# 	'manager': forms.Select(),
		# }

	class Media:
		js=(
			'cvUpdate/cvRulesFormHelper.js',
			)

@admin.register(Rules)
class RulesAdmin(admin.ModelAdmin):
	form=RulesForm
	list_display = ['rule', 'comment']


# @admin.register(boxes_tasks)
# class BoxesTasksAdmin(admin.ModelAdmin):
# 	list_display = ['boxes', 'tasks']
# 	list_filter=['boxes']
