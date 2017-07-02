from django.contrib import admin
from .models import Bridges,ControlBlocks,Options
from django import forms

# Register your models here.

# from .models import Channels
# @admin.register(Channels)
# class ChannelsAdmin(admin.ModelAdmin):
# 	list_filter = ('bridge',)

@admin.register(Bridges)
class BridgesAdmin(admin.ModelAdmin):
	list_display = ['name', 'ip','controlblock','chcount']
	# exclude=('chcount',)

@admin.register(ControlBlocks)
class ControlBlocksAdmin(admin.ModelAdmin):
	list_display = ['name', 'ip']

class OptionsForm(forms.ModelForm):
	pass

@admin.register(Options)
class OptionsAdmin(admin.ModelAdmin):
	form=OptionsForm
	list_display = ['name', 'valuen']